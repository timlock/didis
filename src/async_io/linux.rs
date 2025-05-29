#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[cfg(not(rust_analyzer))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use crate::async_io::{AsyncIO, Completion};
use std::collections::VecDeque;
use std::io;
use std::mem::zeroed;
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd};
use std::ptr::null_mut;
use std::time::Duration;

impl From<&io_uring_cqe> for Completion {
    fn from(cqe: &io_uring_cqe) -> Self {
        let task_ptr: *mut Task = cqe.user_data as _;
        let task = unsafe { Box::from_raw(task_ptr) };

        match *task {
            Task::Accept(listener) => {
                if cqe.res < 0 {
                    return Completion::Accept(
                        listener,
                        Err(io::Error::from_raw_os_error(-cqe.res)),
                    );
                }
                let socket = unsafe { TcpStream::from_raw_fd(cqe.res) };
                match socket.peer_addr() {
                    Ok(address) => Completion::Accept(listener, Ok((socket, address))),
                    Err(err) => Completion::Accept(listener, Err(err)),
                }
            }
            Task::Receive(socket, received) => {
                if cqe.res < 0 {
                    return Completion::Receive(
                        socket,
                        received,
                        Err(io::Error::from_raw_os_error(-cqe.res)),
                    );
                }
                Completion::Receive(socket, received, Ok(cqe.res as _))
            }
            Task::Send(socket, sent) => {
                if cqe.res < 0 {
                    return Completion::Send(
                        socket,
                        sent,
                        Err(io::Error::from_raw_os_error(-cqe.res)),
                    );
                }
                Completion::Send(socket, sent, Ok(cqe.res as _))
            }
        }
    }
}

enum Task {
    Accept(TcpListener),
    Send(TcpStream, Box<[u8]>),
    Receive(TcpStream, Box<[u8]>),
}

pub struct IO {
    ring: io_uring,
    pending: VecDeque<Task>,
    queue_depth: u32,
}

impl IO {
    pub fn new(queue_depth: u32) -> io::Result<Self> {
        let mut ring: io_uring = unsafe { zeroed() };
        let ret = unsafe { io_uring_queue_init(queue_depth, &mut ring, 0) };
        if ret < 0 {
            return Err(io::Error::from_raw_os_error(-ret));
        }

        Ok(Self {
            ring,
            pending: Default::default(),
            queue_depth,
        })
    }

    fn flush(&mut self, duration: Duration) -> io::Result<Vec<Completion>> {
        let mut timeout = __kernel_timespec {
            tv_sec: duration.as_secs() as _,
            tv_nsec: duration.subsec_nanos() as _,
        };

        let mut cqes = vec![null_mut(); self.queue_depth as usize];

        let ret = unsafe {
            io_uring_submit_and_wait_timeout(
                &mut self.ring,
                cqes.as_mut_ptr(),
                1,
                &mut timeout as *mut _,
                null_mut(),
            )
        };

        if ret < 0 {
            let error_code = -ret as u32;
            return match error_code {
                ETIME => Ok(Vec::new()),
                _ => Err(io::Error::from_raw_os_error(error_code as _)),
            };
        }

        let cqes_iter = cqes.iter().filter(|&&cqe| cqe as usize != 0);

        let completed = ret as usize;
        let mut completions = Vec::with_capacity(completed);

        for cqe in cqes_iter {
            let result = unsafe { &**cqe };
            let completion = Completion::from(result);
            unsafe { io_uring_cqe_seen(&mut self.ring, *cqe) };
            completions.push(completion);
        }

        Ok(completions)
    }
}

impl AsyncIO for IO {
    fn accept(&mut self, listener: TcpListener) {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            self.pending.push_back(Task::Accept(listener));
            return;
        }

        let fd = listener.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Accept(listener)));

        unsafe {
            io_uring_prep_accept(sqe, fd, null_mut(), null_mut(), 0);
            (*sqe).user_data = task_ptr as _;
        }
    }

    fn receive(&mut self, socket: TcpStream, mut buffer: Box<[u8]>) {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            self.pending.push_back(Task::Receive(socket, buffer));
            return;
        }

        let fd = socket.as_raw_fd();
        let buf_ptr = buffer.as_mut_ptr();
        let buf_len = buffer.len();
        let task_ptr = Box::into_raw(Box::new(Task::Receive(socket, buffer)));

        unsafe {
            io_uring_prep_recv(sqe, fd, buf_ptr as _, buf_len, 0);
            (*sqe).user_data = task_ptr as _;
        }
    }

    fn send(&mut self, socket: TcpStream, buffer: Box<[u8]>, len: usize) {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            self.pending.push_back(Task::Send(socket, buffer));
            return;
        }

        let fd = socket.as_raw_fd();
        let mut buffer = buffer;
        let buf_ptr = buffer.as_mut_ptr();
        let task_ptr = Box::into_raw(Box::new(Task::Send(socket, buffer)));

        unsafe {
            io_uring_prep_send(sqe, fd, buf_ptr as _, len, 0);
            (*sqe).user_data = task_ptr as _;
        }
    }

    fn poll(&mut self) -> io::Result<Vec<Completion>> {
        self.poll_timeout(Duration::from_secs(0))
    }

    fn poll_timeout(&mut self, duration: Duration) -> io::Result<Vec<Completion>> {
        self.flush(duration)
    }
}

impl Drop for IO {
    fn drop(&mut self) {
        unsafe { io_uring_queue_exit(&mut self.ring) };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::str::FromStr;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn accept() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8000")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(32)?;
        io.accept(listener);

        let address = SocketAddr::from_str("127.0.0.1:8000")?;
        let handler = thread::spawn(move || {
            println!("Connect to server");
            let result = TcpStream::connect_timeout(&address, Duration::from_secs(5));
            match result {
                Ok(s) => println!("CONNECTED"),
                Err(err) => println!("Could not conenct {err}"),
            };
        });

        handler.join().unwrap();

        let results = io.flush(Duration::from_secs(1))?;
        assert_eq!(1, results.len());

        Ok(())
    }

    #[test]
    fn echo() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8001")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(32)?;
        io.accept(listener);

        let address = SocketAddr::from_str("127.0.0.1:8001")?;
        let handler = thread::spawn(move || {
            eprintln!("Connect to server");
            let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();

            eprintln!("Sending hello to server");
            let sent = b"hello";
            socket.write_all(sent.as_slice()).unwrap();

            eprintln!("Waiting for response from server");
            let mut received = vec![0u8; 5];
            socket.read_exact(received.as_mut_slice()).unwrap();

            eprintln!("Received {:?} from server", received);
            assert_eq!(sent, received.as_slice());
        });

        let mut results = io.flush(Duration::from_secs(1))?;
        assert_eq!(1, results.len());
        let (socket, _) = match results.remove(0) {
            Completion::Accept(_, socket) => socket?,
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Client connected");

        let buf = Box::new([0u8; 1024]);
        io.receive(socket, buf);
        results = io.flush(Duration::from_secs(1))?;
        let (socket, buf, len) = match results.remove(0) {
            Completion::Receive(socket, buf, len) => (socket, buf, len?),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Received {len} bytes from client");
        assert_eq!(5, len);
        assert_eq!(b"hello", buf[..5].iter().as_slice());

        let mut buf = Box::new([0u8; 1024]);
        buf[..5].copy_from_slice(b"hello");
        io.send(socket, buf, 5);
        results = io.flush(Duration::from_secs(1))?;
        let (socket, buf, len) = match results.remove(0) {
            Completion::Send(socket, buf, len) => (socket, buf, len?),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Sent {} bytes to client", len);

        handler.join().unwrap();

        Ok(())
    }
}
