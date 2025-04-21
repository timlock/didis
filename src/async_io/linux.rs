#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[cfg(not(rust_analyzer))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::collections::{HashMap, VecDeque};
use std::mem::{zeroed, MaybeUninit};
use std::net::{TcpListener, TcpStream};
use std::ops::Deref;
use std::os::fd::{AsRawFd, FromRawFd};
use std::ptr::{null, null_mut};
use std::time::Duration;
use std::{io, ptr};

const QUEUE_DEPTH: u32 = 256;

const BUFFER_SIZE: usize = 1024;

pub enum Completion {
    Accept(TcpListener, io::Result<TcpStream>),
    Close(TcpStream, io::Result<()>),
    Send(TcpStream, Box<[u8; BUFFER_SIZE]>, io::Result<usize>),
    Receive(TcpStream, Box<[u8; BUFFER_SIZE]>, io::Result<usize>),
}

impl From<io_uring_cqe> for Completion {
    fn from(cqe: io_uring_cqe) -> Self {
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
                Completion::Accept(listener, Ok(socket))
            }
            Task::Close(socket) => {
                if cqe.res < 0 {
                    return Completion::Close(socket, Err(io::Error::from_raw_os_error(-cqe.res)));
                }
                Completion::Close(socket, Ok(()))
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
    Close(TcpStream),
    Send(TcpStream, Box<[u8; BUFFER_SIZE]>),
    Receive(TcpStream, Box<[u8; BUFFER_SIZE]>),
}

pub struct IO {
    ring: io_uring,
    pending: VecDeque<Task>,
}

impl IO {
    pub fn new(queue_depth: u32) -> Result<Self, io::Error> {
        let mut ring: io_uring = unsafe { zeroed() };
        let ret = unsafe { io_uring_queue_init(queue_depth, &mut ring, 0) };
        if ret < 0 {
            return Err(io::Error::from_raw_os_error(-ret));
        }

        Ok(Self {
            ring,
            pending: Default::default(),
        })
    }

    pub fn accept(&mut self, listener: TcpListener) {
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

    pub fn close(&mut self, socket: TcpStream) {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            self.pending.push_back(Task::Close(socket));
            return;
        }

        let fd = socket.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Close(socket)));

        unsafe {
            io_uring_prep_close(sqe, fd);
            (*sqe).user_data = task_ptr as _;
        }
    }

    pub fn receive(&mut self, socket: TcpStream, mut buffer: Box<[u8; BUFFER_SIZE]>) {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            self.pending.push_back(Task::Receive(socket, buffer));
            return;
        }

        let fd = socket.as_raw_fd();
        let buf_ptr = buffer.as_mut_ptr();
        let task_ptr = Box::into_raw(Box::new(Task::Receive(socket, buffer)));

        unsafe {
            io_uring_prep_recv(sqe, fd, buf_ptr as _, BUFFER_SIZE, 0);
            (*sqe).user_data = task_ptr as _;
        }
    }

    pub fn send(&mut self, socket: TcpStream, buffer: Box<[u8; BUFFER_SIZE]>, len: usize) {
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

    pub fn poll(&mut self) -> io::Result<Vec<Completion>> {
        self.poll_timeout(Duration::from_secs(0))
    }

    pub fn poll_timeout(&mut self, duration: Duration) -> io::Result<Vec<Completion>> {
        self.flush(duration)
    }

    fn flush(&mut self, duration: Duration) -> io::Result<Vec<Completion>> {
        let mut timeout = __kernel_timespec {
            tv_sec: duration.as_secs() as _,
            tv_nsec: duration.subsec_nanos() as _,
        };

        let mut cqes = [null_mut(); QUEUE_DEPTH as usize];

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
            return Err(io::Error::from_raw_os_error(-ret));
        }

        let cqes_iter = cqes.iter().filter(|&&cqe| cqe as usize != 0);

        let completed = ret as usize;
        let mut completions = Vec::with_capacity(completed);

        for cqe in cqes_iter {
            let result = unsafe { ptr::read(*cqe) };
            unsafe { io_uring_cqe_seen(&mut self.ring, *cqe) };
            let completion = Completion::from(result);
            completions.push(completion);
        }

        Ok(completions)
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

        let mut io = IO::new(QUEUE_DEPTH)?;
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

        let mut io = IO::new(QUEUE_DEPTH)?;
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
        let socket = match results.remove(0) {
            Completion::Accept(_, socket) => socket?,
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Client connected");

        let buf = Box::new([0u8; BUFFER_SIZE]);
        io.receive(socket, buf);
        results = io.flush(Duration::from_secs(1))?;
        let (socket, buf, len) = match results.remove(0) {
            Completion::Receive(socket, buf, len) => (socket, buf, len?),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Received {len} bytes from client");
        assert_eq!(5, len);
        assert_eq!(b"hello", buf[..5].iter().as_slice());

        let mut buf = Box::new([0u8; BUFFER_SIZE]);
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
