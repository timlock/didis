use crate::async_io::{AsyncIO, Completion};
use libc::{
    close, kevent, kqueue, timespec, EVFILT_READ, EVFILT_WRITE, EV_ADD, EV_CLEAR, EV_ONESHOT,
};
use std::collections::VecDeque;
use std::io;
use std::io::{Read, Write};
use std::mem::zeroed;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::raw::c_int;
use std::time::Duration;

impl From<Task> for Completion {
    fn from(value: Task) -> Self {
        match value {
            Task::Accept(socket) => {
                let result = socket.accept();
                Completion::Accept(socket, result)
            }
            Task::Receive(mut socket, mut buf, id) => {
                let n = socket.read(buf.as_mut());
                Completion::Receive(socket, buf, n, id)
            }
            Task::Send(mut socket, mut buf, len,id) => {
                let n = socket.write(&mut buf[..len]);
                Completion::Send(socket, buf, n, id)
            }
        }
    }
}

enum Task {
    Accept(TcpListener),
    Receive(TcpStream, Box<[u8]>, u64),
    Send(TcpStream, Box<[u8]>, usize, u64),
}

pub struct IO {
    kqueue: c_int,
    pending: VecDeque<kevent>,
    events: Vec<kevent>,
}

impl IO {
    pub fn new(queue_depth: usize) -> io::Result<Self> {
        let kqueue = unsafe { kqueue() };
        if kqueue == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            kqueue,
            pending: Default::default(),
            events: unsafe { vec![zeroed(); queue_depth] },
        })
    }

    fn flush(&mut self, changes: usize, duration: Duration) -> usize {
        let events_ptr = self.events.as_mut_ptr();

        let timeout = timespec {
            tv_sec: duration.as_secs() as _,
            tv_nsec: duration.subsec_nanos() as _,
        };

        let completed_events = unsafe {
            kevent(
                self.kqueue,
                events_ptr,
                changes as _,
                events_ptr,
                self.events.len() as _,
                &timeout,
            )
        };

        if completed_events < 0 {
            eprintln!("Operation will fail: {}", io::Error::last_os_error());
        }

        completed_events as usize
    }

    fn fill_change_list(&mut self) -> usize {
        for i in 0..self.events.len() {
            match self.pending.pop_back() {
                Some(next) => self.events[i] = next,
                None => return i,
            }
        }

        self.events.len()
    }
}

impl AsyncIO for IO {
    fn accept(&mut self, socket: TcpListener) {
        let fd = socket.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Accept(socket)));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_READ,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: task_ptr as _,
        };

        self.pending.push_back(event);
    }

    fn receive(&mut self, socket: TcpStream, buf: Box<[u8]>, id: u64) {
        let fd = socket.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Receive(socket, buf, id)));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_READ,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: task_ptr as _,
        };

        self.pending.push_back(event);
    }

    fn send(&mut self, socket: TcpStream, buf: Box<[u8]>, len: usize, id: u64) {
        let fd = socket.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Send(socket, buf, len, id)));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_WRITE,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: task_ptr as _,
        };

        self.pending.push_back(event);
    }

    fn poll(&mut self) -> io::Result<Vec<Completion>> {
        self.poll_timeout(Duration::from_secs(0))
    }

    fn poll_timeout(&mut self, duration: Duration) -> io::Result<Vec<Completion>> {
        let added_events = self.fill_change_list();
        let completed_events = self.flush(added_events, duration);

        let mut results = Vec::new();

        for event in self.events[..completed_events].iter() {
            let task_ptr: *mut Task = event.udata as _;
            let task = unsafe { Box::from_raw(task_ptr) };
            let result = Completion::from(*task);
            results.push(result);
        }

        Ok(results)
    }
}

impl Drop for IO {
    fn drop(&mut self) {
        unsafe { close(self.kqueue) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::str::FromStr;
    use std::time::Duration;
    use std::{error, thread};

    #[test]
    fn accept() -> Result<(), Box<dyn error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8000")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(1)?;
        io.accept(listener.try_clone().unwrap());

        let address = SocketAddr::from_str("127.0.0.1:8000")?;
        let handler = thread::spawn(move || {
            println!("Connect to server");
            let result = TcpStream::connect_timeout(&address, Duration::from_secs(5));
            match result {
                Ok(_) => println!("CONNECTED"),
                Err(err) => println!("Could not conenct {err}"),
            };
        });

        handler.join().unwrap();

        let results = io.poll()?;
        assert_eq!(1, results.len());

        Ok(())
    }

    #[test]
    fn echo() -> Result<(), Box<dyn error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8001")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(8)?;
        io.accept(listener.try_clone().unwrap());

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

        let mut results = io.poll_timeout(Duration::from_secs(1))?;
        assert_eq!(1, results.len());
        let (socket, _) = match results.remove(0) {
            Completion::Accept(_, stream) => stream?,
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Client connected");

        let buf = Box::new([0u8; 1024]);
        io.receive(socket.try_clone().unwrap(), buf, id);
        results = io.poll_timeout(Duration::from_secs(1))?;
        let (buf, len, id) = match results.remove(0) {
            Completion::Receive(_, buf, len, id) => (buf, len?, id),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Received {len} bytes from client {id}");
        assert_eq!(5, len);
        assert_eq!(b"hello", buf[..5].iter().as_slice());

        let mut buf = Box::new([0u8; 1024]);
        buf[..5].copy_from_slice(b"hello");
        io.send(socket.try_clone().unwrap(), buf, 5, id);
        results = io.poll_timeout(Duration::from_secs(1))?;
        let (_, len, id) = match results.remove(0) {
            Completion::Send(_, buf, len, id) => (buf, len?, id),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Sent {} bytes to client {}", len, id);

        handler.join().unwrap();

        Ok(())
    }
}
