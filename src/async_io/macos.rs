use libc::{
    close, kevent, kqueue, timespec, EVFILT_READ, EVFILT_WRITE,
    EV_ADD, EV_CLEAR, EV_ONESHOT,
};
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::io;
use std::io::{Read, Write};
use std::mem::zeroed;
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::c_int;
use std::time::Duration;

const BUFFER_SIZE: usize = 1024;
#[derive(Debug)]
pub enum Completion {
    Accept(TcpListener, TcpStream),
    Close(RawFd),
    Send(TcpStream, Box<[u8; BUFFER_SIZE]>, usize),
    Receive(TcpStream, Box<[u8; BUFFER_SIZE]>, usize),
}

enum Task {
    Accept(TcpListener),
    Receive(TcpStream, Box<[u8; BUFFER_SIZE]>),
    Send(TcpStream, Box<[u8; BUFFER_SIZE]>, usize),
}

impl Task {
    fn complete(self) -> io::Result<Completion> {
        let result = match self {
            Task::Accept(socket) => {
                let (stream, _) = socket.accept()?;
                Completion::Accept(socket, stream)
            }
            Task::Receive(mut socket, mut buf) => {
                let n = socket.read(buf.as_mut_slice())?;
                Completion::Receive(socket, buf, n)
            }
            Task::Send(mut socket, mut buf, len) => {
                let n = socket.write(&mut buf[..len])?;
                Completion::Send(socket, buf, n)
            }
        };
        Ok(result)
    }
}

pub struct IO<const N: usize> {
    kqueue: c_int,
    pending: VecDeque<kevent>,
    events: [kevent; N],
    tasks: HashMap<u64, Task>,
    next_id: u64,
}

impl<const N: usize> IO<N> {
    pub fn new() -> io::Result<Self> {
        let kqueue = unsafe { kqueue() };
        if kqueue == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            kqueue,
            pending: Default::default(),
            events: unsafe { [zeroed(); N] },
            tasks: Default::default(),
            next_id: 0,
        })
    }

    pub fn accept(&mut self, socket: TcpListener) {
        let fd = socket.as_raw_fd();
        let operation_id = self.add_task(Task::Accept(socket));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_READ,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: operation_id as _,
        };

        self.pending.push_back(event);
    }

    pub fn receive(&mut self, socket: TcpStream, buf: Box<[u8; BUFFER_SIZE]>) {
        let fd = socket.as_raw_fd();
        let operation_id = self.add_task(Task::Receive(socket, buf));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_READ,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: operation_id as _,
        };

        self.pending.push_back(event);
    }

    pub fn send(&mut self, socket: TcpStream, buf: Box<[u8; BUFFER_SIZE]>, len: usize) {
        let fd = socket.as_raw_fd();
        let operation_id = self.add_task(Task::Send(socket, buf, len));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_WRITE,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: operation_id as _,
        };

        self.pending.push_back(event);
    }

    pub fn poll(&mut self) -> io::Result<Vec<Completion>> {
        self.poll_timeout(Duration::from_secs(0))
    }

    pub fn poll_timeout(&mut self, duration: Duration) -> io::Result<Vec<Completion>> {
        let added_events = self.fill_change_list();
        let completed_events = self.flush(added_events, duration);

        let mut results = Vec::new();

        for event in self.events[..completed_events].iter() {
            let task_id = event.udata as u64;
            match self.tasks.remove(&task_id) {
                Some(task) => {
                    let result = task.complete()?;
                    results.push(result);
                }
                None => eprintln!("retrieved event with unknown id {task_id}"),
            }
        }

        Ok(results)
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

    fn add_task(&mut self, task: Task) -> u64 {
        let user_data = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.tasks.insert(user_data, task);
        user_data
    }
}

impl<const N: usize> Drop for IO<N> {
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

        let mut io: IO<1> = IO::new()?;
        io.accept(listener.try_clone().unwrap());

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

        let results = io.poll()?;
        assert_eq!(1, results.len());

        Ok(())
    }

    #[test]
    fn echo() -> Result<(), Box<dyn error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8001")?;
        listener.set_nonblocking(true)?;

        let mut io: IO<8> = IO::new()?;
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
        let socket = match results.remove(0) {
            Completion::Accept(socket, stream) => stream,
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Client connected");

        let buf = Box::new([0u8; BUFFER_SIZE]);
        io.receive(socket.try_clone().unwrap(), buf);
        results = io.poll_timeout(Duration::from_secs(1))?;
        let (buf, len) = match results.remove(0) {
            Completion::Receive(stream, buf, len) => (buf, len),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Received {len} bytes from client");
        assert_eq!(5, len);
        assert_eq!(b"hello", buf[..5].iter().as_slice());

        let mut buf = Box::new([0u8; BUFFER_SIZE]);
        buf[..5].copy_from_slice(b"hello");
        io.send(socket.try_clone().unwrap(), buf, 5);
        results = io.poll_timeout(Duration::from_secs(1))?;
        let (buf, len) = match results.remove(0) {
            Completion::Send(stream, buf, len) => (buf, len),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Sent {} bytes to client", len);

        handler.join().unwrap();

        Ok(())
    }
}
