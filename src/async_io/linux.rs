#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[cfg(not(rust_analyzer))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::collections::{HashMap, VecDeque};
use std::mem::zeroed;
use std::net::TcpStream;
use std::os::fd::{FromRawFd, RawFd};
use std::ptr::null_mut;
use std::{io, ptr};

const QUEUE_DEPTH: u32 = 256;

const BUFFER_SIZE: usize = 1024;

pub enum Completion {
    Accept(TcpStream),
    Close(RawFd),
    Send(BufPtr),
    Receive(BufPtr),
}

#[derive(Debug)]
struct BufPtr {
    buffer: *mut u8,
    len: usize,
}
impl BufPtr {
    fn new(buffer: Box<[u8; BUFFER_SIZE]>) -> Self {
        let buffer = Box::into_raw(buffer) as *mut u8;
        Self { buffer, len: 0 }
    }

    fn complete(self) -> (Box<[u8]>, usize) {
        let buffer = unsafe {
            let slice = std::slice::from_raw_parts_mut(self.buffer, BUFFER_SIZE);
            Box::from_raw(slice)
        };
        (buffer, self.len)
    }
}

enum Task {
    Accept,
    Close,
    Send(BufPtr),
    Receive(BufPtr),
}

struct TaskData {
    task: Task,
    file_descriptor: RawFd,
}

pub struct IO {
    ring: io_uring,
    task_queue: VecDeque<TaskData>,
    tasks: HashMap<u64, TaskData>,
    next_id: u64,
}

impl IO {
    pub fn new(queue_depth: u32) -> Result<Self, io::Error> {
        let mut ring: io_uring = unsafe { zeroed() };
        let ret = unsafe { io_uring_queue_init(queue_depth, &mut ring, 0) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            ring,
            task_queue: Default::default(),
            tasks: Default::default(),
            next_id: 0,
        })
    }

    pub fn accept(&mut self, fd: RawFd) -> Result<(), io::Error> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let task_id = self.add_task(Task::Accept, fd);
        unsafe {
            io_uring_prep_accept(sqe, fd, null_mut(), null_mut(), 0);
            (*sqe).user_data = task_id;
        }

        Ok(())
    }

    pub fn close(&mut self, fd: RawFd) -> Result<(), io::Error> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let task_id = self.add_task(Task::Close, fd);
        unsafe {
            io_uring_prep_close(sqe, fd);
            (*sqe).user_data = task_id;
        }
        Ok(())
    }

    pub fn receive(&mut self, fd: RawFd, buffer: Box<[u8; BUFFER_SIZE]>) -> io::Result<()> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let wrapper = BufPtr::new(buffer);
        let buf_ptr = wrapper.buffer as *mut _;
        let task_id = self.add_task(Task::Receive(wrapper), fd);

        unsafe {
            io_uring_prep_recv(sqe, fd, buf_ptr, BUFFER_SIZE, 0);
            (*sqe).user_data = task_id;
        }
        Ok(())
    }

    pub fn send(
        &mut self,
        fd: RawFd,
        buffer: Box<[u8; BUFFER_SIZE]>,
        len: usize,
    ) -> io::Result<()> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let wrapper = BufPtr::new(buffer);
        let buf_ptr = wrapper.buffer as *mut _;
        let task_id = self.add_task(Task::Send(wrapper), fd);

        unsafe {
            io_uring_prep_send(sqe, fd, buf_ptr, len, 0);
            (*sqe).user_data = task_id;
        }
        Ok(())
    }

    pub fn submit(&mut self) -> io::Result<usize> {
        let ret = unsafe { io_uring_submit(&mut self.ring) };

        if ret < 0 {
            Err(io::Error::from_raw_os_error(-ret))
        } else {
            Ok(ret as usize)
        }
    }

    pub fn poll(&mut self) -> Vec<Completion> {
        let mut results = Vec::new();
        while let Some(cqe) = self.peek_for_completion() {
            if let Some(result) = self.handle(cqe) {
                results.push(result);
            }
        }

        results
    }

    pub fn poll_blocking(&mut self) -> io::Result<Vec<Completion>> {
        let cqe = self.wait_for_completion()?;
        let first_res = self.handle(cqe);
        let mut results = self.poll();

        if let Some(res) = first_res {
            results.insert(0, res);
        }

        Ok(results)
    }

    fn handle(&mut self, cqe: io_uring_cqe) -> Option<Completion> {
        let id = cqe.user_data as u64;
        match self.tasks.remove(&id) {
            Some(task) => match task.task {
                Task::Accept => {
                    let socket = unsafe { TcpStream::from_raw_fd(cqe.res) };
                    Some(Completion::Accept(socket))
                }
                Task::Close => Some(Completion::Close(task.file_descriptor)),
                Task::Receive(mut receive) => {
                    receive.len = cqe.res as usize;
                    Some(Completion::Receive(receive))
                }
                Task::Send(mut send) => {
                    send.len = cqe.res as usize;
                    Some(Completion::Send(send))
                }
            },
            None => {
                eprintln!("Received completion for unknown task id {}", id);
                None
            }
        }
    }

    fn peek_for_completion(&mut self) -> Option<io_uring_cqe> {
        let mut cqe: *mut io_uring_cqe = null_mut();
        let ret = unsafe { io_uring_peek_cqe(&mut self.ring, &mut cqe) };

        if ret < 0 || cqe.is_null() {
            None
        } else {
            let result = unsafe { ptr::read(cqe) };
            unsafe { io_uring_cqe_seen(&mut self.ring, cqe) };
            Some(result)
        }
    }

    fn wait_for_completion(&mut self) -> io::Result<io_uring_cqe> {
        let mut cqe: *mut io_uring_cqe = null_mut();
        let ret = unsafe { io_uring_wait_cqe(&mut self.ring, &mut cqe) };

        if ret < 0 || cqe.is_null() {
            Err(io::Error::from_raw_os_error(-ret))
        } else {
            let result = unsafe { ptr::read(cqe) };
            unsafe { io_uring_cqe_seen(&mut self.ring, cqe) };
            Ok(result)
        }
    }

    fn add_task(&mut self, task: Task, fd: RawFd) -> u64 {
        let user_data = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        self.tasks.insert(
            user_data,
            TaskData {
                task,
                file_descriptor: fd,
            },
        );
        user_data
    }
}

impl Drop for IO {
    fn drop(&mut self) {
        unsafe { io_uring_queue_exit(&mut self.ring) };
    }
}

mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::str::FromStr;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn accept() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8000")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(QUEUE_DEPTH)?;
        io.accept(listener.as_raw_fd())?;
        io.submit()?;

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

        let results = io.poll();
        assert_eq!(1, results.len());

        Ok(())
    }

    #[test]
    fn echo() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8001")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(QUEUE_DEPTH)?;
        io.accept(listener.as_raw_fd())?;
        io.submit()?;

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

        let mut results = io.poll_blocking()?;
        assert_eq!(1, results.len());
        let socket = match results.remove(0) {
            Completion::Accept(socket) => socket,
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Client connected");

        let buf = Box::new([0u8; BUFFER_SIZE]);
        io.receive(socket.as_raw_fd(), buf)?;
        io.submit()?;
        results = io.poll_blocking()?;
        let (buf, len) = match results.remove(0) {
            Completion::Receive(buf) => buf.complete(),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Received {len} bytes from client");
        assert_eq!(5, len);
        assert_eq!(b"hello", buf[..5].iter().as_slice());

        let mut buf = Box::new([0u8; BUFFER_SIZE]);
        buf[..5].copy_from_slice(b"hello");
        io.send(socket.as_raw_fd(), buf, 5)?;
        io.submit()?;
        results = io.poll_blocking()?;
        let (buf, len) = match results.remove(0) {
            Completion::Send(buf) => buf.complete(),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Sent {} bytes to client", len);

        handler.join().unwrap();

        Ok(())
    }
}
