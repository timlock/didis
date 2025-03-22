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

pub enum Res {
    Accept(TcpStream),
    Close,
    Send((Box<[u8]>, usize)),
    Receive(Box<[u8]>),
}

struct Accept {
    address: *mut sockaddr,
    address_len: *mut socklen_t,
    new_socket: Option<TcpStream>,
}

impl Accept {
    fn new(address: *mut sockaddr, address_len: *mut socklen_t) -> Self {
        Self {
            address,
            address_len,
            new_socket: None,
        }
    }

    fn complete(self) -> TcpStream {
        self.new_socket.unwrap()
    }
}

struct Send {
    buffer: *mut u8,
    capacity: usize,
}
impl Send {
    fn new(buffer: *mut u8, capacity: usize) -> Self {
        Self { buffer, capacity }
    }
}
struct Receive {
    buffer: *mut u8,
}
impl Receive {
    fn new(buffer: *mut u8) -> Self {
        Self { buffer }
    }
    fn complete(self, len: usize) -> Box<[u8]> {
        let buffer = unsafe {
            let slice = std::slice::from_raw_parts_mut(self.buffer, len);
            Box::from_raw(slice)
        };
        buffer
    }
}
enum Task {
    Accept(Accept),
    Close,
    Send(Send),
    Receive(Receive),
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

        let sock_address: *mut sockaddr = Box::into_raw(Box::new(unsafe { zeroed() }));
        let sock_address_len: *mut socklen_t = Box::into_raw(Box::new(size_of::<sockaddr>() as _));

        let task_id = self.add_task(
            Task::Accept(Accept::new(sock_address, sock_address_len)),
            fd,
        );
        unsafe {
            io_uring_prep_accept(sqe, fd, sock_address, sock_address_len, 0);
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

    pub fn receive(&mut self, fd: RawFd, buffer: Box<[u8]>) -> io::Result<()> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let buffer = Box::into_raw(buffer) as *mut u8;
        let task_id = self.add_task(Task::Receive(Receive::new(buffer)), fd);

        unsafe {
            io_uring_prep_recv(sqe, fd, buffer as *mut _, BUFFER_SIZE, 0);
            (*sqe).user_data = task_id;
        }
        Ok(())
    }

    pub fn send(&mut self, fd: RawFd, buffer: Box<[u8]>) -> io::Result<()> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let capacity = buffer.len();
        let buffer = Box::into_raw(buffer) as *mut u8;
        let task_id = self.add_task(Task::Send(Send::new(buffer, capacity)), fd);

        unsafe {
            io_uring_prep_send(sqe, fd, buffer as *const _, BUFFER_SIZE, 0);
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

    pub fn poll(&mut self) -> Vec<Res> {
        let mut results = Vec::new();
        if let Some(cqe) = self.peek_completion() {
            let id = cqe.user_data as u64;
            match self.tasks.remove(&id) {
                Some(task) => match task.task {
                    Task::Accept(accept) => {
                        let socket = unsafe { TcpStream::from_raw_fd(cqe.res) };
                        
                        results.push(Res::Accept(socket));
                    }
                    Task::Close => {}
                    Task::Receive(receive) => {
                        let buffer = unsafe {
                            let slice = std::slice::from_raw_parts_mut(receive.buffer, cqe.res as usize);
                            Box::from_raw(slice)
                        };
                        
                        results.push(Res::Receive(buffer));
                    }
                    Task::Send(send) => {
                        let buffer = unsafe {
                            let slice = std::slice::from_raw_parts_mut(send.buffer, send.capacity);
                            Box::from_raw(slice)
                        };
                        
                        results.push(Res::Send((buffer, cqe.res as usize)));
                    }
                },
                None => {
                    println!("Received completion for unknown task id {}", id);
                }
            };
        }

        results
    }

    fn peek_completion(&mut self) -> Option<io_uring_cqe> {
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
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::str::FromStr;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn accept() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8888")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(QUEUE_DEPTH)?;
        io.accept(listener.as_raw_fd())?;
        io.submit()?;

        let address = SocketAddr::from_str("127.0.0.1:8888")?;
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
}
