#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[cfg(not(rust_analyzer))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::collections::{HashMap, VecDeque};
use std::mem::zeroed;
use std::os::fd::RawFd;
use std::ptr::null_mut;
use std::{io, ptr};

const QUEUE_DEPTH: u32 = 256;

const BUFFER_SIZE: usize = 1024;

struct Accept<'a> {
    address: *mut sockaddr,
    address_len: *mut socklen_t,
    callback_fn: Box<dyn FnMut((sockaddr, socklen_t)) + Send + 'a>,
}

impl<'a> Accept<'a> {
    fn new(
        address: *mut sockaddr,
        address_len: *mut socklen_t,
        callback_fn: impl FnMut((sockaddr, socklen_t)) + Send + 'a,
    ) -> Self {
        Self {
            address,
            address_len,
            callback_fn: Box::new(callback_fn),
        }
    }
    fn complete(mut self) {
        unsafe {
            let address = Box::from_raw(self.address);
            let address_len = Box::from_raw(self.address_len);
            (self.callback_fn)((*address, *address_len));
        }
    }
}

enum Task<'a> {
    Accept(Accept<'a>),
    Close,
    Receive(*mut u8),
    Send(*const u8),
}

struct TaskData<'a> {
    task: Task<'a>,
    file_descriptor: RawFd,
}

pub struct IO<'a> {
    ring: io_uring,
    task_queue: VecDeque<TaskData<'a>>,
    tasks: HashMap<u64, TaskData<'a>>,
    next_id: u64,
}

impl<'a> IO<'a> {
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

    pub fn accept(
        &mut self,
        fd: RawFd,
        callback_fn: impl FnMut((sockaddr, socklen_t)) + Send + 'a,
    ) -> Result<(), io::Error> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let sock_address: *mut sockaddr = Box::into_raw(Box::new(unsafe { zeroed() }));
        let sock_address_len: *mut socklen_t = Box::into_raw(Box::new(size_of::<sockaddr>() as _));

        let task_id = self.add_task(
            Task::Accept(Accept::new(sock_address, sock_address_len, callback_fn)),
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

    pub fn receive(&mut self, fd: RawFd) -> io::Result<()> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let buffer = Box::into_raw(Box::new([0u8; BUFFER_SIZE])) as *mut u8;
        let task_id = self.add_task(Task::Receive(buffer), fd);

        unsafe {
            io_uring_prep_recv(sqe, fd, buffer as *mut _, BUFFER_SIZE, 0);
            (*sqe).user_data = task_id;
        }
        Ok(())
    }

    pub fn send(&mut self, fd: RawFd, buffer: *const u8, len: usize) -> io::Result<()> {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let task_id = self.add_task(Task::Send(buffer), fd);

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

    pub fn poll(&mut self) {
        if let Some(cqe) = self.peek_completion() {
            let id = cqe.user_data as u64;
            match self.tasks.remove(&id) {
                Some(task) => match task.task {
                    Task::Accept(accept) => {
                        accept.complete();
                    }
                    Task::Close => {}
                    Task::Receive(_) => {}
                    Task::Send(_) => {}
                },
                None => {
                    println!("Received completion for unknown task id {}", id);
                }
            };
        }
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

    fn add_task(&mut self, task: Task<'a>, fd: RawFd) -> u64 {
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

impl<'a> Drop for IO<'a> {
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
        let mut a = 1;
        io.accept(listener.as_raw_fd(), |(address, len)| {
            println!("accept client {:?} {}", address, len);
            let b = 1 + a;
        })?;
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

        io.poll();

        println!("{}", a);
        Ok(())
    }
}
