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

enum Task {
    Accept((*mut sockaddr, *mut usize)),
    Close,
    Receive(*mut u8),
    Send(*const u8),
}

impl Drop for Task {
    fn drop(&mut self) {
        match self {
            Task::Accept((address, address_len)) => unsafe {
                let _ = Box::from_raw(address);
                let _ = Box::from_raw(address_len);
            },
            Task::Receive(buffer) => unsafe {
                let _ = Box::from_raw(buffer);
            },
            Task::Send(buffer) => unsafe {
                let _ = Box::from_raw(buffer);
            },
            _ => {}
        }
    }
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

    pub fn accept<C>(&mut self, fd: RawFd, callback :C) -> Result<(), io::Error>
    where
        C: FnMut((*mut sockaddr, *mut usize)) + Send + 'static,
    {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        let sock_address = Box::into_raw(Box::new(unsafe { zeroed() }));
        let sock_address_len = Box::into_raw(Box::new(size_of::<sockaddr>()));

        let task_id = self.add_task(Task::Accept((sock_address, sock_address_len)), fd);
        unsafe {
            io_uring_prep_accept(sqe, fd, sock_address, sock_address_len as *mut _, 0);
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

    fn peek_completion(&mut self) -> Option<io_uring_cqe> {
        let mut cqe: *mut io_uring_cqe = null_mut();
        let ret = unsafe { io_uring_peek_cqe(&mut self.ring, &mut cqe) };

        if ret < 0 || cqe.is_null() {
            let last_err = io::Error::last_os_error();
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
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::thread;

    #[test]
    fn accept() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let mut io = IO::new(QUEUE_DEPTH)?;
        io.accept(listener.as_raw_fd(), |(address, len)| {

        })?;
        io.submit()?;

        let handler = thread::spawn(||{
            let mut stream = TcpStream::connect("127.0.0.1:0").unwrap();
            return 
        });
        
        io.peek_completion();
        
        handler.join().unwrap();

        Ok(())
    }
}
pub fn test_uring() -> io::Result<()> {
    let queue_depth: u32 = 1;
    let mut ring = setup_io_uring(queue_depth)?;

    println!("Submitting NOP operation");
    submit_noop(&mut ring)?;

    println!("Waiting for completion");
    wait_for_completion(&mut ring)?;

    unsafe { io_uring_queue_exit(&mut ring) };
    Ok(())
}

fn setup_io_uring(queue_depth: u32) -> io::Result<io_uring> {
    let mut ring: io_uring = unsafe { zeroed() };
    let ret = unsafe { io_uring_queue_init(queue_depth, &mut ring, 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(ring)
}

fn submit_noop(ring: &mut io_uring) -> io::Result<()> {
    unsafe {
        let sqe = io_uring_get_sqe(ring);
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        io_uring_prep_nop(sqe);
        (*sqe).user_data = 0x88;

        let ret = io_uring_submit(ring);
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
    }

    Ok(())
}

fn wait_for_completion(ring: &mut io_uring) -> io::Result<()> {
    let mut cqe: *mut io_uring_cqe = null_mut();
    let ret = unsafe { io_uring_wait_cqe(ring, &mut cqe) };

    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    unsafe {
        println!("NOP completed with result: {}", (*cqe).res);
        println!("User data: 0x{:x}", (*cqe).user_data);
        io_uring_cqe_seen(ring, cqe);
    }

    Ok(())
}
