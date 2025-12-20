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
            Task::Receive(socket, received, id) => {
                if cqe.res < 0 {
                    return Completion::Receive(
                        socket,
                        received,
                        Err(io::Error::from_raw_os_error(-cqe.res)),
                        id
                    );
                }
                Completion::Receive(socket, received, Ok(cqe.res as _), id)
            }
            Task::Send(socket, sent, id) => {
                if cqe.res < 0 {
                    return Completion::Send(
                        socket,
                        sent,
                        Err(io::Error::from_raw_os_error(-cqe.res)),
                        id
                    );
                }
                Completion::Send(socket, sent, Ok(cqe.res as _), id)
            }
        }
    }
}

enum Task {
    Accept(TcpListener),
    Send(TcpStream, Box<[u8]>, u64),
    Receive(TcpStream, Box<[u8]>, u64),
}

pub struct IO {
    ring: io_uring,
    pending: VecDeque<Task>,
    queue_depth: usize,
}

impl IO {
    pub fn new(queue_depth: usize) -> io::Result<Self> {
        let mut ring: io_uring = unsafe { zeroed() };
        let ret = unsafe { io_uring_queue_init(queue_depth as _, &mut ring, 0) };
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

        let mut cqes = vec![null_mut(); self.queue_depth];

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

    fn receive(&mut self, socket: TcpStream, mut buffer: Box<[u8]>, id: u64) {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            self.pending.push_back(Task::Receive(socket, buffer, id));
            return;
        }

        let fd = socket.as_raw_fd();
        let buf_ptr = buffer.as_mut_ptr();
        let buf_len = buffer.len();
        let task_ptr = Box::into_raw(Box::new(Task::Receive(socket, buffer, id)));

        unsafe {
            io_uring_prep_recv(sqe, fd, buf_ptr as _, buf_len, 0);
            (*sqe).user_data = task_ptr as _;
        }
    }

    fn send(&mut self, socket: TcpStream, buffer: Box<[u8]>, len: usize, id: u64) {
        let sqe = unsafe { io_uring_get_sqe(&mut self.ring) };
        if sqe.is_null() {
            self.pending.push_back(Task::Send(socket, buffer, id));
            return;
        }

        let fd = socket.as_raw_fd();
        let mut buffer = buffer;
        let buf_ptr = buffer.as_mut_ptr();
        let task_ptr = Box::into_raw(Box::new(Task::Send(socket, buffer, id)));

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
