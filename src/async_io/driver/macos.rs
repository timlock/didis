use crate::async_io::{DriverOS, Completion, Task, TaskHandle};
use libc::{
    close, kevent, kqueue, timespec, EVFILT_READ, EVFILT_WRITE, EV_ADD, EV_CLEAR, EV_ONESHOT,
};
use std::collections::VecDeque;
use std::io;
use std::io::{Read, Write};
use std::mem::zeroed;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::raw::c_int;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use crate::async_io::driver::{Completion, DriverOS, Task, TaskHandle};
use crate::async_io::slice::{Slice, SliceMut};

impl From<Task> for Completion {
    fn from(value: Task) -> Self {
        match value {
            Task::Accept(socket,handle) => {
                let result = socket.accept();
                Completion::Accept(result, handle)
            }
            Task::Receive(mut socket, mut buf, handle) => {
                let n = socket.read(buf.as_mut());
                Completion::Receive( buf, n, handle)
            }
            Task::Send(mut socket, buf, len,handle) => {
                let n = socket.write(&buf[..len]);
                Completion::Send( buf, n, handle)
            }
        }
    }
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

impl DriverOS for IO {
    fn accept(&mut self, socket: TcpListener) -> TaskHandle<io::Result<(TcpStream, SocketAddr)>>{
        let handle = TaskHandle {
            result: Arc::new(Mutex::new(None)),
            waker: Arc::new(Mutex::new(None)),
        };
        
        let fd = socket.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Accept(socket, handle.clone())));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_READ,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: task_ptr as _,
        };

        self.pending.push_back(event);
        handle
    }

    fn receive(
        &mut self,
        socket: TcpStream,
        buffer: SliceMut,
    ) -> TaskHandle<(io::Result<usize>, SliceMut)> {
        let handle = TaskHandle {
            result: Arc::new(Mutex::new(None)),
            waker: Arc::new(Mutex::new(None)),
        };
        
        let fd = socket.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Receive(socket, buffer, handle.clone())));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_READ,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: task_ptr as _,
        };

        self.pending.push_back(event);
        handle.clone()
    }

    fn send(
        &mut self,
        socket: TcpStream,
        buffer: Slice,
    ) -> TaskHandle<(io::Result<usize>, Slice)> {
        let handle = TaskHandle {
            result: Arc::new(Mutex::new(None)),
            waker: Arc::new(Mutex::new(None)),
        };
        
        let fd = socket.as_raw_fd();
        let task_ptr = Box::into_raw(Box::new(Task::Send(socket, buffer, handle.clone())));

        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_WRITE,
            flags: EV_ADD | EV_CLEAR | EV_ONESHOT,
            fflags: 0,
            data: 0,
            udata: task_ptr as _,
        };

        self.pending.push_back(event);
        handle
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
