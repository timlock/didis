use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::{c_int, c_void};
use std::ptr::null_mut;
use std::{error, io};

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[derive(Debug)]
pub enum Error {
    Init(c_int),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Init(code) => write!(f, "Failed to init kqueue: {code}"),
        }
    }
}

impl error::Error for Error {}

enum Operation {
    Read {
        buf: Vec<u8>,
        callback: Box<dyn FnMut(io::Result<Vec<u8>>) + Send + 'static>,
    },
    Write {
        buf: Vec<u8>,
        callback: Box<dyn FnMut(io::Result<usize>) + Send + 'static>,
    },
}

pub struct IO {
    kqueue: c_int,
    change_list: VecDeque<kevent>,
    operations: HashMap<u64, Operation>,
    last_id: u64,
}

impl IO {
    pub fn new() -> Result<Self, Error> {
        let kqueue = unsafe { kqueue() };
        if kqueue == -1 {
            return Err(Error::Init(kqueue));
        }
        Ok(Self {
            kqueue,
            change_list: Default::default(),
            operations: Default::default(),
            last_id: 0,
        })
    }

    pub fn read<C>(&mut self, fd: RawFd, buf: Vec<u8>, callback: C)
    where
        C: FnMut(io::Result<Vec<u8>>) + Send + 'static,
    {
        let mut operation_id = self.last_id.wrapping_add(1);
        self.operations.insert(
            operation_id,
            Operation::Read {
                buf,
                callback: Box::from(callback),
            },
        );

        let id_ptr = &mut operation_id as *mut u64 as *mut c_void;
        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_READ as i16,
            flags: (EV_ADD | EV_CLEAR) as u16,
            fflags: 0,
            data: 0,
            udata: id_ptr,
        };

        self.change_list.push_back(event);
    }

    pub fn write<C>(&mut self, fd: RawFd, buf: Vec<u8>, callback: C)
    where
        C: FnMut(io::Result<usize>) + Send + 'static,
    {
        let mut operation_id = self.last_id.wrapping_add(1);
        self.operations.insert(
            operation_id,
            Operation::Write {
                buf,
                callback: Box::from(callback),
            },
        );

        let id_ptr = &mut operation_id as *mut u64 as *mut c_void;
        let event = kevent {
            ident: fd as usize,
            filter: EVFILT_WRITE as i16,
            flags: (EV_ADD | EV_CLEAR) as u16,
            fflags: 0,
            data: 0,
            udata: id_ptr,
        };

        self.change_list.push_back(event);
    }
}

impl Drop for IO {
    fn drop(&mut self) {
        unsafe { libc::close(self.kqueue) };
    }
}
