#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use crate::async_io::driver::linux::*;

#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "macos")]
pub use crate::async_io::driver::macos::*;

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use crate::async_io::slice::{Slice, SliceMut};

pub struct Driver {
    inner: IO,
}
impl Driver {
    pub fn new(driver_os: IO) -> Driver {
        Driver { inner: driver_os }
    }

    pub fn accept(
        &mut self,
        listener: TcpListener,
    ) -> TaskHandle<io::Result<(TcpStream, SocketAddr)>> {
        self.inner.accept(listener)
    }
    pub fn send(
        &mut self,
        socket: TcpStream,
        slice: Slice,
    ) -> TaskHandle<(io::Result<usize>, Slice)> {
        self.inner.send(socket, slice)
    }
    pub fn receive(
        &mut self,
        socket: TcpStream,
        buffer: SliceMut,
    ) -> TaskHandle<(io::Result<usize>, SliceMut)> {
        self.inner.receive(socket, buffer)
    }
    pub fn poll(&mut self) -> io::Result<()> {
        let completions = self.inner.poll()?;
        for completion in completions {
            completion.wake();
        }

        Ok(())
    }
    
    pub fn poll_timeout(&mut self, duration: Duration) -> io::Result<()> {
        let completions = self.inner.poll_timeout(duration)?;
        for completion in completions {
            completion.wake();
        }

        Ok(())
    }
}

pub trait DriverOS {
    fn accept(&mut self, listener: TcpListener) -> TaskHandle<io::Result<(TcpStream, SocketAddr)>>;
    fn send(
        &mut self,
        socket: TcpStream,
        buffer: Slice,
    ) -> TaskHandle<(io::Result<usize>, Slice)>;
    fn receive(
        &mut self,
        socket: TcpStream,
        buffer: SliceMut,
    ) -> TaskHandle<(io::Result<usize>, SliceMut)>;
    fn poll(&mut self) -> io::Result<Vec<Completion>>;
    fn poll_timeout(&mut self, duration: Duration) -> io::Result<Vec<Completion>>;
}

pub enum Completion {
    Accept(
        io::Result<(TcpStream, SocketAddr)>,
        TaskHandle<io::Result<(TcpStream, SocketAddr)>>,
    ),
    Send(
        Slice,
        io::Result<usize>,
        TaskHandle<(io::Result<usize>, Slice)>,
    ),
    Receive(
        SliceMut,
        io::Result<usize>,
        TaskHandle<(io::Result<usize>, SliceMut)>,
    ),
}

impl Completion {
    pub fn wake(self) {
        match self {
            Completion::Accept(result, handle) => {
                handle.result.lock().unwrap().replace(result);
                handle.waker.lock().unwrap().as_ref().unwrap().wake_by_ref();
            }
            Completion::Send(buf, len, handle) => {
                handle.result.lock().unwrap().replace((len, buf));
                handle.waker.lock().unwrap().as_ref().unwrap().wake_by_ref();
            }
            Completion::Receive(buf, len, handle) => {
                handle.result.lock().unwrap().replace((len, buf));
                handle.waker.lock().unwrap().as_ref().unwrap().wake_by_ref();
            }
        }
    }
}

enum Task {
    Accept(TcpListener, TaskHandle<io::Result<(TcpStream, SocketAddr)>>),
    Send(
        TcpStream,
        Slice,
        TaskHandle<(io::Result<usize>, Slice)>,
    ),
    Receive(
        TcpStream,
        SliceMut,
        TaskHandle<(io::Result<usize>, SliceMut)>,
    ),
}

pub struct TaskHandle<T> {
    result: Arc<Mutex<Option<T>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Clone for TaskHandle<T> {
    fn clone(&self) -> Self {
        TaskHandle {
            result: self.result.clone(),
            waker: self.waker.clone(),
        }
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let new_waker = cx.waker();
        let mut current_waker = self.waker.lock().unwrap();

        if current_waker
            .as_ref()
            .is_some_and(|current_waker| current_waker.will_wake(new_waker))
            || current_waker.is_none()
        {
            *current_waker = Some(new_waker.clone());
        }

        match self.result.lock().unwrap().take() {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::async_io::slice::AsSlice;
use super::*;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::str::FromStr;
    use std::time::Duration;
    use std::{error, thread};
    use crate::async_io::slice::AsSliceMut;

    #[test]
    fn accept() -> Result<(), Box<dyn error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:8000")?;
        listener.set_nonblocking(true)?;

        let mut io = IO::new(1)?;
        io.accept(listener.try_clone()?);

        let address = SocketAddr::from_str("127.0.0.1:8000")?;
        let handler = thread::spawn(move || {
            println!("Connect to server");
            let result = TcpStream::connect_timeout(&address, Duration::from_secs(5));
            match result {
                Ok(_) => println!("CONNECTED"),
                Err(err) => println!("Could not connect {err}"),
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
        io.accept(listener.try_clone()?);

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
            Completion::Accept(stream, _) => stream?,
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Client connected");

        let buf = Box::new([0u8; 1024]);
        io.receive(socket.try_clone()?, buf.slice_mut(..));
        results = io.poll_timeout(Duration::from_secs(1))?;
        let (buf, len) = match results.remove(0) {
            Completion::Receive(buf, len, _) => (buf, len?),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Received {len} bytes from client");
        assert_eq!(5, len);
        assert_eq!(b"hello", buf[..5].iter().as_slice());

        let mut buf = Box::new([0u8; 1024]);
        buf[..5].copy_from_slice(b"hello");
        io.send(socket.try_clone()?, buf.slice(..5));
        results = io.poll_timeout(Duration::from_secs(1))?;
        let (_, len) = match results.remove(0) {
            Completion::Send(buf, len, _) => (buf, len?),
            _ => return Err("Should be accept".into()),
        };
        eprintln!("Sent {} bytes to client", len);

        handler.join().unwrap();

        Ok(())
    }
}
