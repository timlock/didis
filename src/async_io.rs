#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub use crate::async_io::linux::*;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use crate::async_io::macos::*;

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

pub enum Completion {
    Accept(TcpListener, io::Result<(TcpStream, SocketAddr)>),
    Send(TcpStream, Box<[u8]>, io::Result<usize>, u64),
    Receive(TcpStream, Box<[u8]>, io::Result<usize>, u64),
}

pub trait AsyncIO{
    fn accept(&mut self, listener: TcpListener);
    fn receive(&mut self, socket: TcpStream, buffer: Box<[u8]>, id: u64);
    fn send(&mut self, socket: TcpStream, buffer: Box<[u8]>, len: usize, id: u64);
    fn poll(&mut self) -> io::Result<Vec<Completion>>;
    fn poll_timeout(&mut self, duration: Duration) -> io::Result<Vec<Completion>>;
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
        io.receive(socket.try_clone().unwrap(), buf, 1);
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