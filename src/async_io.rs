#[cfg(target_os = "linux")]
mod linux;

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;
#[cfg(target_os = "linux")]
pub use crate::async_io::linux::*;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use crate::async_io::macos::*;

pub enum Completion {
    Accept(TcpListener, io::Result<(TcpStream, SocketAddr)>),
    Send(TcpStream, Box<[u8]>, io::Result<usize>),
    Receive(TcpStream, Box<[u8]>, io::Result<usize>),
}

pub trait AsyncIO{
    fn accept(&mut self, listener: TcpListener);
    fn receive(&mut self, socket: TcpStream, buffer: Box<[u8]>);
    fn send(&mut self, socket: TcpStream, buffer: Box<[u8]>, len: usize);
    fn poll(&mut self) -> io::Result<Vec<Completion>>;
    fn poll_timeout(&mut self, duration: Duration) -> io::Result<Vec<Completion>>;
}