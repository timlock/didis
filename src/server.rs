use socket::TcpStreamNonBlocking;

use crate::parser::{command, resp, resp_consuming};
use crate::server::listener::TcpListenerNonBlocking;
use std::{cell, rc};
use std::{
    collections::HashMap,
    io::{self},
    net::SocketAddr,
};

mod listener;
mod socket;

pub struct Server {
    listener: TcpListenerNonBlocking,
    pub connections: HashMap<SocketAddr, Connection>,
}

impl Server {
    pub fn new(address: &str) -> io::Result<Self> {
        let listener = TcpListenerNonBlocking::bind(address)?;
        Ok(Server {
            listener,
            connections: HashMap::new(),
        })
    }

    pub fn accept_connections(&mut self) -> io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok(None) => return Ok(()),
                Ok(Some((stream, address))) => {
                    println!("new connection: {address}");
                    self.connections.insert(address, Connection::from(stream));
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn disconnect(&mut self, address: SocketAddr) {
        self.connections.remove(&address);
    }
}

pub struct Connection {
    // pub incoming: command::Decoder<IoRef<TcpStreamNonBlocking>>,
    pub incoming: command::ConsumingDecoder<IoRef<TcpStreamNonBlocking>>,
    pub outgoing: IoRef<TcpStreamNonBlocking>,
}
impl Connection {
    pub fn new(stream: TcpStreamNonBlocking) -> Self {
        let socket_ref = IoRef::from(stream);
        // let resp_decoder = resp::Decoder::new(socket_ref.clone());
        // let command_decoder = command::Decoder::new(resp_decoder);

        let consuming_resp_decoder = resp_consuming::Decoder::new(socket_ref.clone());
        let command_decoder = command::ConsumingDecoder::new(consuming_resp_decoder);

        Self {
            incoming: command_decoder,
            outgoing: socket_ref,
        }
    }
}

impl From<TcpStreamNonBlocking> for Connection {
    fn from(value: TcpStreamNonBlocking) -> Self {
        Connection::new(value)
    }
}

pub struct IoRef<T> {
    inner: rc::Rc<cell::RefCell<T>>,
}

impl<T> IoRef<T>
where
    T: io::Read + io::Write,
{
    pub fn new(stream: T) -> Self {
        Self {
            inner: rc::Rc::new(cell::RefCell::new(stream)),
        }
    }
}

impl<T> From<T> for IoRef<T>
where
    T: io::Read + io::Write,
{
    fn from(value: T) -> Self {
        IoRef::new(value)
    }
}

impl<T> io::Read for IoRef<T>
where
    T: io::Read + io::Write,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.borrow_mut().read(buf)
    }
}

impl<T> io::Write for IoRef<T>
where
    T: io::Read + io::Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.borrow_mut().flush()
    }
}

impl<T> Clone for IoRef<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
