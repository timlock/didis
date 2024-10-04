use std::{net, io};
use crate::server::socket::SocketNonBlocking;

pub struct TcpListenerNonBlocking {
    inner: net::TcpListener,
}

impl TcpListenerNonBlocking {
    pub fn bind(address: &str) -> io::Result<Self> {
        let inner = net::TcpListener::bind(address)?;
        inner.set_nonblocking(true)?;
        Ok(Self { inner })
    }

    pub fn accept(&self) -> io::Result<Option<(SocketNonBlocking, net::SocketAddr)>> {
        match self.inner.accept() {
            Ok((stream, address)) => {
                let socket = SocketNonBlocking::new(stream)?;
                Ok(Some((socket, address)))
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(err),
        }
    }
}

