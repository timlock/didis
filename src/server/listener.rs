use crate::server::socket::TcpStreamNonBlocking;
use std::{io, net};

pub struct TcpListenerNonBlocking {
    inner: net::TcpListener,
}

impl TcpListenerNonBlocking {
    pub fn bind(address: &str) -> io::Result<Self> {
        let inner = net::TcpListener::bind(address)?;
        inner.set_nonblocking(true)?;
        Ok(Self { inner })
    }

    pub fn accept(&self) -> io::Result<Option<(TcpStreamNonBlocking, net::SocketAddr)>> {
        match self.inner.accept() {
            Ok((stream, address)) => {
                let socket = TcpStreamNonBlocking::try_from(stream)?;
                Ok(Some((socket, address)))
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(err),
        }
    }
}
