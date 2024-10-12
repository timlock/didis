use std::{io, net};

pub struct TcpStreamNonBlocking {
    inner: net::TcpStream,
}

impl TcpStreamNonBlocking {
    pub fn new(stream: net::TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self { inner: stream })
    }
}

impl io::Read for TcpStreamNonBlocking {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner.read(buf) {
            Ok(0) => Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
            Ok(size) => Ok(size),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(err) => Err(err),
        }
    }
}

impl io::Write for TcpStreamNonBlocking {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl TryFrom<net::TcpStream> for TcpStreamNonBlocking {
    type Error = io::Error;

    fn try_from(value: net::TcpStream) -> Result<Self, Self::Error> {
        TcpStreamNonBlocking::new(value)
    }
}
