use std::{io, net};

pub struct SocketNonBlocking {
    inner: net::TcpStream,
}

impl SocketNonBlocking {
    pub fn new(stream: net::TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self { inner: stream })
    }
}

impl io::Read for SocketNonBlocking {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner.read(buf) {
            Ok(size) if size == 0 => Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
            Ok(size) => Ok(size),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(err) => Err(err),
        }
    }
}

impl io::Write for SocketNonBlocking{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}