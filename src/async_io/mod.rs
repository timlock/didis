use crate::async_io::slice::{AsSlice, Slice, SliceMut};
use std::io;
use std::net::{SocketAddr, TcpStream};

pub mod buf;
pub mod driver;
pub mod mpsc;
pub mod net;
pub mod runtime;
pub mod slice;

pub trait AsyncAccept {
    fn async_accept(&self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>>;
}

pub trait AsyncRead {
    fn async_read(&self, buf: SliceMut) -> impl Future<Output = (io::Result<usize>, SliceMut)>;
}

pub trait AsyncWrite {
    fn async_write(&mut self, buf: Slice) -> impl Future<Output = (io::Result<usize>, Slice)>;

    fn async_write_all(&mut self, mut buf: Slice) -> impl Future<Output = (io::Result<()>, Slice)> {
        async move {
            while !buf.is_empty() {
                let (result, b) = self.async_write(buf).await;
                buf = b;
                match result {
                    Ok(0) => {
                        return (
                            Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                "failed to write whole buffer",
                            )),
                            buf,
                        );
                    }
                    Ok(n) => buf = buf.slice(n..),
                    Err(e) => return (Err(e), buf),
                }
            }

            (Ok(()), buf)
        }
    }
}
