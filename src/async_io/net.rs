use crate::async_io::runtime::CURRENT;
use crate::async_io::slice::{Slice, SliceMut};
use crate::async_io::{AsyncAccept, AsyncRead, AsyncWrite};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};


impl AsyncAccept for TcpListener {
    fn async_accept(&self) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> {
        CURRENT.with_borrow(|executor| {
            executor
                .as_ref()
                .unwrap()
                .driver
                .borrow_mut()
                .accept(self.try_clone().unwrap())
        })
    }
}


impl AsyncRead for TcpStream {
    fn async_read(&self, buf: SliceMut) -> impl Future<Output = (io::Result<usize>, SliceMut)> {
        CURRENT.with_borrow(|executor| {
            executor
                .as_ref()
                .unwrap()
                .driver
                .borrow_mut()
                .receive(self.try_clone().unwrap(), buf)
        })
    }
}



impl AsyncWrite for TcpStream {
    fn async_write(&mut self, buf: Slice) -> impl Future<Output = (io::Result<(usize)>, Slice)> {
        CURRENT.with_borrow(|executor| {
            executor
                .as_ref()
                .unwrap()
                .driver
                .borrow_mut()
                .send(self.try_clone().unwrap(), buf)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_io::driver::IO;
    use crate::async_io::runtime::{Runtime, spawn};
    use crate::async_io::slice::{AsSlice, AsSliceMut};
    use std::error;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::str::FromStr;
    use std::time::Duration;

    #[test]
    fn accept() -> Result<(), Box<dyn error::Error>> {
        let runtime = Runtime::new(IO::new(256)?);
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:8000")?;
            listener.set_nonblocking(true)?;

            let handle = listener.async_accept();

            let task_handle = spawn(async {
                let address = SocketAddr::from_str("127.0.0.1:8000").unwrap();
                TcpStream::connect_timeout(&address, Duration::from_secs(5))
            });

            handle.await?;
            task_handle.await?;
            Ok(())
        })
    }

    #[test]
    fn echo() -> Result<(), Box<dyn error::Error>> {
        let runtime = Runtime::new(IO::new(256)?);
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:8001")?;
            listener.set_nonblocking(true)?;

            let handle = listener.async_accept();

            spawn(async {
                eprintln!("Connect to server");
                let address = SocketAddr::from_str("127.0.0.1:8001").unwrap();
                let mut socket =
                    TcpStream::connect_timeout(&address, Duration::from_secs(5)).unwrap();

                eprintln!("Sending hello to server");
                let sent = b"hello";
                let (len, buf) = socket
                    .async_write(sent.to_vec().slice(..sent.len()))
                    .await;
                assert_eq!(sent.len(), len.unwrap());

                eprintln!("Waiting for response from server");
                let received = Box::new([0u8; 5]);
                let (len, received) = socket.async_read(received.slice_mut(..)).await;
                assert_eq!(sent.len(), len.unwrap());

                eprintln!("Received {:?} from server", received);
                assert_eq!(sent.as_slice(), received.to_vec().as_slice());
            });

            let (mut stream, _) = handle.await?;
            eprintln!("Client connected");

            let buf = Box::new([0u8; 1024]);
            let (len, buf) = stream.async_read(buf.slice_mut(..)).await;
            let len = len?;
            eprintln!("Received {len} bytes from client");
            assert_eq!(5, len);
            assert_eq!(b"hello", buf[..5].iter().as_slice());

            let mut buf = Box::new([0u8; 1024]);
            buf[..5].copy_from_slice(b"hello");
            let (sent, buf) = stream.async_write(buf.slice(..5)).await;
            eprintln!("Sent {} bytes to client", sent?);
            Ok(())
        })
    }
}
