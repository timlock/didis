use crate::async_io::AsyncWrite;
use crate::async_io::slice::{AsSlice, Slice};
use std::io;

pub struct BufWriter<T: AsyncWrite> {
    buf: Option<Vec<u8>>,
    writer: T,
    b: Vec<u8>,
}

impl<T: AsyncWrite> BufWriter<T> {
    pub fn new(writer: T) -> BufWriter<T> {
        BufWriter {
            buf: Some(Vec::with_capacity(8 * 1024)),
            writer,
            b: Vec::new(),
        }
    }
    pub async fn flush(&mut self) -> io::Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let slice = self.buf.take().unwrap().slice(..);
        let (result, slice) = self.writer.async_write_all(slice).await;
        result?;
        let mut buf = slice.into_inner();
        buf.clear();
        self.buf = Some(buf);

        Ok(())
    }

    pub fn get_ref(&self) -> &T {
        &self.writer
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.writer
    }

    fn with_buf<F>(&mut self, mut f: F)
    where
        F: FnMut(Vec<u8>) -> Vec<u8>,
    {
        let buf = f(self.buf.take().unwrap());
        self.buf = Some(buf);
    }

    async fn with_buf_async<F>(&mut self, mut f: F)
    where
        F: AsyncFnMut(Vec<u8>) -> Vec<u8>,
    {
        let buf = f(self.buf.take().unwrap()).await;
        self.buf = Some(buf);
    }

    fn remaining_space(&self) -> usize {
        let buf = self
            .buf
            .as_ref()
            .expect("buf should be present when calling remaining_space()");
        buf.capacity() - buf.len()
    }

    fn len(&self) -> usize {
        let buf = self
            .buf
            .as_ref()
            .expect("buf should be present when calling remaining_space()");
        buf.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn write_to_buf(&mut self, bytes: &[u8]) {
        self.with_buf(|mut inner_buf| {
            inner_buf.extend_from_slice(bytes);
            inner_buf
        });
    }
}

impl<T: AsyncWrite> AsyncWrite for BufWriter<T> {
    async fn async_write(&mut self, buf: Slice) -> (io::Result<usize>, Slice) {
        if self.remaining_space() > buf.len() {
            self.write_to_buf(buf.as_ref());
            return (Ok(buf.len()), buf);
        }

        if let Err(err) = self.flush().await {
            return (Err(err), buf);
        }

        if self.remaining_space() < buf.len() {
            let (result, buf) = self.writer.async_write_all(buf).await;
            let result = match result {
                Ok(()) => Ok(buf.len()),
                Err(err) => Err(err),
            };
            return (result, buf);
        }

        self.write_to_buf(buf.as_ref());
        (Ok(buf.len()), buf)
    }
}
