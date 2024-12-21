use std::io;
use std::io::{IoSlice, IoSliceMut, Read, Write};

#[derive(Debug)]
pub struct RingBuffer<const N: usize> {
    buf: [u8; N],
    read_pos: usize,
    write_pos: usize,
}

impl<const N: usize> RingBuffer<N> {
    pub fn push(&mut self, value: u8) -> bool {
        if self.full() {
            return false;
        }

        let pos = self.write_pos % N;
        self.buf[pos] = value;
        self.write_pos = (self.write_pos + 1) % (N * 2);
        return true;
    }

    pub fn pop(&mut self) -> Option<u8> {
        if self.empty() {
            return None;
        }

        let pos = self.read_pos % N;
        let content = self.buf[pos];
        self.read_pos = (self.read_pos + 1) % (N * 2);

        Some(content)
    }

    pub fn empty(&self) -> bool {
        self.write_pos == self.read_pos
    }

    pub fn full(&self) -> bool {
        self.size() == N
    }

    pub fn size(&self) -> usize {
        self.write_pos - self.read_pos
    }
    pub fn populate(&mut self, reader: &mut impl Read) -> io::Result<usize> {
        let (first, second) = self.writeable_ranges_ref();
        let mut slices = [IoSliceMut::new(first), IoSliceMut::new(second)];

        let n = reader.read_vectored(&mut slices)?;
        self.write_pos += n;
        self.write_pos = self.write_pos % (N * 2);

        Ok(n)
    }

    pub fn content(&mut self) -> Vec<u8> {
        let mut buf = vec![0; self.size()];
        self.read(&mut buf)
            .expect("reading from ringbuffer should not fail");
        buf
    }

    fn writeable_ranges_ref(&mut self) -> (&mut [u8], &mut [u8]) {
        if self.full() {
            return (&mut [], &mut []);
        }

        if self.empty() {
            return (self.buf.as_mut_slice(), &mut []);
        }

        let read_pos = self.read_pos % N;
        let write_pos = self.write_pos % N;

        if write_pos < read_pos {
            return (
                &mut self.buf[write_pos..read_pos],
                &mut [],
            );
        }

        let (first, second) = self.buf.split_at_mut(write_pos);
        (&mut first[..read_pos], second)
    }

    fn readable_ranges_ref(&mut self) -> (&mut [u8], &mut [u8]) {
        if self.empty() {
            return (&mut [], &mut []);
        }

        let read_pos = self.read_pos % N;
        let write_pos = self.write_pos % N;

        if read_pos < write_pos {
            return (
                &mut self.buf[read_pos..write_pos],
                &mut [],
            );
        }

        let is_full = self.full();

        let (first, second) = self.buf.split_at_mut(read_pos);
        if is_full {
            return (second, first);
        }
        (first, &mut second[..write_pos])
    }
}

impl<const N: usize> Default for RingBuffer<N> {
    fn default() -> Self {
        Self {
            buf: [0; N],
            read_pos: Default::default(),
            write_pos: Default::default(),
        }
    }
}

impl<const N: usize> Read for RingBuffer<N> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let (first, second) = self.readable_ranges_ref();
        let slices = [IoSlice::new(first), IoSlice::new(second)];

        let n = buf.write_vectored(&slices)?;
        self.read_pos = (self.read_pos + n) % (N * 2);

        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::io::Cursor;

    use super::*;

    #[test]
    fn populate() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"12345678".to_vec());
        let mut ringbuffer = RingBuffer::<8>::default();

        ringbuffer.populate(&mut input)?;

        let output = ringbuffer.content();
        assert_eq!(b"12345678", output.as_slice());
        Ok(())
    }

    #[test]
    fn over_populate() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"12345678".to_vec());
        let mut ringbuffer = RingBuffer::<4>::default();

        let n = ringbuffer.populate(&mut input)?;

        assert_eq!(4, n);

        let output = ringbuffer.content();
        assert_eq!(b"1234", output.as_slice());
        Ok(())
    }

    #[test]
    fn write_cursor_overflow() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"12345678".to_vec());
        let mut ringbuffer = RingBuffer::<8>::default();

        ringbuffer.populate(&mut input)?;

        let mut output = vec![0;4];
        ringbuffer.read(&mut output)?;
        assert_eq!(b"1234", output.as_slice());

        input = Cursor::new(b"abcd".to_vec());
        ringbuffer.populate(&mut input)?;

        output = ringbuffer.content();
        assert_eq!(b"5678abcd", output.as_slice());

        Ok(())
    }

    #[test]
    fn push() -> Result<(), Box<dyn Error>> {
        let mut ringbuffer = RingBuffer::<2>::default();

        assert!(ringbuffer.push(1));
        assert!(ringbuffer.push(2));
        assert!(!ringbuffer.push(3));

        let output = ringbuffer.pop();
        assert!(output.is_some());
        assert_eq!(1, output.unwrap());

        let output = ringbuffer.pop();
        assert!(output.is_some());
        assert_eq!(2, output.unwrap());

        Ok(())
    }
}
