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
        true
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
        let (size, _) = self.write_pos.overflowing_sub(self.read_pos);
        size % (N * 2)
        // if self.write_pos < self.read_pos {
        //     return self.read_pos - self.write_pos;
        // }
        // self.write_pos - self.read_pos
    }
    pub fn populate(&mut self, reader: &mut impl Read) -> io::Result<usize> {
        let (first, second) = self.writeable_ranges_ref();
        let mut slices = [IoSliceMut::new(first), IoSliceMut::new(second)];

        let n = reader.read_vectored(&mut slices)?;
        self.write_pos += n;
        self.write_pos = self.write_pos % (N * 2);

        Ok(n)
    }

    // TODO make self non mutable
    pub fn peek(&mut self) -> Vec<u8> {
        let old_read_pos = self.read_pos;
        let content = self.content();
        self.read_pos = old_read_pos;
        content
    }

    pub fn add_read_pos(&mut self, amount: usize) {
        self.read_pos = (self.read_pos + amount) % (N * 2);
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

        let read_pos = self.read_pos % N;
        let write_pos = self.write_pos % N;

        if self.empty() {
            let (first, second) = self.buf.split_at_mut(write_pos);
            return (second, first);
        }

        if write_pos < read_pos {
            return (&mut self.buf[write_pos..read_pos], &mut []);
        }

        let (first, second) = self.buf.split_at_mut(write_pos);
        (second, &mut first[..read_pos])
    }

    fn readable_ranges_ref(&mut self) -> (&mut [u8], &mut [u8]) {
        let read_pos = self.read_pos % N;
        let write_pos = self.write_pos % N;

        if self.empty() {
            let (first, second) = self.buf.split_at_mut(read_pos);
            return (second, first);
        }

        if read_pos < write_pos {
            return (&mut self.buf[read_pos..write_pos], &mut []);
        }

        let is_full = self.full();

        let (first, second) = self.buf.split_at_mut(read_pos);
        if is_full {
            return (second, first);
        }
        (second, &mut first[..write_pos])
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

        let n = ringbuffer.populate(&mut input)?;
        assert_eq!(8, n);

        let output = ringbuffer.content();
        assert_eq!(b"12345678", output.as_slice());
        Ok(())
    }

    #[test]
    fn write_pos_after_read_pos() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"123456".to_vec());
        let mut ringbuffer = RingBuffer::<8>::default();

        let mut n = ringbuffer.populate(&mut input)?;
        assert_eq!(6, n);

        let mut output = vec![0; 2];
        n = ringbuffer.read(&mut output)?;
        assert_eq!(2, n);
        assert_eq!(b"12", output.as_slice());

        input = Cursor::new(b"789A".to_vec());
        n = ringbuffer.populate(&mut input)?;
        assert_eq!(4, n);

        let output = ringbuffer.content();
        assert_eq!(b"3456789A", output.as_slice());
        Ok(())
    }
    #[test]
    fn read_pos_after_write_pos() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"1234".to_vec());
        let mut ringbuffer = RingBuffer::<4>::default();

        let mut n = ringbuffer.populate(&mut input)?;
        assert_eq!(4, n);

        let mut output = vec![0; 4];
        n = ringbuffer.read(&mut output)?;
        assert_eq!(4, n);
        assert_eq!(b"1234", output.as_slice());

        input = Cursor::new(b"5678".to_vec());
        n = ringbuffer.populate(&mut input)?;
        assert_eq!(4, n);

        output = vec![0; 3];
        n = ringbuffer.read(&mut output)?;
        assert_eq!(3, n);
        assert_eq!(b"567", output.as_slice());

        let output = ringbuffer.content();
        assert_eq!(1, output.len());
        assert_eq!(b"8", output.as_slice());
        Ok(())
    }

    #[test]
    fn overwrite_read_entries() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"1234".to_vec());
        let mut ringbuffer = RingBuffer::<4>::default();

        let mut n = ringbuffer.populate(&mut input)?;
        assert_eq!(4, n);

        let mut output = vec![0; 4];
        n = ringbuffer.read(&mut output)?;
        assert_eq!(4, n);
        assert_eq!(b"1234", output.as_slice());

        input = Cursor::new(b"789A".to_vec());
        n = ringbuffer.populate(&mut input)?;
        assert_eq!(4, n);

        let output = ringbuffer.content();
        assert_eq!(b"789A", output.as_slice());
        Ok(())
    }
    #[test]
    fn greater_input_than_capacity() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"12345678".to_vec());
        let mut ringbuffer = RingBuffer::<4>::default();

        let n = ringbuffer.populate(&mut input)?;

        assert_eq!(4, n);

        let output = ringbuffer.content();
        assert_eq!(b"1234", output.as_slice());
        Ok(())
    }

    #[test]
    fn populate_full_buffer() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"1234".to_vec());
        let mut ringbuffer = RingBuffer::<4>::default();

        ringbuffer.populate(&mut input)?;

        input = Cursor::new(b"5678".to_vec());
        let n = ringbuffer.populate(&mut input)?;

        assert_eq!(0, n);

        let output = ringbuffer.content();
        assert_eq!(b"1234", output.as_slice());
        Ok(())
    }

    #[test]
    fn read_empty_buffer() -> Result<(), Box<dyn Error>> {
        let mut ringbuffer = RingBuffer::<4>::default();

        let output = ringbuffer.content();
        assert_eq!(0, output.len());
        assert!(ringbuffer.empty());
        assert_eq!(b"", output.as_slice());

        Ok(())
    }
    #[test]
    fn write_cursor_overflow() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"12345678".to_vec());
        let mut ringbuffer = RingBuffer::<8>::default();

        ringbuffer.populate(&mut input)?;

        let mut output = vec![0; 4];
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
