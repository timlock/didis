use std::io;
use std::io::{IoSlice, IoSliceMut, Read, Write};
use std::iter::Cycle;
use std::ops::Range;

#[derive(Debug)]
pub struct RingBuffer<const N: usize> {
    buf: [u8; N],
    read_pos: RingBufferIndex,
    write_pos: RingBufferIndex,
}

impl<const N: usize> RingBuffer<N> {
    pub fn push(&mut self, value: u8) {
        if self.full() {
            self.read_pos.next();
        }
        self.buf[self.write_pos.peek() % N] = value;
        self.write_pos.next();
    }

    pub fn pop(&mut self) -> Option<u8> {
        if self.empty() {
            return None;
        }

        let content = self.buf[self.read_pos.peek() % N];
        self.read_pos.next();

        Some(content)
    }

    pub fn empty(&self) -> bool {
        self.write_pos.peek() == self.read_pos.peek()
    }

    pub fn full(&self) -> bool {
        self.size() == N
    }

    pub fn size(&self) -> usize {
        self.write_pos.peek() - self.read_pos.peek()
    }
    pub fn populate(&mut self, reader: &mut impl Read) -> io::Result<usize> {
        let (first, second) = self.writeable_ranges_ref();
        let mut slices = [IoSliceMut::new(first), IoSliceMut::new(second)];

        let n = reader.read_vectored(&mut slices)?;
        self.write_pos.add(n - 1);

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

        if (self.write_pos.peek() % N) < (self.read_pos.peek() % N) {
            return (
                &mut self.buf[(self.write_pos.peek() % N)..(self.read_pos.peek() % N)],
                &mut [],
            );
        }

        let (first, second) = self.buf.split_at_mut(self.write_pos.peek());
        (first, &mut second[..self.read_pos.peek()])
    }

    fn readable_ranges_ref(&mut self) -> (&mut [u8], &mut [u8]) {
        if self.empty() {
            return (&mut [], &mut []);
        }

        if (self.read_pos.peek() % N) < (self.write_pos.peek() % N) {
            return (
                &mut self.buf[(self.read_pos.peek() % N)..(self.write_pos.peek() % N)],
                &mut [],
            );
        }

        let is_full = self.full();

        let (first, second) = self.buf.split_at_mut(self.read_pos.peek());
        if is_full {
            return (second, first);
        }
        (first, &mut second[..(self.write_pos.peek() % N)])
    }

    fn writeable_ranges(&self) -> (Option<Range<usize>>, Option<Range<usize>>) {
        if self.full() {
            return (None, None);
        }

        if self.write_pos.peek() < self.read_pos.peek() {
            return (Some(self.write_pos.peek()..self.read_pos.peek()), None);
        }

        (
            Some(self.write_pos.peek()..N),
            Some(0..self.read_pos.peek()),
        )
    }
}

impl<const N: usize> Default for RingBuffer<N> {
    fn default() -> Self {
        Self {
            buf: [0; N],
            read_pos: RingBufferIndex::default(),
            write_pos: RingBufferIndex::default(),
        }
    }
}

impl<const N: usize> Write for RingBuffer<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.writeable_ranges() {
            (None, None) => Ok(0),
            (Some(range), None) | (None, Some(range)) => {
                let n = self.buf[range].as_mut().write(buf)?;
                self.write_pos.add(n);
                Ok(n)
            }
            (Some(first), Some(second)) => {
                let mut n = self.buf[first].as_mut().write(buf)?;
                n += self.buf[second].as_mut().write(&buf[n..])?;

                self.write_pos.add(n);

                Ok(n)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<const N: usize> Read for RingBuffer<N> {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let (first, second) = self.readable_ranges_ref();
        let slices = [IoSlice::new(first), IoSlice::new(second)];

        let n = buf.write_vectored(&slices)?;
        self.read_pos.add(n);

        Ok(n)
    }
}

#[derive(Debug)]
struct RingBufferIndex {
    range: Cycle<Range<usize>>,
    index: usize,
}

impl RingBufferIndex {
    fn peek(&self) -> usize {
        self.index
    }

    fn next(&mut self) -> usize {
        self.index = self
            .range
            .next()
            .expect("There should always be a next item because Cycle never returns None");

        self.index
    }

    fn add(&mut self, n: usize) -> usize {
        self.index = self
            .range
            .nth(n)
            .expect("There should always be a next item because Cycle never returns None");

        self.index
    }
}

impl Default for RingBufferIndex {
    fn default() -> Self {
        let mut range_iter = (0..usize::MAX).cycle();
        let _ = range_iter.next(); // we want to start at 0 and the first call to next should return 1
        Self {
            range: range_iter,
            index: 0,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::io::Cursor;

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
    fn populate_full_buffer() -> Result<(), Box<dyn Error>> {
        let mut input = Cursor::new(b"12345678".to_vec());
        let mut ringbuffer = RingBuffer::<4>::default();

        let n = ringbuffer.populate(&mut input)?;

        assert_eq!(4, n);

        let output = ringbuffer.content();
        assert_eq!(b"1234", output.as_slice());
        Ok(())
    }

    #[test]
    fn push() -> Result<(), Box<dyn Error>> {
        let mut ringbuffer = RingBuffer::<2>::default();

        ringbuffer.push(1);
        ringbuffer.push(2);
        ringbuffer.push(3);

        let output = ringbuffer.pop();
        assert!(output.is_some());
        assert_eq!(2, output.unwrap());

        let output = ringbuffer.pop();
        assert!(output.is_some());
        assert_eq!(3, output.unwrap());

        Ok(())
    }

    #[test]
    fn index_overflow() -> Result<(), Box<dyn Error>> {
        let mut write_range = (0..usize::MAX).cycle();
        write_range.nth(usize::MAX - 1);
        let write_index = RingBufferIndex {
            range: write_range,
            index: usize::MAX,
        };

        let mut ringbuffer = RingBuffer::<2>::default();
        ringbuffer.write_pos = write_index;
        ringbuffer.buf = [1,0];

        ringbuffer.push(2);
        ringbuffer.push(3);

        let output = ringbuffer.pop();
        assert!(output.is_some());
        assert_eq!(2, output.unwrap());

        let output = ringbuffer.pop();
        assert!(output.is_some());
        assert_eq!(3, output.unwrap());

        Ok(())
    }
}
