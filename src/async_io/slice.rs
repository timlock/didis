use std::collections::Bound;
use std::ops::{Deref, DerefMut, Range, RangeBounds};

#[derive(Debug)]
pub struct Slice {
    buf: Vec<u8>,
    range: Range<usize>,
}

impl Slice {
    pub fn as_ptr(&self) -> *const u8 {
        self.buf[self.range.clone()].as_ptr()
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }
}

impl Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf[self.range.clone()]
    }
}

pub trait AsSlice {
    fn slice(self, range: impl RangeBounds<usize>) -> Slice
    where
        Self: Sized;
}

impl AsSlice for Box<[u8]> {
    fn slice(self, range: impl RangeBounds<usize>) -> Slice
    where
        Self: Sized,
    {
        Slice {
            range: sub_range(range, 0..self.len()),
            buf: self.into_vec(),
        }
    }
}

impl<const N: usize> AsSlice for Box<[u8; N]> {
    fn slice(self, range: impl RangeBounds<usize>) -> Slice
    where
        Self: Sized,
    {
        Slice {
            range: sub_range(range, 0..self.len()),
            buf: (self as Box<[u8]>).into_vec(),
        }
    }
}

impl AsSlice for Vec<u8> {
    fn slice(self, range: impl RangeBounds<usize>) -> Slice
    where
        Self: Sized,
    {
        Slice {
            range: sub_range(range, 0..self.len()),
            buf: self,
        }
    }
}

impl AsSlice for Slice {
    fn slice(self, range: impl RangeBounds<usize>) -> Slice
    where
        Self: Sized,
    {
        Slice {
            range: sub_range(range, self.range),
            buf: self.buf,
        }
    }
}

#[derive(Debug)]
pub struct SliceMut {
    buf: Vec<u8>,
    range: Range<usize>,
}

impl SliceMut {
    pub fn as_ptr(&self) -> *const u8 {
        self.buf[self.range.clone()].as_ptr()
    }
    pub fn as_mut_ptr(&mut self) -> *const u8 {
        self.buf[self.range.clone()].as_mut_ptr()
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn is_empty(&self) -> bool {
        self.range.len() == 0
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }
}

impl Deref for SliceMut {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf[self.range.clone()]
    }
}

impl DerefMut for SliceMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf[self.range.clone()]
    }
}
pub trait AsSliceMut {
    fn slice_mut(self, range: impl RangeBounds<usize>) -> SliceMut
    where
        Self: Sized;
}

impl AsSliceMut for Box<[u8]> {
    fn slice_mut(self, range: impl RangeBounds<usize>) -> SliceMut
    where
        Self: Sized,
    {
        SliceMut {
            range: sub_range(range, 0..self.len()),
            buf: self.into_vec(),
        }
    }
}

impl<const N: usize> AsSliceMut for Box<[u8; N]> {
    fn slice_mut(self, range: impl RangeBounds<usize>) -> SliceMut
    where
        Self: Sized,
    {
        SliceMut {
            range: sub_range(range, 0..self.len()),
            buf: (self as Box<[u8]>).into_vec(),
        }
    }
}

impl AsSliceMut for Vec<u8> {
    fn slice_mut(self, range: impl RangeBounds<usize>) -> SliceMut
    where
        Self: Sized,
    {
        SliceMut {
            range: sub_range(range, 0..self.len()),
            buf: self,
        }
    }
}

impl AsSliceMut for SliceMut {
    fn slice_mut(self, range: impl RangeBounds<usize>) -> SliceMut
    where
        Self: Sized,
    {
        SliceMut {
            range: sub_range(range, self.range),
            buf: self.buf,
        }
    }
}

fn sub_range(sub_range: impl RangeBounds<usize>, range: Range<usize>) -> Range<usize> {
    let start = match sub_range.start_bound() {
        Bound::Included(s) => *s,
        Bound::Excluded(s) => s + 1,
        Bound::Unbounded => 0,
    } + range.start;

    let end = match sub_range.end_bound() {
        Bound::Included(s) => s + 1 + range.start,
        Bound::Excluded(s) => *s + range.start,
        Bound::Unbounded => range.end,
    };

    assert!(start >= range.start);
    assert!(end <= range.end);

    start..end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_slice() {
        let buf = vec![0; 10]
            .into_iter()
            .enumerate()
            .map(|(i, _)| i as u8)
            .collect::<Vec<_>>();

        let slice = buf.slice(2..6);
        assert_eq!([2, 3, 4, 5], slice.as_ref());

        let slice = slice.slice(2..=3);
        assert_eq!([4, 5], slice.as_ref());

        let slice = slice.slice(..);
        assert_eq!([4, 5], slice.as_ref());
    }

    #[test]
    fn sub_slice_mut() {
        let buf = vec![0; 10]
            .into_iter()
            .enumerate()
            .map(|(i, _)| i as u8)
            .collect::<Vec<_>>();

        let slice = buf.slice_mut(2..6);
        assert_eq!([2, 3, 4, 5], slice.as_ref());

        let slice = slice.slice_mut(2..=3);
        assert_eq!([4, 5], slice.as_ref());

        let slice = slice.slice_mut(..);
        assert_eq!([4, 5], slice.as_ref());
    }

    #[test]
    fn slices_mut() {
        let buf = vec![0; 10]
            .into_iter()
            .enumerate()
            .map(|(i, _)| i as u8)
            .collect::<Vec<_>>();

        assert_eq!(&buf[..], buf.clone().slice_mut(..).as_ref());
        assert_eq!(&buf[0..0], buf.clone().slice_mut(0..0).as_ref());
        assert_eq!(&buf[0..10], buf.clone().slice_mut(0..10).as_ref());
        assert_eq!(&buf[0..=0], buf.clone().slice_mut(0..=0).as_ref());
        assert_eq!(&buf[1..], buf.clone().slice_mut(1..).as_ref());
        assert_eq!(&buf[..3], buf.clone().slice_mut(..3).as_ref());
    }
}
