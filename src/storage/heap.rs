use std::cmp::Ordering;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct MinHeap<K, V> {
    inner: VecDeque<(K, V)>,
}

impl<K, V> MinHeap<K, V>
where
    K: PartialOrd,
{
    pub fn insert(&mut self, key: K, value: V) {
        self.inner.push_back((key, value));
        self.heapify_up(self.inner.len() - 1);
    }

    pub fn extract(&mut self) -> Option<(K, V)> {
        let extracted = self.inner.pop_front()?;
        if let Some(last) = self.inner.pop_back() {
            self.inner.push_front(last);
            self.heapify_down(0);
        }

        Some(extracted)
    }

    pub fn peek(&self) -> Option<&(K,V)> {
       self.inner.front()
    }

    fn heapify_up(&mut self, mut index: usize) {
        if index == 0 {
            return;
        }

        while index != 0 && &self.inner[index].0 < &self.inner[parent(index)].0 {
            self.inner.swap(index, parent(index));
            index = parent(index);
        }
    }

    fn heapify_down(&mut self, mut index: usize) {
        loop {
            let left = self.inner.get(left_child(index));
            let right = self.inner.get(right_child(index));

            let smaller_child = match (left, right) {
                (None, None) => return,
                (Some(_), None) => left_child(index),
                (None, Some(_)) => right_child(index),
                (Some(_), Some(_)) => {
                    if &self.inner[left_child(index)].0 < &self.inner[right_child(index)].0 {
                        left_child(index)
                    } else {
                        right_child(index)
                    }
                }
            };

            if &self.inner[index].0 > &self.inner[smaller_child].0 {
                self.inner.swap(index, smaller_child);
                index = smaller_child;
            } else {
                return;
            }
        }
    }
}

impl<K, V> Default for MinHeap<K, V>
where
    K: PartialOrd{
    fn default() -> Self {
        MinHeap {
            inner: VecDeque::new(),
        }

    }
}

impl<K, V, I> From<I> for MinHeap<K, V>
where
    I: IntoIterator<Item = (K, V)>,
    K: PartialOrd,
{
    fn from(iter: I) -> Self {
        let mut min_heap = MinHeap {
            inner: VecDeque::new(),
        };
        min_heap.inner.extend(iter);

        if min_heap.inner.is_empty() {
            return min_heap;
        }

        let subtrees = min_heap.inner.len() / 2;

        for i in (0..subtrees).rev() {
            min_heap.heapify_down(i);
        }

        min_heap
    }
}

fn parent(index: usize) -> usize {
    (index - 1) / 2
}

fn left_child(index: usize) -> usize {
    2 * index + 1
}

fn right_child(index: usize) -> usize {
    2 * index + 2
}

struct Entry<T> {
    key: String,
    table_id: usize,
    value: T,
}

impl<T> PartialEq<Self> for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.table_id == other.table_id
    }
}

impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.key.as_str().partial_cmp(other.key.as_str()) {
            Some(Ordering::Equal) | None => {}
            Some(other) => return Some(other),
        };

        self.table_id.partial_cmp(&other.table_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error;

    #[test]
    fn insert() -> Result<(), Box<dyn error::Error>> {
        let mut heap = MinHeap::default();
        heap.insert(15, 0);
        heap.insert(5, 0);
        heap.insert(3, 0);
        heap.insert(4, 0);
        heap.insert(8, 0);

        assert_eq!(3, heap.extract().unwrap().0);
        assert_eq!(4, heap.extract().unwrap().0);
        assert_eq!(5, heap.extract().unwrap().0);
        assert_eq!(8, heap.extract().unwrap().0);
        assert_eq!(15, heap.extract().unwrap().0);

        Ok(())
    }

    #[test]
    fn from_unsorted_array() -> Result<(), Box<dyn error::Error>> {
        let vec = vec![(15, 0), (5, 0), (3, 0), (4, 0), (8, 0)];
        let mut heap = MinHeap::from(vec);

        assert_eq!(3, heap.extract().unwrap().0);
        assert_eq!(4, heap.extract().unwrap().0);
        assert_eq!(5, heap.extract().unwrap().0);
        assert_eq!(8, heap.extract().unwrap().0);
        assert_eq!(15, heap.extract().unwrap().0);

        Ok(())
    }
}
