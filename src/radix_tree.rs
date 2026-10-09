use std::cmp::min;
use std::mem::take;

#[derive(Debug)]
pub struct RadixTree<V> {
    root: Node<V>,
    items: usize,
}

impl<V> RadixTree<V> {
    pub fn get(&self, key: &str) -> Option<&V> {
        self.root.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        self.root.get_mut(key)
    }
    pub fn insert(&mut self, key: String, value: V) -> Option<V> {
        match self.root.put(key, value) {
            Some(old_value) => Some(old_value),
            None => {
                self.items += 1;
                None
            }
        }
    }

    pub fn len(&self) -> usize {
        self.items
    }

    pub fn is_empty(&self) -> bool {
        self.items == 0
    }
}

impl<V> Default for RadixTree<V> {
    fn default() -> Self {
        RadixTree {
            root: Node::default(),
            items: 0,
        }
    }
}

#[derive(Debug)]
struct Node<V> {
    label: String,
    value: Option<V>,
    children: Vec<Node<V>>,
}

impl<V> Default for Node<V> {
    fn default() -> Self {
        Node {
            label: String::new(),
            value: None,
            children: Vec::new(),
        }
    }
}

impl<V> Node<V> {
    fn new(
        label: String,
        value: Option<V>,
        children: impl IntoIterator<Item = Node<V>>,
    ) -> Node<V> {
        let mut node = Node {
            label,
            value,
            children: children.into_iter().collect(),
        };
        node.children.sort_by(|a, b| a.label.cmp(&b.label));

        node
    }

    fn get(&self, key: &str) -> Option<&V> {
        let mut remaining = key;
        let mut current = self;

        while !remaining.is_empty() {
            current = current
                .children
                .iter()
                .filter(|n| remaining.starts_with(&n.label))
                .max_by_key(|n| n.label.len())?;

            remaining = &remaining[current.label.len()..];
        }

        current.value.as_ref()
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        let mut remaining = key;
        let mut current = self;

        while !remaining.is_empty() {
            current = current
                .children
                .iter_mut()
                .filter(|n| remaining.starts_with(&n.label))
                .max_by_key(|n| n.label.len())?;

            remaining = &remaining[current.label.len()..];
        }

        current.value.as_mut()
    }

    fn put(&mut self, key: String, value: V) -> Option<V> {
        let mut remaining = key;
        let mut current = self;

        while !remaining.is_empty() {
            let candidate = current
                .children
                .iter_mut()
                .map(|n| shared_prefix(&remaining, &n.label))
                .enumerate()
                .filter(|(_, p)| !p.is_empty())
                .max_by_key(|(_, p)| p.len());

            let (i, prefix_len) = match candidate {
                Some((i, prefix)) => (i, prefix.len()),
                None => {
                    current.add_child(Node::new(remaining, Some(value), []));
                    return None;
                }
            };

            drop(remaining.drain(..prefix_len));

            if prefix_len < current.children[i].label.len() {
                let mut candidate = current.children.remove(i);
                let new_candidate_label = candidate.label.split_off(prefix_len);
                current.add_child(Node::new(
                    candidate.label,
                    None,
                    [
                        Node::new(new_candidate_label, candidate.value, candidate.children),
                        Node::new(remaining, Some(value), []),
                    ],
                ));
                return None;
            }

            let next = &mut current.children[i];

            if remaining.is_empty() {
                let old_value = next.value.take();
                next.value = Some(value);
                return old_value;
            }

            current = next;
        }

        None
    }

    fn add_child(&mut self, child: Node<V>) {
        self.children.push(child);
        self.children.sort_by(|a, b| a.label.cmp(&b.label));
    }
}

impl<V> IntoIterator for RadixTree<V> {
    type Item = (String, V);
    type IntoIter = RadixTreeIter<V>;

    fn into_iter(self) -> Self::IntoIter {
        let mut to_visit = Vec::with_capacity(self.len());
        to_visit.push((self.root, false));

        RadixTreeIter {
            to_visit,
            label: String::new(),
        }
    }
}

pub struct RadixTreeIter<V> {
    // TODO replace Vec with custom stack based on this https://doc.rust-lang.org/nomicon/vec/vec-layout.html
    to_visit: Vec<(Node<V>, bool)>,
    label: String,
}

impl<V> Iterator for RadixTreeIter<V> {
    type Item = (String, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut next;

        loop {
            next = loop {
                let (node, fully_visited) = self.to_visit.pop()?;
                if fully_visited {
                    self.label.truncate(self.label.len() - node.label.len());
                } else {
                    break node;
                }
            };

            let children = take(&mut next.children);
            self.label += next.label.as_str();
            let value = next.value.take();

            self.to_visit.push((next, true));

            for child in children.into_iter().rev() {
                self.to_visit.push((child, false));
            }

            if let Some(value) = value {
                return Some((self.label.clone(), value));
            }
        }
    }
}

fn shared_prefix<'a>(a: &'a str, b: &str) -> &'a str {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();

    let min = min(a_bytes.len(), b_bytes.len());

    for i in 0..min {
        if a_bytes[i] != b_bytes[i] {
            return &a[..i];
        }
    }

    &a[..min]
}

#[cfg(test)]
mod test {
    use crate::radix_tree::RadixTree;

    #[test]
    fn empty_tree_returns_none() {
        let tree = RadixTree::<String>::default();
        assert_eq!(None, tree.get("unknown"));
        assert!(tree.is_empty());
    }

    #[test]
    fn single_insert() {
        let mut tree = RadixTree::default();
        tree.insert("value".to_owned(), "value".to_owned());
        assert_eq!(Some(&"value".to_owned()), tree.get("value"));
        assert_eq!(1, tree.len());
    }

    #[test]
    fn insert_for_existing_key() {
        let mut tree = RadixTree::default();
        assert_eq!(None, tree.insert("value".to_owned(), "value".to_owned()));
        assert_eq!(1, tree.len());

        assert_eq!(
            Some("value".to_owned()),
            tree.insert("value".to_owned(), "updated".to_owned())
        );
        assert_eq!(1, tree.len());
    }

    #[test]
    fn insert_in_order() {
        let mut tree = RadixTree::default();
        let values = [
            "romane".to_owned(),
            "romanus".to_owned(),
            "romulus".to_owned(),
            "rubens".to_owned(),
            "ruber".to_owned(),
            "rubicon".to_owned(),
            "rubicundus".to_owned(),
        ];

        for value in &values {
            tree.insert(value.clone(), value.clone());
            assert_eq!(Some(value), tree.get(value));
        }

        assert_eq!(values.len(), tree.len());

        for value in &values {
            assert_eq!(Some(value), tree.get(value));
        }

        let mut iter = tree.into_iter();
        for value in values {
            assert_eq!(Some((value.clone(), value)), iter.next());
        }

        assert_eq!(None, iter.next());
    }
    #[test]
    fn insert_reverse_order() {
        let mut tree = RadixTree::default();
        let mut values = [
            "romane".to_owned(),
            "romanus".to_owned(),
            "romulus".to_owned(),
            "rubens".to_owned(),
            "ruber".to_owned(),
            "rubicon".to_owned(),
            "rubicundus".to_owned(),
        ];

        values.reverse();

        for value in &values {
            tree.insert(value.clone(), value.clone());
            assert_eq!(Some(value), tree.get(value));
        }

        assert_eq!(values.len(), tree.len());

        for value in &values {
            assert_eq!(Some(value), tree.get(value));
        }

        let mut iter = tree.into_iter();
        for value in values.into_iter().rev() {
            assert_eq!(Some((value.clone(), value)), iter.next());
        }

        assert_eq!(None, iter.next());
    }

    #[test]
    fn insert_keys_without_shared_prefix() {
        let mut tree = RadixTree::default();
        let values = [
            "apple".to_owned(),
            "banana".to_owned(),
            "citrus".to_owned(),
            "dragon fruit".to_owned(),
            "eggplant".to_owned(),
        ];

        for value in &values {
            tree.insert(value.clone(), value.clone());
            assert_eq!(Some(value), tree.get(value));
        }

        assert_eq!(values.len(), tree.len());

        for value in &values {
            assert_eq!(Some(value), tree.get(value));
        }

        let mut iter = tree.into_iter();
        for value in values {
            assert_eq!(Some((value.clone(), value)), iter.next());
        }

        assert_eq!(None, iter.next());
    }
}
