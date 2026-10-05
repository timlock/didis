use std::cmp::min;
use std::collections::VecDeque;
use std::iter;

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

    pub fn first_key_value(&mut self) -> Option<(String, &V)> {
        self.root.first_key_value()
    }

    pub fn last_key_value(&mut self) -> Option<(String, &V)> {
        self.root.last_key_value()
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

    fn first_key_value(&self) -> Option<(String, &V)> {
        if self.children.is_empty() {
            return None;
        }
        let mut current = self;
        let mut key = String::new();

        loop {
            if let Some(next) = current
                .children
                .iter()
                .filter(|n| n.value.is_some())
                .min_by(|a, b| a.label.cmp(&b.label))
            {
                key += next.label.as_str();
                return Some((key, next.value.as_ref().unwrap()));
            } else if let Some(next) = current.children.iter().min_by(|a, b| a.label.cmp(&b.label))
            {
                current = next;
                key += next.label.as_str();
            } else {
                break;
            }
        }

        let value = current.value.as_ref()?;

        Some((key, value))
    }

    fn last_key_value(&self) -> Option<(String, &V)> {
        if self.children.is_empty() {
            return None;
        }

        let mut current = self;
        let mut key = String::new();

        while let Some(next) = current.children.iter().max_by(|a, b| a.label.cmp(&b.label)) {
            current = next;
            key += next.label.as_str();
        }

        let value = current.value.as_ref()?;

        Some((key, value))
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
                .max_by_key(|(_, p)| p.len());

            let (i, prefix_len) = match candidate {
                Some((i, prefix)) => (i, prefix.len()),
                None => {
                    current.add_children(iter::once(Node::new(remaining, Some(value), Vec::new())));
                    return None;
                }
            };

            drop(remaining.drain(..prefix_len));

            if prefix_len < current.children[i].label.len() {
                let mut candidate = current.children.remove(i);
                let new_candidate_label = candidate.label.split_off(prefix_len);
                current.add_children(iter::once(Node::new(
                    candidate.label,
                    None,
                    vec![
                        Node::new(new_candidate_label, candidate.value, candidate.children),
                        Node::new(remaining, Some(value), Vec::new()),
                    ],
                )));
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

    fn add_children(&mut self, children: impl IntoIterator<Item = Node<V>>) {
        self.children.extend(children);
        self.children.sort_by(|a, b| a.label.cmp(&b.label));
    }
}

impl<'a, V> IntoIterator for &'a RadixTree<V> {
    type Item = (String, &'a V);
    type IntoIter = RadixTreeIter<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        let mut to_visit = VecDeque::new();
        to_visit.push_front((&self.root, false));

        RadixTreeIter {
            to_visit,
            label: String::new(),
        }
    }
}

pub struct RadixTreeIter<'a, V> {
    to_visit: VecDeque<(&'a Node<V>, bool)>,
    label: String,
}

impl<'a, V> Iterator for RadixTreeIter<'a, V> {
    type Item = (String, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let next = loop {
            let (node, fully_visited) = self.to_visit.front_mut()?;
            if *fully_visited {
                self.label.truncate(self.label.len() - node.label.len());
                self.to_visit.pop_front().unwrap();
            } else {
                *fully_visited = true;
                break self.to_visit.front()?.0;
            }
        };

        for child in next.children.iter().rev() {
            self.to_visit.push_front((child, false));
        }

        self.label += next.label.as_str();

        //TODO remove recursiveness
        match &next.value {
            Some(value) => Some((self.label.clone(), value)),
            None => self.next(),
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
        tree.insert("value".to_string(), "value".to_string());
        assert_eq!(Some("value"), tree.get("value").map(String::as_str));
        assert_eq!(1, tree.len());
    }

    #[test]
    fn insert_for_existing_key() {
        let mut tree = RadixTree::default();
        assert_eq!(None, tree.insert("value".to_string(), "value".to_string()));
        assert_eq!(1, tree.len());

        assert_eq!(
            Some("value".to_string()),
            tree.insert("value".to_string(), "updated".to_string())
        );
        assert_eq!(1, tree.len());
    }

    #[test]
    fn multiple_inserts() {
        let mut tree = RadixTree::default();
        let values = [
            "romane".to_string(),
            "romanus".to_string(),
            "romulus".to_string(),
            "rubens".to_string(),
            "ruber".to_string(),
            "rubicon".to_string(),
            "rubicundus".to_string(),
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
        for value in &values {
            assert_eq!(Some((value.clone(), value)), iter.next());
        }
    }

    #[test]
    fn first_key_value() {
        let mut tree = RadixTree::default();
        let values = [
            "romane".to_string(),
            "romanus".to_string(),
            "romulus".to_string(),
            "rubens".to_string(),
            "ruber".to_string(),
            "rubicon".to_string(),
            "rubicundus".to_string(),
        ];

        for value in &values {
            tree.insert(value.clone(), value.clone());
        }

        assert_eq!(
            Some(("romane".to_string(), "romane")),
            tree.first_key_value()
                .map(|(key, value)| (key, value.as_str()))
        );
    }

    #[test]
    fn last_key_value() {
        let mut tree = RadixTree::default();
        let values = [
            "romane".to_string(),
            "romanus".to_string(),
            "romulus".to_string(),
            "rubens".to_string(),
            "ruber".to_string(),
            "rubicon".to_string(),
            "rubicundus".to_string(),
        ];

        for value in &values {
            tree.insert(value.clone(), value.clone());
        }

        assert_eq!(
            Some(("rubicundus".to_string(), "rubicundus")),
            tree.first_key_value()
                .map(|(key, value)| (key, value.as_str()))
        );
    }
}
