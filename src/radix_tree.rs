use std::cmp::min;
use std::collections::VecDeque;
use std::iter;
use std::path::Iter;

#[derive(Default)]
struct Node<V> {
    label: String,
    value: Option<V>,
    children: Vec<Node<V>>,
}

impl<V> Node<V> {
    pub fn get(&self, key: &str) -> Option<&V> {
        let mut remaining = key;
        let mut current = self;

        while !remaining.is_empty() {
            let mut candidate: Option<&Node<V>> = None;
            for node in &current.children {
                if !remaining.starts_with(&node.label) {
                    continue;
                }

                candidate = match candidate {
                    Some(other) if node.label.len() > other.label.len() => Some(node),
                    Some(other) => Some(other),
                    None => Some(node),
                };
            }

            current = candidate?;

            remaining = &remaining[current.label.len()..];
        }

        current.value.as_ref()
    }

    pub fn put(&mut self, key: String, value: V) -> Option<V> {
        let mut remaining = key.as_str();
        let mut current = self;

        while !remaining.is_empty() {
            let mut candidate: Option<(usize, &str)> = None;
            for (i, node) in current.children.iter_mut().enumerate() {
                let prefix = shared_prefix(remaining, &node.label);

                candidate = match candidate {
                    Some((_, other_prefix)) if prefix.len() > other_prefix.len() => {
                        Some((i, prefix))
                    }
                    Some(other) => Some(other),
                    None => Some((i, prefix)),
                };
            }

            let (i, prefix) = match candidate {
                Some(candidate) => candidate,
                None => {
                    current.add_children(iter::once(Node::new(
                        remaining.to_string(),
                        Some(value),
                        Vec::new(),
                    )));
                    return None;
                }
            };

            let candidate_label_len = &current.children[i].label.len();

            remaining = &remaining[prefix.len()..];

            if prefix.len() < *candidate_label_len {
                let candidate = current.children.remove(i);
                current.add_children(iter::once(Node::new(
                    prefix.to_string(),
                    None,
                    vec![
                        Node::new(
                            candidate.label[prefix.len()..].to_string(),
                            candidate.value,
                            candidate.children,
                        ),
                        Node::new(remaining.to_string(), Some(value), Vec::new()),
                    ],
                )));
                return None;
            }

            let candidate = &mut current.children[i];

            if remaining.is_empty() {
                let old_value = candidate.value.take();
                candidate.value = Some(value);
                return old_value;
            }

            current = candidate;
        }

        None
    }

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

    fn add_children(&mut self, children: impl IntoIterator<Item = Node<V>>) {
        self.children.extend(children);
        self.children.sort_by(|a, b| a.label.cmp(&b.label));
    }
}

impl<'a, V> IntoIterator for &'a Node<V> {
    type Item = (String, &'a V);
    type IntoIter = NodeIter<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        let mut to_visit = VecDeque::new();
        to_visit.push_front((self, false));

        NodeIter {
            to_visit,
            label_parts: Vec::new(),
        }
    }
}

struct NodeIter<'a, V> {
    to_visit: VecDeque<(&'a Node<V>, bool)>,
    label_parts: Vec<&'a str>,
}

impl<'a, V> Iterator for NodeIter<'a, V> {
    type Item = (String, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let next = loop {
            let (_, fully_visited) = self.to_visit.front_mut()?;
            if *fully_visited {
                self.label_parts.pop().unwrap();
                self.to_visit.pop_front().unwrap();
            } else {
                *fully_visited = true;
                break self.to_visit.front()?.0;
            }
        };

        for child in next.children.iter().rev() {
            self.to_visit.push_front((child, false));
        }

        self.label_parts.push(next.label.as_str());

        match &next.value {
            Some(value) => Some((self.label_parts.join(""), value)),
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
    use crate::radix_tree::Node;

    #[test]
    fn empty_tree_returns_none() {
        let node = Node::<String>::default();
        assert_eq!(None, node.get("unknown"));
    }

    #[test]
    fn single_insert() {
        let mut node = Node::default();
        node.put("value".to_string(), "value".to_string());
        assert_eq!(Some("value"), node.get("value").map(String::as_str));
    }

    #[test]
    fn multiple_inserts() {
        let mut node = Node::default();
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
            node.put(value.clone(), value.clone());
            assert_eq!(Some(value), node.get(value));
        }

        for value in &values {
            assert_eq!(Some(value), node.get(value));
        }

        let mut iter = node.into_iter();
        for value in &values {
            assert_eq!(Some((value.clone(), value)), iter.next());
        }
    }
}
