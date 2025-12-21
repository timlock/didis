use crate::parser::resp::Value;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub struct ChannelStore {
    channels: HashMap<String, HashSet<u64>>,
    clients: HashMap<u64, HashSet<String>>,
}

impl ChannelStore {
    pub fn subscribe<'a>(&mut self, client_id: u64, channel: String) -> usize {
        let client_subscriptions = self.clients.entry(client_id).or_default();
        client_subscriptions.insert(channel.clone());

        let subscribed_clients = self.channels.entry(channel).or_default();
        subscribed_clients.insert(client_id);

        client_subscriptions.len()
    }

    pub fn unsubscribe<'a>(&mut self, client_id: u64, channel: &str) -> usize {
        let sub_count = match self.clients.get_mut(&client_id) {
            Some(subscriptions) => {
                subscriptions.remove(channel);
                subscriptions.len()
            }
            None => return 0,
        };

        if let Some(subscribed_clients) = self.channels.get_mut(channel) {
            subscribed_clients.remove(&client_id);
        }

        sub_count
    }

    pub fn remove_client<'a>(&mut self, client_id: u64) {
        match self.clients.remove(&client_id) {
            Some(subscribed_channels) => {
                for channel in subscribed_channels {
                    let subscribed_clients = self
                        .channels
                        .get_mut(&channel)
                        .expect("A client should only be subscribed to existing channels");
                    subscribed_clients.remove(&client_id);
                }
            }
            None => {
                // client is not subscribed to any channel
            }
        }
    }

    pub fn subscribers(&self, channel: &str) -> impl Iterator<Item = u64> {
        match self.channels.get(channel) {
            Some(subscribers) => subscribers.iter().cloned(),
            None => Default::default(),
        }
    }
}

pub struct Message {
    pub receivers: Vec<u64>,
    pub value: Value,
}

impl Message {
    pub fn subscribe(
        receivers: impl IntoIterator<Item = u64>,
        channel: String,
        channel_count: i64,
    ) -> Message {
        let value = Value::Push(vec![
            Value::BulkString(String::from("subscribe")),
            Value::BulkString(channel),
            Value::Integer(channel_count),
        ]);

        Message {
            receivers: receivers.into_iter().collect(),
            value,
        }
    }

    pub fn unsubscribe(
        receivers: impl IntoIterator<Item = u64>,
        channel: String,
        channel_count: i64,
    ) -> Message {
        let value = Value::Push(vec![
            Value::BulkString(String::from("unsubscribe")),
            Value::BulkString(channel),
            Value::Integer(channel_count),
        ]);

        Message {
            receivers: receivers.into_iter().collect(),
            value,
        }
    }

    pub fn publish(
        receivers: impl IntoIterator<Item = u64>,
        channel: String,
        payload: String,
    ) -> Message {
        let value = Value::Push(vec![
            Value::BulkString(String::from("message")),
            Value::BulkString(channel),
            Value::BulkString(payload),
        ]);

        Message {
            receivers: receivers.into_iter().collect(),
            value,
        }
    }
}
