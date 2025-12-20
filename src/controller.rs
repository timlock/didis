use crate::dictionary::Dictionary;
use crate::parser::command::Command;
use crate::parser::resp::{Reference, ValOrRef, Value};
use crate::pubsub::{ChannelStore, Message};
use std::collections::VecDeque;
use std::num::ParseIntError;

#[derive(Default)]
pub struct Controller {
    dictionary: Dictionary,
    channel_store: ChannelStore,
    messages: VecDeque<Message>,
}

impl Controller {
    pub fn handle_command(&mut self, client_id: u64, command: Command) -> Option<ValOrRef> {
        let result: ValOrRef = match command {
            Command::Ping(None) => Reference::SimpleString("PONG").into(),
            Command::Ping(Some(text)) => Value::BulkString(text.into_owned()).into(),
            Command::Echo(s) => Value::BulkString(s.into_owned()).into(),
            Command::Get(key) => match self.dictionary.get(&key) {
                Some(value) => Reference::BulkString(value).into(),
                None => Value::Null.into(),
            },
            Command::Set {
                key,
                value,
                overwrite_rule,
                get,
                expire_rule,
            } => {
                match self.dictionary.set(
                    key.as_ref(),
                    value.into_owned(),
                    overwrite_rule,
                    get,
                    expire_rule,
                ) {
                    Ok(Some(old_value)) => Value::BulkString(old_value).into(),
                    Ok(None) if !get => Value::ok().into(),
                    Ok(None) => Value::Null.into(),
                    Err(_) => Value::Null.into(),
                }
            }
            Command::ConfigGet(key) => {
                // minimal implementation to allow benchmarking with redis-cli
                if key[0] == "appendonly" {
                    Reference::Array(vec![
                        Reference::BulkString("appendonly"),
                        Reference::BulkString("no"),
                    ])
                    .into()
                } else {
                    Reference::Array(vec![
                        Reference::BulkString("save"),
                        Reference::BulkString(""),
                    ])
                    .into()
                }
            }
            Command::Client => Value::ok().into(), // minimal implementation to allow benchmarking with redis-cli
            Command::Exists(keys) => {
                let mut count = 0;
                for key in keys {
                    if self.dictionary.get(&key).is_some() {
                        count += 1;
                    }
                }

                Value::Integer(count).into()
            }
            Command::Delete(keys) => {
                let mut count = 0;
                for key in keys {
                    if self.dictionary.delete(&key).is_some() {
                        count += 1;
                    }
                }

                Value::Integer(count).into()
            }
            Command::Increment(key) => match self.dictionary.increment(key.as_ref()) {
                Ok(value) => ValOrRef::Val(Value::Integer(value)),
                Err(err) => ValOrRef::Val(Value::SimpleError(String::from(
                    "value is not an integer or out of range",
                ))),
            },
            Command::Decrement(key) => match self.dictionary.decrement(key.as_ref()) {
                Ok(value) => ValOrRef::Val(Value::Integer(value)),
                Err(err) => ValOrRef::Val(Value::SimpleError(String::from(
                    "value is not an integer or out of range",
                ))),
            },
            Command::Subscribe(channels) => {
                for channel in channels {
                    let channel = channel.into_owned();
                    let subscribed_channels =
                        self.channel_store.subscribe(client_id, channel.clone());

                    let subscribers: Vec<u64> =
                        self.channel_store.subscribers(channel.as_ref()).collect();

                    let message =
                        Message::subscribe(subscribers, channel, subscribed_channels as i64);
                    self.messages.push_back(message);
                }
                return None;
            }
            Command::Unsubscribe(channels) => {
                for channel in channels {
                    let subscribers: Vec<u64> =
                        self.channel_store.subscribers(channel.as_ref()).collect();

                    let subscribed_channels =
                        self.channel_store.unsubscribe(client_id, channel.as_ref());

                    let message = Message::unsubscribe(
                        subscribers,
                        channel.into_owned(),
                        subscribed_channels as i64,
                    );
                    self.messages.push_back(message);
                }
                return None;
            }
            Command::Publish { channel, message } => {
                let subscribers: Vec<u64> =
                    self.channel_store.subscribers(channel.as_ref()).collect();
                let subscribers_len = subscribers.len() as i64;

                let message =
                    Message::publish(subscribers, channel.into_owned(), message.into_owned());
                self.messages.push_back(message);

                Value::Integer(subscribers_len).into()
            }
        };
        Some(result)
    }

    pub fn remove_client(&mut self, client_id: &u64) {
        self.channel_store.remove_client(*client_id);
    }

    pub fn has_messages(&self) -> bool {
        !self.messages.is_empty()
    }
    pub fn messages(&mut self) -> Vec<Message> {
        self.messages.drain(..).collect()
    }
}
