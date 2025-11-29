use crate::dictionary::Dictionary;
use crate::parser::command::Command;
use crate::parser::resp::{Reference, ValOrRef, Value};
use crate::pubsub::ChannelStore;

#[derive(Default)]
pub struct Controller {
    dictionary: Dictionary,
    channel_store: ChannelStore,
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
            Command::Subscribe(channels) => {
                for channel in channels {
                    let subscribed_channels = self
                        .channel_store
                        .subscribe(client_id, channel.into_owned());
                    //TODO publish message
                }
                return None;
            }
            Command::Unsubscribe(channels) => {
                for channel in channels {
                    let subscribed_channels =
                        self.channel_store.unsubscribe(client_id, channel.as_ref());
                    //TODO publish message
                }
                return None;
            }
            Command::Publish { channel, message } => {
                let subscribers = self.channel_store.subscribers(channel.as_ref());
                //TODO publish message
                Value::Integer(subscribers.size_hint().0 as i64).into()
            }
        };
        Some(result)
    }
}
