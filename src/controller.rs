use crate::dictionary::Dictionary;
use crate::parser::command::Command;
use crate::parser::resp::Resp;

#[derive(Default)]
pub struct Controller {
    dictionary: Dictionary,
}

impl Controller {
    pub fn new(dictionary: Dictionary) -> Self {
        Self { dictionary }
    }

    pub fn handle_command(&mut self, command: Command) -> Resp {
        match command {
            Command::Ping(None) => Resp::SimpleString("PONG".to_string()),
            Command::Ping(Some(text)) => Resp::BulkString(text),
            Command::Echo(s) => Resp::BulkString(s),
            Command::Get(key) => match self.dictionary.get(&key) {
                Some(value) => Resp::BulkString(value.to_string()),
                None => Resp::Null,
            },
            Command::Set {
                key,
                value,
                overwrite_rule,
                get,
                expire_rule,
            } => {
                match self
                    .dictionary
                    .set(key, value, overwrite_rule, get, expire_rule)
                {
                    Ok(Some(old_value)) => Resp::BulkString(old_value),
                    Ok(None) if !get => Resp::ok(),
                    Ok(None) => Resp::Null,
                    Err(_) => Resp::Null,
                }
            }
            Command::ConfigGet(key) => {
                if key == "appendonly" {
                    return Resp::Array(vec![
                        Resp::BulkString("appendonly".to_string()),
                        Resp::BulkString("no".to_string()),
                    ]);
                }
                Resp::Array(vec![
                    Resp::BulkString("save".to_string()),
                    Resp::BulkString("".to_string()),
                ])
            }
            Command::Client => Resp::ok(),
            Command::Exists(keys) => {
                let mut count = 0;
                for key in keys {
                    if self.dictionary.get(&key).is_some() {
                        count += 1;
                    }
                }

                Resp::Integer(count)
            }
        }
    }
}
