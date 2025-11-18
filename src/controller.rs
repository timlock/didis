use crate::dictionary::Dictionary;
use crate::parser::command::Command;
use crate::parser::resp::Value;

#[derive(Default)]
pub struct Controller {
    dictionary: Dictionary,
}

impl Controller {
    pub fn new(dictionary: Dictionary) -> Self {
        Self { dictionary }
    }

    pub fn handle_command(&mut self, command: Command) -> Value {
        match command {
            Command::Ping(None) => Value::SimpleString("PONG".to_string()),
            Command::Ping(Some(text)) => Value::BulkString(text),
            Command::Echo(s) => Value::BulkString(s),
            Command::Get(key) => match self.dictionary.get(&key) {
                Some(value) => Value::BulkString(value.to_string()),
                None => Value::Null,
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
                    Ok(Some(old_value)) => Value::BulkString(old_value),
                    Ok(None) if !get => Value::ok(),
                    Ok(None) => Value::Null,
                    Err(_) => Value::Null,
                }
            }
            Command::ConfigGet(key) => {
                if key == "appendonly" {
                    return Value::Array(vec![
                        Value::BulkString("appendonly".to_string()),
                        Value::BulkString("no".to_string()),
                    ]);
                }
                Value::Array(vec![
                    Value::BulkString("save".to_string()),
                    Value::BulkString("".to_string()),
                ])
            }
            Command::Client => Value::ok(),
            Command::Exists(keys) => {
                let mut count = 0;
                for key in keys {
                    if self.dictionary.get(&key).is_some() {
                        count += 1;
                    }
                }

                Value::Integer(count)
            }
        }
    }
}
