use crate::{
    dictionary::Dictionary,
};
use crate::parser::command::Command;
use crate::parser::resp::Resp;

pub struct Controller {
    dictionary: Dictionary<String>,
}

impl Controller {
    pub fn new(dictionary: Dictionary<String>) -> Self {
        Self {
            dictionary,
        }
    }

    pub fn handle_command(&mut self, command: Command) -> Resp {
        match command {
            Command::Ping => Resp::SimpleString("PONG".to_string()),
            Command::Echo(s) => Resp::BulkString(s),
            Command::Get(key) => match self.dictionary.get(&key) {
                Some(value) => Resp::BulkString(value.to_string()),
                None => Resp::Null,
            },
            Command::Set { key, value } => {
                self.dictionary.set(key, value, None, false, None);
                Resp::ok()
            }
            Command::ConfigGet => Resp::Integer(0),
            Command::Client => Resp::ok(),
        }
    }
}
