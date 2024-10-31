use crate::dictionary::Dictionary;
use crate::parser::command::Command;
use crate::parser::resp::Resp;

pub struct Controller {
    dictionary: Dictionary<Vec<u8>>,
}

impl Controller {
    pub fn new(dictionary: Dictionary<Vec<u8>>) -> Self {
        Self {
            dictionary,
        }
    }

    pub fn handle_command(&mut self, command: Command) -> Resp {
        match command {
            Command::Ping(None) => Resp::SimpleString(b"PONG".to_vec()),
            Command::Ping(Some(text)) => Resp::BulkString(text),
            Command::Echo(s) => Resp::BulkString(s),
            Command::Get(key) => match self.dictionary.get(&key) {
                Some(value) => Resp::BulkString(value.clone()),
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
