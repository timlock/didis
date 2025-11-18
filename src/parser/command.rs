use super::resp;
use crate::parser::resp::{Value, parse_resp};
use std::fmt::{Display, Formatter, write};
use std::ops::Add;
use std::time::{Duration, SystemTime};
use std::{error, fmt, io};

pub struct RingDecoder<T> {
    resp_decoder: resp::RingDecoder<T>,
}

impl<T> RingDecoder<T>
where
    T: io::Read,
{
    pub fn new(resp_decoder: resp::RingDecoder<T>) -> Self {
        Self { resp_decoder }
    }
}

impl<T> Iterator for RingDecoder<T>
where
    T: io::Read,
{
    type Item = Result<Command, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let resp = match self.resp_decoder.next() {
            Some(Ok(resp)) => resp,
            Some(Err(resp::Error::Io(err))) => return Some(Err(Error::Io(err))),
            Some(Err(err)) => return Some(Err(Error::Parse(err))),
            None => return None,
        };

        match parse_command(resp) {
            Ok(command) => Some(Ok(command)),
            Err(err) => Some(Err(err)),
        }
    }
}

pub struct Decoder<T> {
    resp_decoder: resp::Decoder<T>,
}

impl<T> Decoder<T>
where
    T: io::Read,
{
    pub fn new(resp_decoder: resp::Decoder<T>) -> Self {
        Self { resp_decoder }
    }
}

impl<T> Iterator for Decoder<T>
where
    T: io::Read,
{
    type Item = Result<Command, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let resp = match self.resp_decoder.next() {
            Some(Ok(resp)) => resp,
            Some(Err(err)) => return Some(Err(err.into())),
            None => return None,
        };

        match parse_command(resp) {
            Ok(command) => Some(Ok(command)),
            Err(err) => Some(Err(err)),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    UnknownCommand(String),
    InvalidNumberOfArguments(usize),
    InvalidStart,
    MissingName,
    UnexpectedResp,
    Parse(resp::Error),
    Io(io::Error),
}

impl error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownCommand(name) => write!(f, "Command {name} is unknown"),
            Error::InvalidNumberOfArguments(amount) => {
                write!(f, "Invalid amount of arguments {amount}")
            }
            Error::InvalidStart => write!(f, "Invalid begin for a command"),
            Error::MissingName => write!(f, "Command lacks name"),
            Error::UnexpectedResp => write!(f, "Unexpected format"),
            Error::Parse(err) => write!(f, "Parse error: {}", err),
            Error::Io(err) => write!(f, "IO error: {}", err),
        }
    }
}
impl From<resp::Error> for Error {
    fn from(value: resp::Error) -> Self {
        match value {
            resp::Error::Io(err) => Error::Io(err),
            _ => Error::Parse(value),
        }
    }
}
#[derive(Debug, Default)]
pub struct Parser {
    resp_parser: resp::Parser,
}

impl Parser {
    pub fn parse_all(&mut self, buf: &[u8]) -> (Vec<Result<Command, Error>>, usize) {
        let mut read = 0;
        let mut commands = Vec::new();
        while read < buf.len() {
            match self.resp_parser.parse(&buf[read..]) {
                Ok((Some(resp), n)) => {
                    read += n;
                    let command = parse_command(resp);
                    let is_err = command.is_err();
                    commands.push(command);

                    if is_err {
                        break;
                    }
                }
                Ok((None, n)) => read += n,
                Err(err) => {
                    commands.push(Err(err.into()));
                    break;
                }
            }
        }

        (commands, read)
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Command {
    Ping(Option<String>),
    Echo(String),
    Get(String),
    Set {
        key: String,
        value: String,
        overwrite_rule: Option<OverwriteRule>,
        get: bool,
        expire_rule: Option<ExpireRule>,
    },
    ConfigGet(String),
    Client,
    Exists(Vec<String>),
}

impl Command {
    pub fn to_resp(self) -> Value {
        match self {
            Command::Ping(message) => {
                let mut command_parts = vec![Value::BulkString(String::from("PING"))];
                if let Some(message) = message {
                    command_parts.push(Value::BulkString(message));
                }
                Value::Array(command_parts)
            }
            Command::Echo(message) => Value::Array(vec![
                Value::BulkString(String::from("ECHO")),
                Value::BulkString(message),
            ]),
            Command::Get(key) => Value::Array(vec![
                Value::BulkString(String::from("GET")),
                Value::BulkString(key),
            ]),
            Command::Set {
                key,
                value,
                overwrite_rule,
                get,
                expire_rule,
            } => {
                let mut command_parts = vec![
                    Value::BulkString(String::from("SET")),
                    Value::BulkString(key),
                    Value::BulkString(value),
                ];
                if let Some(overwrite_rule) = overwrite_rule {
                    command_parts.push(Value::from(overwrite_rule));
                }
                if get {
                    command_parts.push(Value::BulkString(String::from("GET")))
                }
                if let Some(expire_rule) = expire_rule {
                    command_parts.push(Value::from(expire_rule));
                }

                Value::Array(command_parts)
            }
            Command::ConfigGet(c) => Value::Array(vec![
                Value::BulkString(String::from("CONFIG")),
                Value::BulkString(String::from("GET")),
                Value::BulkString(c),
            ]),
            Command::Client => Value::Array(vec![Value::BulkString(String::from("CLIENT"))]),
            Command::Exists(keys) => {
                let mut command_parts = vec![Value::BulkString(String::from("EXISTS"))];
                for key in keys {
                    command_parts.push(Value::BulkString(key));
                }

                Value::Array(command_parts)
            }
        }
    }
    pub fn to_bytes(self) -> Vec<u8> {
        Value::from(self).to_bytes()
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Command::Ping(value) => match value {
                None => write!(f, "PING"),
                Some(value) => write!(f, "PING {}", value),
            },
            Command::Echo(value) => write!(f, "ECHO {}", value),
            Command::Get(key) => write!(f, "GET {}", key),
            Command::Set {
                key,
                value,
                overwrite_rule,
                get,
                expire_rule,
            } => {
                write!(f, "SET {}", key)?;
                if let Some(overwrite_rule) = overwrite_rule {
                    write!(f, " {:?}", overwrite_rule)?;
                }
                if value.len() < 32 {
                    write!(f, "{}", value)?;
                } else {
                    write!(f, "{}...(shortened)", &value[..32])?;
                }
                if *get {
                    write!(f, " GET")?;
                }
                if let Some(expire_rule) = expire_rule {
                    write!(f, " {:?}", expire_rule)?;
                }
                Ok(())
            }

            Command::ConfigGet(value) => {
                write!(f, "CONFIG GET {}", value)
            }
            Command::Client => {
                write!(f, "CLIENT")
            }
            Command::Exists(keys) => {
                write!(f, "KEYS {:?}", keys)
            }
        }
    }
}

impl From<Command> for Value {
    fn from(value: Command) -> Self {
        value.to_resp()
    }
}

impl From<Command> for Vec<u8> {
    fn from(value: Command) -> Self {
        value.to_bytes()
    }
}

pub fn parse_command(resp: Value) -> Result<Command, Error> {
    let mut segment_iter = match resp {
        Value::Array(vec) => vec.into_iter(),
        _ => return Err(Error::InvalidStart),
    };

    let name = match segment_iter.next() {
        Some(resp) => match resp {
            Value::BulkString(name) => name,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::MissingName),
    };

    match name.to_uppercase().as_str() {
        "PING" => parse_ping(segment_iter),
        "ECHO" => parse_echo(segment_iter),
        "GET" => parse_get(segment_iter),
        "SET" => parse_set(segment_iter),
        "CONFIG" => parse_config_get(segment_iter),
        "CLIENT" => parse_client(segment_iter),
        "EXISTS" => parse_exists(segment_iter),
        _ => Err(Error::UnknownCommand(name)),
    }
}

pub fn parse_command_bytes(value: &[u8]) -> Result<(impl Iterator<Item = Command>, &[u8]), Error> {
    let mut remaining = value;
    let mut result = Vec::new();
    loop {
        match parse_resp(remaining)? {
            (Some(resp), r) => {
                let command = parse_command(resp)?;
                result.push(command);
                remaining = r;
            }
            (None, r) => {
                remaining = r;
                break;
            }
        };
    }
    Ok((result.into_iter(), remaining))
}

fn parse_ping(mut iter: impl Iterator<Item =Value>) -> Result<Command, Error> {
    let text = match iter.next() {
        Some(resp) => match resp {
            Value::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Ok(Command::Ping(None)),
    };

    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::Ping(Some(text)))
}

fn parse_echo(mut iter: impl Iterator<Item =Value>) -> Result<Command, Error> {
    let text = match iter.next() {
        Some(resp) => match resp {
            Value::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::Echo(text))
}

fn parse_get(mut iter: impl Iterator<Item =Value>) -> Result<Command, Error> {
    let key = match iter.next() {
        Some(resp) => match resp {
            Value::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::Get(key))
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum ExpireRule {
    ExpiresInSecs(Duration),
    ExpiresInMillis(Duration),
    ExpiresAtSecs(Duration),
    ExpiresAtMillis(Duration),
    KeepTTL,
}
impl ExpireRule {
    pub fn calculate_expire_time(&self) -> Option<SystemTime> {
        match self {
            ExpireRule::ExpiresInSecs(s) => SystemTime::now().checked_add(*s),
            ExpireRule::ExpiresInMillis(ms) => SystemTime::now().checked_add(*ms),
            ExpireRule::ExpiresAtSecs(s) => Some(SystemTime::UNIX_EPOCH.checked_add(*s)?),
            ExpireRule::ExpiresAtMillis(ms) => Some(SystemTime::UNIX_EPOCH.checked_add(*ms)?),
            ExpireRule::KeepTTL => None,
        }
    }
}
impl TryFrom<&[u8]> for ExpireRule {
    type Error = &'static str;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.starts_with(b"EX") {
            let trimmed = &value[2..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed).map_err(|_| "Invalid UTF-8 string")?;
            let secs = ascii_number.parse().map_err(|_| "Invalid number")?;
            let dur = Duration::from_secs(secs);
            return Ok(ExpireRule::ExpiresInSecs(dur));
        }
        if value.starts_with(b"PX") {
            let trimmed = &value[2..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed).map_err(|_| "Invalid UTF-8 string")?;
            let millis = ascii_number.parse().map_err(|_| "Invalid number")?;
            let dur = Duration::from_millis(millis);
            return Ok(ExpireRule::ExpiresInMillis(dur));
        }
        if value.starts_with(b"EXAT") {
            let trimmed = &value[4..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed).map_err(|_| "Invalid UTF-8 string")?;
            let secs = ascii_number.parse().map_err(|_| "Invalid number")?;
            return Ok(ExpireRule::ExpiresAtSecs(Duration::from_secs(secs)));
        }
        if value.starts_with(b"PXAT") {
            let trimmed = &value[4..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed).map_err(|_| "Invalid UTF-8 string")?;
            let millis = ascii_number.parse().map_err(|_| "Invalid number")?;
            return Ok(ExpireRule::ExpiresAtMillis(Duration::from_millis(millis)));
        }
        if value == b"KEEPTTL" {
            return Ok(ExpireRule::KeepTTL);
        }

        Err("")
    }
}

impl From<ExpireRule> for Value {
    fn from(value: ExpireRule) -> Self {
        match value {
            ExpireRule::ExpiresInSecs(duration) => {
                Value::BulkString(format!("EX {}", duration.as_secs()))
            }
            ExpireRule::ExpiresInMillis(duration) => {
                Value::BulkString(format!("PX {}", duration.as_millis()))
            }
            ExpireRule::ExpiresAtSecs(duration) => {
                Value::BulkString(format!("EXAT {}", duration.as_secs()))
            }
            ExpireRule::ExpiresAtMillis(duration) => {
                Value::BulkString(format!("PXAT {}", duration.as_millis()))
            }
            ExpireRule::KeepTTL => Value::BulkString(String::from("KEEPTTL")),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum OverwriteRule {
    NotExists,
    Exists,
}

impl TryFrom<&[u8]> for OverwriteRule {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        match value {
            b"NX" => Ok(OverwriteRule::NotExists),
            b"XX" => Ok(OverwriteRule::Exists),
            _ => Err(()),
        }
    }
}

impl From<OverwriteRule> for Value {
    fn from(value: OverwriteRule) -> Self {
        match value {
            OverwriteRule::NotExists => Value::BulkString(String::from("NX")),
            OverwriteRule::Exists => Value::BulkString(String::from("XX")),
        }
    }
}

fn parse_set(mut iter: impl Iterator<Item =Value>) -> Result<Command, Error> {
    let key = match iter.next() {
        Some(resp) => match resp {
            Value::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let value = match iter.next() {
        Some(resp) => match resp {
            Value::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let mut overwrite_rule = None;
    let mut get = false;
    let mut expire_rule = None;
    for next in iter {
        match next {
            Value::BulkString(text) => {
                let uppercase = text.to_ascii_uppercase();
                if let Ok(r) = OverwriteRule::try_from(uppercase.as_ref()) {
                    overwrite_rule = Some(r);
                } else if uppercase == "GET" {
                    get = true;
                } else if let Ok(p) = ExpireRule::try_from(uppercase.as_ref()) {
                    expire_rule = Some(p);
                }
            }
            _ => return Err(Error::UnexpectedResp),
        }
    }

    Ok(Command::Set {
        key,
        value,
        overwrite_rule,
        get,
        expire_rule,
    })
}

fn parse_config_get(mut iter: impl Iterator<Item =Value>) -> Result<Command, Error> {
    // Sub command like GET or SET
    let _ = match iter.next() {
        Some(resp) => match resp {
            Value::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let key = match iter.next() {
        Some(resp) => match resp {
            Value::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::ConfigGet(key))
}

fn parse_client(iter: impl Iterator<Item =Value>) -> Result<Command, Error> {
    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::Client)
}
fn parse_exists(iter: impl Iterator<Item =Value>) -> Result<Command, Error> {
    let mut keys = Vec::new();

    for next in iter {
        match next {
            Value::BulkString(text) => {
                keys.push(text);
            }
            _ => return Err(Error::UnexpectedResp),
        }
    }

    Ok(Command::Exists(keys))
}

mod tests {
    use super::*;

    #[test]
    fn parse_ping() -> Result<(), String> {
        let name = "PING".to_string();
        let resp = Value::Array(vec![Value::BulkString(name)]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Ping(None), command);
        Ok(())
    }
    #[test]
    fn parse_echo() -> Result<(), String> {
        let name = "ECHO".to_string();
        let arg = "test".to_string();
        let resp = Value::Array(vec![Value::BulkString(name), Value::BulkString(arg.clone())]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Echo(arg), command);
        Ok(())
    }

    #[test]
    fn parse_set() -> Result<(), String> {
        let resp = Value::Array(vec![
            Value::BulkString("SET".to_string()),
            Value::BulkString("key".to_string()),
            Value::BulkString("value".to_string()),
            Value::BulkString("XX".to_string()),
            Value::BulkString("GET".to_string()),
            Value::BulkString("EX 10".to_string()),
        ]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(
            Command::Set {
                key: "key".to_string(),
                value: "value".to_string(),
                overwrite_rule: Some(OverwriteRule::Exists),
                get: true,
                expire_rule: Some(ExpireRule::ExpiresInSecs(Duration::from_secs(10))),
            },
            command
        );
        Ok(())
    }

    #[test]
    fn parse_set_no_options() -> Result<(), String> {
        let resp = Value::Array(vec![
            Value::BulkString("SET".to_string()),
            Value::BulkString("key".to_string()),
            Value::BulkString("value".to_string()),
        ]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(
            Command::Set {
                key: "key".to_string(),
                value: "value".to_string(),
                overwrite_rule: None,
                get: false,
                expire_rule: None,
            },
            command
        );
        Ok(())
    }
}
