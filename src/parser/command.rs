use super::resp;
use crate::parser::resp::Resp;
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

        println!("resp: {}", resp);
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

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Command {
    Ping(Option<Vec<u8>>),
    Echo(Vec<u8>),
    Get(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        overwrite_rule: Option<OverwriteRule>,
        get: bool,
        expire_rule: Option<ExpireRule>,
    },
    ConfigGet(Vec<u8>),
    Client,
}

fn parse_command(resp: Resp) -> Result<Command, Error> {
    let mut segment_iter = match resp {
        Resp::Array(vec) => vec.into_iter(),
        _ => return Err(Error::InvalidStart),
    };

    let name = match segment_iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(name) => name,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::MissingName),
    };

    match name.to_ascii_uppercase().as_slice() {
        b"PING" => parse_ping(segment_iter),
        b"ECHO" => parse_echo(segment_iter),
        b"GET" => parse_get(segment_iter),
        b"SET" => parse_set(segment_iter),
        b"CONFIG" => parse_config_get(segment_iter),
        b"CLIENT" => parse_client(segment_iter),
        _ => Err(Error::UnknownCommand(
            String::from_utf8_lossy(&name).into_owned(),
        )),
    }
}

fn parse_ping(mut iter: impl Iterator<Item = Resp>) -> Result<Command, Error> {
    let text = match iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(text) => text,
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

fn parse_echo(mut iter: impl Iterator<Item = Resp>) -> Result<Command, Error> {
    let text = match iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(text) => text,
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

fn parse_get(mut iter: impl Iterator<Item = Resp>) -> Result<Command, Error> {
    let key = match iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(text) => text,
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
    ExpiresAtSecs(SystemTime),
    ExpiresAtMillis(SystemTime),
    KEEPTTL,
}
impl ExpireRule {
    pub fn calculate_expire_time(&self) -> Option<SystemTime> {
        match self {
            ExpireRule::ExpiresInSecs(s) => SystemTime::now().checked_add(*s),
            ExpireRule::ExpiresInMillis(ms) => SystemTime::now().checked_add(*ms),
            ExpireRule::ExpiresAtSecs(t) => Some(*t),
            ExpireRule::ExpiresAtMillis(t) => Some(*t),
            ExpireRule::KEEPTTL => None,
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
            let dur = Duration::from_secs(millis);
            return Ok(ExpireRule::ExpiresInMillis(dur));
        }
        if value.starts_with(b"EXAT") {
            let trimmed = &value[4..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed).map_err(|_| "Invalid UTF-8 string")?;
            let secs = ascii_number.parse().map_err(|_| "Invalid number")?;
            let timestamp = SystemTime::now().add(Duration::from_secs(secs));
            return Ok(ExpireRule::ExpiresAtSecs(timestamp));
        }
        if value.starts_with(b"PXAT") {
            let trimmed = &value[4..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed).map_err(|_| "Invalid UTF-8 string")?;
            let millis = ascii_number.parse().map_err(|_| "Invalid number")?;
            let timestamp = SystemTime::now().add(Duration::from_millis(millis));
            return Ok(ExpireRule::ExpiresAtMillis(timestamp));
        }
        if value == b"KEEPTTL" {
            return Ok(ExpireRule::KEEPTTL);
        }

        Err("")
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

fn parse_set(mut iter: impl Iterator<Item = Resp>) -> Result<Command, Error> {
    let key = match iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let value = match iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let mut overwrite_rule = None;
    let mut get = false;
    let mut expire_rule = None;
    for next in iter {
        match next {
            Resp::BulkString(text) => {
                let uppercase = text.to_ascii_uppercase();
                if let Ok(r) = OverwriteRule::try_from(uppercase.as_ref()) {
                    overwrite_rule = Some(r);
                } else if uppercase == b"GET" {
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

fn parse_config_get(mut iter: impl Iterator<Item = Resp>) -> Result<Command, Error> {
    // Sub command like GET or SET
    let _ = match iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(text) => text,
            _ => return Err(Error::UnexpectedResp),
        },
        None => return Err(Error::InvalidNumberOfArguments(0)),
    };

    let key = match iter.next() {
        Some(resp) => match resp {
            Resp::BulkString(text) => text,
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

fn parse_client(mut iter: impl Iterator<Item = Resp>) -> Result<Command, Error> {
    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::Client)
}
mod tests {
    use super::*;

    #[test]
    fn parse_ping() -> Result<(), String> {
        let name = b"PING".to_vec();
        let resp = Resp::Array(vec![Resp::BulkString(name)]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Ping(None), command);
        Ok(())
    }
    #[test]
    fn parse_echo() -> Result<(), String> {
        let name = b"ECHO".to_vec();
        let arg = b"test".to_vec();
        let resp = Resp::Array(vec![Resp::BulkString(name), Resp::BulkString(arg.clone())]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Echo(arg), command);
        Ok(())
    }

    #[test]
    fn parse_set() -> Result<(), String> {
        let resp = Resp::Array(vec![
            Resp::BulkString(b"SET".to_vec()),
            Resp::BulkString(b"key".to_vec()),
            Resp::BulkString(b"value".to_vec()),
            Resp::BulkString(b"XX".to_vec()),
            Resp::BulkString(b"GET".to_vec()),
            Resp::BulkString(b"EX 10".to_vec()),
        ]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(
            Command::Set {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
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
        let resp = Resp::Array(vec![
            Resp::BulkString(b"SET".to_vec()),
            Resp::BulkString(b"key".to_vec()),
            Resp::BulkString(b"value".to_vec()),
        ]);
        let command = parse_command(resp).map_err(|err| err.to_string())?;
        assert_eq!(
            Command::Set {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
                overwrite_rule: None,
                get: false,
                expire_rule: None,
            },
            command
        );
        Ok(())
    }
}
