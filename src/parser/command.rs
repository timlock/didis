use crate::parser::resp::Resp;
use std::{error, fmt, io};

use super::resp;

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
    type Item = Result<Command, super::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let resp = match self.resp_decoder.next() {
            Some(Ok(resp)) => resp,
            Some(Err(err)) => return Some(Err(err)),
            None => return None,
        };

        match parse_command(resp) {
            Ok(command) => Some(Ok(command)),
            Err(err) => Some(Err(super::Error::Parse(Box::new(err)))),
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
}

impl error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnknownCommand(name) => write!(f, "Command {name} is unknown"),
            Error::InvalidNumberOfArguments(amount) => {
                write!(f, "Invalid amount of arguments {amount}")
            }
            Error::InvalidStart => write!(f, "Invalid begin for a command"),
            Error::MissingName => write!(f, "Command lacks name"),
            Error::UnexpectedResp => write!(f, "Unexpected format"),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Command {
    Ping(Option<Vec<u8>>),
    Echo(Vec<u8>),
    Get(Vec<u8>),
    Set { key: Vec<u8>, value: Vec<u8> },
    ConfigGet,
    Client,
}


fn parse_command(resp: resp::Resp) -> Result<Command, Error> {
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
        _ => Err(Error::UnknownCommand(String::from_utf8_lossy(&name).into_owned())),
    }
}

fn parse_ping(mut iter: impl Iterator<Item = resp::Resp>) -> Result<Command, Error> {
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

fn parse_echo(mut iter: impl Iterator<Item = resp::Resp>) -> Result<Command, Error> {
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

fn parse_get(mut iter: impl Iterator<Item = resp::Resp>) -> Result<Command, Error> {
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

fn parse_set(mut iter: impl Iterator<Item = resp::Resp>) -> Result<Command, Error> {
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

    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::Set { key, value })
}

fn parse_config_get(mut iter: impl Iterator<Item = resp::Resp>) -> Result<Command, Error> {
    let (remaining, _) = iter.size_hint();
    if remaining > 0 {
        return Err(Error::InvalidNumberOfArguments(remaining));
    }

    Ok(Command::ConfigGet)
}

fn parse_client(mut iter: impl Iterator<Item = resp::Resp>) -> Result<Command, Error> {
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
        let command = parse_command(resp).map_err(|err|err.to_string())?;
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
}
