use crate::parser::resp::Resp;
use std::{collections::VecDeque, error, fmt, io};

use super::resp;

pub struct Decoder<T> {
    resp_decoder: resp::Decoder<T>,
}

impl<T> Decoder<T>
where
    T: io::Read + io::Write,
{
    pub fn new(resp_decoder: resp::Decoder<T>) -> Self {
        Self { resp_decoder }
    }

    pub fn get_mut(&mut self) -> &mut T {
        self.resp_decoder.get_mut()
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
    Ping(Option<String>),
    Echo(String),
    Get(String),
    Set { key: String, value: String },
    ConfigGet,
    Client,
}

impl TryFrom<Resp> for Command {
    type Error = Resp;

    fn try_from(value: Resp) -> Result<Self, Self::Error> {
        match value {
            Resp::Array(arr) => Command::try_from(arr),
            _ => Err(Resp::unknown_command(value.to_string().as_str())),
        }
    }
}

impl TryFrom<Vec<Resp>> for Command {
    type Error = Resp;

    fn try_from(value: Vec<Resp>) -> Result<Self, Self::Error> {
        create_command(value)
    }
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

    match name.to_uppercase().as_str() {
        "PING" => parse_ping(segment_iter),
        "ECHO" => parse_echo(segment_iter),
        "GET" => parse_get(segment_iter),
        "SET" => parse_set(segment_iter),
        "CONFIG" => parse_config_get(segment_iter),
        "CLIENT" => parse_client(segment_iter),
        _ => Err(Error::UnknownCommand(name.to_string())),
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

fn create_command(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    let name = command_name(&mut arr)?;
    match name.to_uppercase().as_str() {
        "PING" => Ok(Command::Ping(None)),
        "ECHO" => create_echo(arr),
        "GET" => create_get(arr),
        "SET" => create_set(arr),
        "CONFIG" => Ok(Command::ConfigGet),
        "CLIENT" => Ok(Command::Client),
        _ => Err(Resp::unknown_command(&name)),
    }
}

fn create_set(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    if arr.len() != 2 {
        return Err(Resp::wrong_number_of_arguments());
    }
    let key = match arr.remove(0) {
        Resp::BulkString(s) => s,
        _ => return Err(Resp::invalid_arguments()),
    };
    let value = match arr.remove(0) {
        Resp::BulkString(s) => s,
        _ => return Err(Resp::invalid_arguments()),
    };
    Ok(Command::Set { key, value })
}
fn create_get(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    if arr.len() != 1 {
        return Err(Resp::wrong_number_of_arguments());
    }
    match arr.remove(0) {
        Resp::BulkString(s) => Ok(Command::Get(s)),
        _ => Err(Resp::wrong_number_of_arguments()),
    }
}

fn command_name(arr: &mut Vec<Resp>) -> Result<String, Resp> {
    match arr.remove(0) {
        Resp::BulkString(s) => Ok(s),
        _ => return Err(Resp::wrong_number_of_arguments()),
    }
}

fn create_echo(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    if arr.len() != 1 {
        return Err(Resp::wrong_number_of_arguments());
    }
    match arr.remove(0) {
        Resp::BulkString(s) => Ok(Command::Echo(s)),
        _ => Err(Resp::wrong_number_of_arguments()),
    }
}
mod tests {
    use super::*;

    #[test]
    fn parse_ping() -> Result<(), String> {
        let name = String::from("PING");
        let resp = vec![Resp::BulkString(name)];
        let command = Command::try_from(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Ping(None), command);
        Ok(())
    }
    #[test]
    fn parse_echo() -> Result<(), String> {
        let name = String::from("ECHO");
        let arg = String::from("test");
        let resp = vec![Resp::BulkString(name), Resp::BulkString(arg.clone())];
        let command = Command::try_from(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Echo(arg), command);
        Ok(())
    }
}
