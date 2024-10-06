use core::error;
use std::{fmt, io};

use crate::parser::command::Command;
use crate::parser::resp::Resp;

pub mod command;
pub mod resp;

pub fn parse(data: &[u8]) -> Result<Vec<Command>, Resp> {
    let resps = Resp::parse(data)?;
    let mut commands = Vec::new();
    for resp in resps {
        let command = Command::try_from(resp)?;
        commands.push(command);
    }
    Ok(commands)
}

#[derive(Debug)]
pub enum Error {
    Parse(Box<dyn error::Error>),
    Io(io::Error),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Parse(resp) => write!(f, "A parse error occured: {resp}"),
            Error::Io(err) => write!(f, "An IO error occured: {err}"),
        }
    }
}
impl error::Error for Error {}
