use std::error;
use std::{fmt, io};


pub mod command;
pub mod resp;
mod resp_consuming;

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
