use std::fmt::{self, Display, Formatter};
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::{error, str};
use std::{io, mem};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ValOrRef<'a> {
    Val(Value),
    Ref(Reference<'a>),
}

impl<'a> ValOrRef<'a> {
    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            ValOrRef::Val(value) => value.to_bytes(),
            ValOrRef::Ref(reference) => reference.to_bytes(),
        }
    }

    pub fn as_ref(&self) -> Reference {
        match self {
            ValOrRef::Val(value) => value.as_reference(),
            ValOrRef::Ref(reference) => reference.clone(),
        }
    }

    pub fn to_value(self) -> Value {
        match self {
            ValOrRef::Val(value) => value,
            ValOrRef::Ref(reference) => reference.to_value(),
        }
    }
}

impl<'a> Display for ValOrRef<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ValOrRef::Val(value) => value.fmt(f),
            ValOrRef::Ref(reference) => reference.fmt(f),
        }
    }
}

impl<'a> From<Value> for ValOrRef<'a> {
    fn from(value: Value) -> Self {
        ValOrRef::Val(value)
    }
}

impl<'a> From<Reference<'a>> for ValOrRef<'a> {
    fn from(value: Reference<'a>) -> Self {
        ValOrRef::Ref(value)
    }
}

#[derive(Debug)]
pub enum Error {
    FromUtf8(FromUtf8Error),
    Utf8(Utf8Error),
    ParseInt(ParseIntError),
    LengthMismatch,
    Io(io::Error),
    UnknownResp(u8),
    InvalidToken(u8),
}

impl error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FromUtf8(e) => e.fmt(f),
            Error::Utf8(e) => e.fmt(f),
            Error::ParseInt(e) => e.fmt(f),
            Error::LengthMismatch => write!(f, "Length does not match"),
            Error::Io(error) => write!(f, "IO error: {error}"),
            Error::UnknownResp(byte) => write!(f, "Unknown resp type: {}", &byte),
            Error::InvalidToken(token) => write!(f, "Invalid token: {}", &token),
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::Utf8(value)
    }
}
impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::FromUtf8(value)
    }
}

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::ParseInt(value)
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Value {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
    Null,
}

impl Value {
    pub fn ok() -> Value {
        Value::SimpleString("OK".to_string())
    }
    pub fn to_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        const CRLF: &[u8; 2] = b"\r\n";
        match self {
            Value::SimpleString(s) => {
                bytes.push(b'+');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Value::SimpleError(s) => {
                bytes.push(b'-');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Value::Integer(i) => {
                bytes.push(b':');
                bytes.extend_from_slice(i.to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Value::BulkString(b) => {
                bytes.push(b'$');
                bytes.extend_from_slice(b.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                bytes.extend_from_slice(b.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Value::Array(resps) => {
                bytes.push(b'*');
                bytes.extend_from_slice(resps.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                for resp in resps {
                    let serialized = resp.to_bytes();
                    bytes.extend_from_slice(&serialized);
                }
            }
            Value::Null => bytes.extend_from_slice(b"*-1\r\n"),
        }
        bytes
    }

    pub fn as_reference(&self) -> Reference {
        match self {
            Value::SimpleString(value) => Reference::SimpleString(value),
            Value::SimpleError(value) => Reference::SimpleError(value),
            Value::Integer(value) => Reference::Integer(*value),
            Value::BulkString(value) => Reference::BulkString(value),
            Value::Array(value) => {
                Reference::Array(value.iter().map(Value::as_reference).collect())
            }
            Value::Null => Reference::Null,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::SimpleString(value) => write!(f, "+{}\\r\\n", value),
            Value::SimpleError(value) => write!(f, "-{}\\r\\n", value),
            Value::Integer(value) => write!(f, ":{}\\r\\n", value),
            Value::BulkString(value) => {
                if value.len() < 32 {
                    write!(f, "${}\\r\\n{}\\r\\n", value.len(), value)
                } else {
                    write!(
                        f,
                        "${}\\r\\n{}...(shortened)\\r\\n",
                        value.len(),
                        &value[..32]
                    )
                }
            }
            Value::Array(values) => {
                write!(f, "*{}\\r\\n", values.len())?;
                for value in values {
                    write!(f, "{}", value)?;
                }
                write!(f, "\\r\\n",)
            }
            Value::Null => write!(f, "_\\r\\n"),
        }
    }
}

impl From<&Value> for String {
    fn from(value: &Value) -> Self {
        let mut string = String::new();
        const CLRF: &str = "\r\n";
        match value {
            Value::SimpleString(s) => {
                string += "+";
                string += s;
                string += CLRF;
            }
            Value::SimpleError(e) => {
                string += "-";
                string += e;
                string += CLRF;
            }
            Value::Integer(i) => {
                string += ":";
                string += i.to_string().as_str();
                string += CLRF;
            }
            Value::BulkString(b) => {
                string += "$";
                string += b.len().to_string().as_str();
                string += CLRF;
                string += b;
                string += CLRF;
            }
            Value::Array(a) => {
                string += "*";
                string += a.len().to_string().as_str();
                string += CLRF;
                for i in a {
                    string += String::from(i).as_str();
                }
            }
            Value::Null => {
                string += "*";
                string += "-1";
                string += CLRF;
            }
        }
        string
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Reference<'a> {
    SimpleString(&'a str),
    SimpleError(&'a str),
    Integer(i64),
    BulkString(&'a str),
    Array(Vec<Reference<'a>>),
    Null,
}

impl<'a> Reference<'a> {
    pub fn ok() -> Reference<'a> {
        Reference::SimpleString("OK")
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        const CRLF: &[u8; 2] = b"\r\n";
        match self {
            Reference::SimpleString(s) => {
                bytes.push(b'+');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Reference::SimpleError(s) => {
                bytes.push(b'-');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Reference::Integer(i) => {
                bytes.push(b':');
                bytes.extend_from_slice(i.to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Reference::BulkString(b) => {
                bytes.push(b'$');
                bytes.extend_from_slice(b.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                bytes.extend_from_slice(b.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Reference::Array(resps) => {
                bytes.push(b'*');
                bytes.extend_from_slice(resps.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                for resp in resps {
                    let serialized = resp.to_bytes();
                    bytes.extend_from_slice(&serialized);
                }
            }
            Reference::Null => bytes.extend_from_slice(b"*-1\r\n"),
        }
        bytes
    }

    pub fn to_value(self) -> Value {
        match self {
            Reference::SimpleString(value) => Value::SimpleString(value.to_owned()),
            Reference::SimpleError(value) => Value::SimpleError(value.to_owned()),
            Reference::Integer(value) => Value::Integer(value),
            Reference::BulkString(value) => Value::BulkString(value.to_owned()),
            Reference::Array(values) => {
                let values = values.into_iter().map(|value| value.to_value()).collect();
                Value::Array(values)
            }
            Reference::Null => Value::Null,
        }
    }
}
impl<'a> Display for Reference<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reference::SimpleString(value) => write!(f, "+{}\\r\\n", value),
            Reference::SimpleError(value) => write!(f, "-{}\\r\\n", value),
            Reference::Integer(value) => write!(f, ":{}\\r\\n", value),
            Reference::BulkString(value) => {
                if value.len() < 32 {
                    write!(f, "${}\\r\\n{}\\r\\n", value.len(), value)
                } else {
                    write!(
                        f,
                        "${}\\r\\n{}...(shortened)\\r\\n",
                        value.len(),
                        &value[..32]
                    )
                }
            }
            Reference::Array(values) => {
                write!(f, "*{}\\r\\n", values.len())?;
                for value in values {
                    write!(f, "{}", value)?;
                }
                write!(f, "\\r\\n",)
            }
            Reference::Null => write!(f, "_\\r\\n"),
        }
    }
}

impl<'a> From<&Reference<'a>> for String {
    fn from(value: &Reference<'a>) -> Self {
        let mut string = String::new();
        const CLRF: &str = "\r\n";
        match value {
            Reference::SimpleString(s) => {
                string += "+";
                string += s;
                string += CLRF;
            }
            Reference::SimpleError(e) => {
                string += "-";
                string += e;
                string += CLRF;
            }
            Reference::Integer(i) => {
                string += ":";
                string += i.to_string().as_str();
                string += CLRF;
            }
            Reference::BulkString(b) => {
                string += "$";
                string += b.len().to_string().as_str();
                string += CLRF;
                string += b;
                string += CLRF;
            }
            Reference::Array(a) => {
                string += "*";
                string += a.len().to_string().as_str();
                string += CLRF;
                for i in a {
                    string += String::from(i).as_str();
                }
            }
            Reference::Null => {
                string += "*";
                string += "-1";
                string += CLRF;
            }
        }
        string
    }
}

pub enum ParsedValue<'a, C, I> {
    Complete(C, &'a [u8]),
    Incomplete(I),
}

pub fn parse(value: &[u8]) -> Result<ParsedValue<Reference, Parser>, Error> {
    match expect_value(value)? {
        Some((value_ref, remaining)) => Ok(ParsedValue::Complete(value_ref, remaining)),
        None => {
            let mut parser = Parser::default();
            parser.parse(value)?;
            Ok(ParsedValue::Incomplete(parser))
        }
    }
}

#[derive(Debug)]
enum ParserState {
    SimpleString(LineParser),
    SimpleError(LineParser),
    Integer(IntegerParser),
    BulkString(BulkStringParser),
    Array(ArrayParser),
    None,
}

impl TryFrom<u8> for ParserState {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'+' => Ok(ParserState::SimpleString(LineParser::default())),
            b'-' => Ok(ParserState::SimpleError(LineParser::default())),
            b':' => Ok(ParserState::Integer(IntegerParser::default())),
            b'$' => Ok(ParserState::BulkString(BulkStringParser::default())),
            b'*' => Ok(ParserState::Array(ArrayParser::default())),
            _ => Err(Error::UnknownResp(value)),
        }
    }
}
#[derive(Debug)]
pub struct Parser {
    state: ParserState,
}
impl<'a> Parser {
    pub fn parse(&mut self, value: &'a [u8]) -> Result<Option<(Value, &'a [u8])>, Error> {
        let (value, remaining) = match &mut self.state {
            ParserState::SimpleString(parser) => match parser.parse(value)? {
                Some((value, remaining)) => (Value::SimpleString(value), remaining),
                None => return Ok(None),
            },
            ParserState::SimpleError(parser) => match parser.parse(value)? {
                Some((value, remaining)) => (Value::SimpleError(value), remaining),
                None => return Ok(None),
            },
            ParserState::Integer(parser) => match parser.parse(value)? {
                Some((value, remaining)) => (Value::Integer(value), remaining),
                None => return Ok(None),
            },
            ParserState::BulkString(parser) => match parser.parse(value)? {
                Some((value, remaining)) => (value, remaining),
                None => return Ok(None),
            },
            ParserState::Array(parser) => match parser.parse(value)? {
                Some((value, remaining)) => (value, remaining),
                None => return Ok(None),
            },

            ParserState::None => match value.get(0) {
                Some(identifier) => {
                    self.state = ParserState::try_from(*identifier)?;
                    match self.parse(&value[1..])? {
                        Some((value, remaining)) => (value, remaining),
                        None => return Ok(None),
                    }
                }
                None => return Ok(None),
            },
        };

        self.state = ParserState::None;

        Ok(Some((value, remaining)))
    }

    pub fn parse_all(&mut self, mut bytes: &'a [u8]) -> Result<Vec<Value>, Error> {
        let mut values = Vec::new();
        while let Some((value, remaining)) = self.parse(bytes)? {
            values.push(value);
            bytes = remaining;
        }
        
        Ok(values)
    }
}

impl Default for Parser {
    fn default() -> Self {
        Parser {
            state: ParserState::None,
        }
    }
}

#[derive(Debug, Default)]
struct LineParser {
    buffer: Vec<u8>,
}

impl<'a> LineParser {
    fn parse(&mut self, value: &'a [u8]) -> Result<Option<(String, &'a [u8])>, Error> {
        let mut iter = self.buffer.iter().chain(value).copied();
        match try_line(&mut iter)? {
            Some(len) => {
                let missing = len - self.buffer.len();
                let (line, remaining) = value.split_at(missing);
                self.buffer.extend_from_slice(line);
                self.buffer.pop();
                self.buffer.pop();
                let line = String::from_utf8(mem::take(&mut self.buffer))?;
                Ok(Some((line, remaining)))
            }
            None => {
                self.buffer.extend_from_slice(value);
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Default)]
struct IntegerParser {
    line_parser: LineParser,
}

impl<'a> IntegerParser {
    fn parse(&mut self, value: &'a [u8]) -> Result<Option<(i64, &'a [u8])>, Error> {
        match self.line_parser.parse(value)? {
            Some((line, remaining)) => Ok(Some((line.parse()?, remaining))),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Default)]
struct BulkStringParser {
    integer_parser: IntegerParser,
    buffer: Vec<u8>,
    length: Option<i64>,
}

impl<'a> BulkStringParser {
    fn parse(&mut self, mut value: &'a [u8]) -> Result<Option<(Value, &'a [u8])>, Error> {
        if self.length.is_none() {
            match self.integer_parser.parse(value)? {
                Some((length, remaining)) => {
                    self.length = Some(length);
                    value = remaining;
                }
                None => return Ok(None),
            };
        }

        if let Some(-1) = self.length {
            return Ok(Some((Value::Null, value)));
        }

        if let Some(length) = self.length {
            let mut iter = self.buffer.iter().chain(value).copied();
            match try_bulk(&mut iter, length as usize)? {
                Some(len) => {
                    let missing = len - self.buffer.len();
                    let (bulk_str, remaining) = value.split_at(missing);
                    self.buffer.extend_from_slice(bulk_str);
                    self.buffer.pop();
                    self.buffer.pop();
                    let string = String::from_utf8(mem::take(&mut self.buffer))?;
                    return Ok(Some((Value::BulkString(string), remaining)));
                }
                None => self.buffer.extend_from_slice(value),
            };
        }

        Ok(None)
    }
}

#[derive(Debug, Default)]
struct ArrayParser {
    integer_parser: IntegerParser,
    length: Option<i64>,
    items: Vec<Value>,
    parser: Box<Parser>,
}

impl<'a> ArrayParser {
    fn parse(&mut self, mut value: &'a [u8]) -> Result<Option<(Value, &'a [u8])>, Error> {
        if self.length.is_none() {
            match self.integer_parser.parse(value)? {
                Some((length, remaining)) => {
                    self.length = Some(length);
                    value = remaining;
                }
                None => return Ok(None),
            };
        }

        if let Some(-1) = self.length {
            return Ok(Some((Value::Null, value)));
        }

        if let Some(length) = self.length {
            while self.items.len() < length as usize {
                match self.parser.parse(value)? {
                    Some((item, remaining)) => {
                        self.items.push(item);
                        value = remaining;
                    }
                    None => return Ok(None),
                };
            }
        }

        Ok(Some((Value::Array(mem::take(&mut self.items)), value)))
    }
}

fn try_line(iter: impl IntoIterator<Item = u8>) -> Result<Option<usize>, Error> {
    let mut iter = iter.into_iter().enumerate();
    if let None = iter.find(|(_, b)| *b == b'\r') {
        return Ok(None);
    };
    let lf_pos = match iter.next() {
        Some((pos, b'\n')) => pos,
        Some((_, other)) => return Err(Error::InvalidToken(other)),
        None => return Ok(None),
    };

    Ok(Some(lf_pos + 1)) // lf_pos is the index, we need line length
}

fn try_bulk(iter: impl IntoIterator<Item = u8>, length: usize) -> Result<Option<usize>, Error> {
    let mut iter = iter.into_iter().skip(length);
    match iter.next() {
        Some(b'\r') => {}
        Some(other) => return Err(Error::InvalidToken(other)),
        None => return Ok(None),
    };
    match iter.next() {
        Some(b'\n') => {}
        Some(other) => return Err(Error::InvalidToken(other)),
        None => return Ok(None),
    };

    Ok(Some(length + 2))
}

fn expect_line(value: &[u8]) -> Result<Option<(&str, &[u8])>, Error> {
    let length = match try_line(value.iter().copied())? {
        Some(length) => length,
        None => return Ok(None),
    };

    let (line, remaining) = value.split_at(length);
    let (line, _) = line.split_at(line.len() - 2);
    let line = str::from_utf8(line)?;
    Ok(Some((line, remaining)))
}

fn expect_simple_string(value: &[u8]) -> Result<Option<(Reference, &[u8])>, Error> {
    let (line, remaining) = match expect_line(value)? {
        Some((line, remaining)) => (line, remaining),
        None => return Ok(None),
    };

    Ok(Some((Reference::SimpleString(line), remaining)))
}

fn expect_simple_error(value: &[u8]) -> Result<Option<(Reference, &[u8])>, Error> {
    let (line, remaining) = match expect_line(value)? {
        Some((line, remaining)) => (line, remaining),
        None => return Ok(None),
    };

    Ok(Some((Reference::SimpleError(line), remaining)))
}

fn expect_length(value: &[u8]) -> Result<Option<(i64, &[u8])>, Error> {
    match expect_line(value)? {
        Some((line, remaining)) => Ok(Some((line.parse()?, remaining))),
        None => Ok(None),
    }
}

fn expect_integer(value: &[u8]) -> Result<Option<(Reference, &[u8])>, Error> {
    let (integer, remaining) = match expect_length(value)? {
        Some((line, remaining)) => (line, remaining),
        None => return Ok(None),
    };

    Ok(Some((Reference::Integer(integer), remaining)))
}

fn expect_bulk_string(value: &[u8]) -> Result<Option<(Reference, &[u8])>, Error> {
    let (length, remaining) = match expect_length(value)? {
        Some((length, remaining)) => (length, remaining),
        None => return Ok(None),
    };

    if length == -1 {
        return Ok(Some((Reference::Null, remaining)));
    }

    let length = match try_bulk(remaining.iter().copied(), length as usize)? {
        Some(length) => length,
        None => return Ok(None),
    };

    let (bulk_str, remaining) = remaining.split_at(length);
    let (bulk_str, _) = bulk_str.split_at(bulk_str.len() - 2);
    let string = str::from_utf8(bulk_str)?;
    Ok(Some((Reference::BulkString(string), remaining)))
}

fn expect_array(value: &[u8]) -> Result<Option<(Reference, &[u8])>, Error> {
    let (length, mut remaining) = match expect_length(value)? {
        Some((length, remaining)) => (length, remaining),
        None => return Ok(None),
    };

    if length == -1 {
        return Ok(Some((Reference::Null, remaining)));
    }
    let length = length as usize;
    let mut items = Vec::with_capacity(length);
    for _ in 0..length {
        let (value, r) = match expect_value(remaining)? {
            Some((value, remaining)) => (value, remaining),
            None => return Ok(None),
        };
        items.push(value);
        remaining = r;
    }

    Ok(Some((Reference::Array(items), remaining)))
}

fn expect_value(value: &[u8]) -> Result<Option<(Reference, &[u8])>, Error> {
    if value.is_empty() {
        return Ok(None);
    }

    let type_prefix = value[0];
    let value = &value[1..];

    let result = match type_prefix {
        b'+' => expect_simple_string(value)?,
        b'-' => expect_simple_error(value)?,
        b':' => expect_integer(value)?,
        b'$' => expect_bulk_string(value)?,
        b'*' => expect_array(value)?,
        _ => return Err(Error::UnknownResp(value[0])),
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_null() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::Null, remaining)) => {
                assert_eq!(b"", remaining);
                Ok(())
            }
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn parse_array() -> Result<(), Box<dyn error::Error>> {
        let input = "*1\r\n$4\r\nping\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::Array(array), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(array.len(), 1);
                assert_eq!(Reference::BulkString("ping"), array[0]);
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array2() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::Array(array), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(array.len(), 2);
                assert_eq!(Reference::BulkString("echo"), array[0]);
                assert_eq!(Reference::BulkString("hello world"), array[1]);
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array3() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::Array(array), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(array.len(), 2);
                assert_eq!(Reference::BulkString("get"), array[0]);
                assert_eq!(Reference::BulkString("key"), array[1]);
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array4() -> Result<(), Box<dyn error::Error>> {
        let input = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::Array(array), remaining)) => {
                assert_eq!(
                    b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n",
                    remaining
                );
                assert_eq!(array.len(), 3);
                assert_eq!(Reference::BulkString("CONFIG"), array[0]);
                assert_eq!(Reference::BulkString("GET"), array[1]);
                assert_eq!(Reference::BulkString("save"), array[2]);
                match expect_value(remaining)? {
                    Some((Reference::Array(array), remaining)) => {
                        assert_eq!(b"", remaining);
                        assert_eq!(array.len(), 3);
                        assert_eq!(Reference::BulkString("CONFIG"), array[0]);
                        assert_eq!(Reference::BulkString("GET"), array[1]);
                        assert_eq!(Reference::BulkString("appendonly"), array[2]);
                    }
                    _ => return Err("Should be of type array".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_simple_string() -> Result<(), Box<dyn error::Error>> {
        let input = "+OK\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::SimpleString(string), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(string, "OK");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn parse_simple_error() -> Result<(), Box<dyn error::Error>> {
        let input = "-ERROR message\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::SimpleError(string), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(string, "ERROR message");
                Ok(())
            }
            _ => Err("Should be of type simple error".into()),
        }
    }

    #[test]
    fn parse_empty_bulk_string() -> Result<(), Box<dyn error::Error>> {
        let input = "$0\r\n\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::BulkString(string), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!("", string);

                Ok(())
            }
            _ => Err("Should be of type bulk string".into()),
        }
    }

    #[test]
    fn parse_simple_string2() -> Result<(), Box<dyn error::Error>> {
        let input = "+hello world\r\n";
        match expect_value(input.as_bytes())? {
            Some((Reference::SimpleString(string), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!("hello world", string);
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn parse_null_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::Null, remaining)) => {
                assert_eq!(b"", remaining);
                Ok(())
            }
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn parse_array_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "*1\r\n$4\r\nping\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::Array(array), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(array.len(), 1);
                match &array[0] {
                    Value::BulkString(s) => assert_eq!(s, "ping"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array2_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::Array(array), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(array.len(), 2);
                match &array[0] {
                    Value::BulkString(s) => assert_eq!(s, "echo"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                match &array[1] {
                    Value::BulkString(s) => assert_eq!(s, "hello world"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array3_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::Array(array), remaining)) => {
                assert_eq!(b"", remaining);
                assert_eq!(array.len(), 2);
                match &array[0] {
                    Value::BulkString(s) => assert_eq!(s, "get"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                match &array[1] {
                    Value::BulkString(s) => assert_eq!(s, "key"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array4_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n";
        let mut parser = Parser::default();
        match parser.parse(input.as_bytes())? {
            Some((Value::Array(array), remaining)) => {
                assert_eq!(array.len(), 3);
                match &array[0] {
                    Value::BulkString(s) => assert_eq!(s, "CONFIG"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &array[1] {
                    Value::BulkString(s) => assert_eq!(s, "GET"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &array[2] {
                    Value::BulkString(s) => assert_eq!(s, "save"),
                    _ => return Err("Expected bulk string".into()),
                }
                assert_eq!(
                    b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n",
                    remaining
                );
                match parser.parse(remaining)? {
                    Some((Value::Array(array), remaining)) => {
                        assert_eq!(array.len(), 3);
                        assert_eq!(b"", remaining);
                        match &array[0] {
                            Value::BulkString(s) => assert_eq!(s, "CONFIG"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &array[1] {
                            Value::BulkString(s) => assert_eq!(s, "GET"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &array[2] {
                            Value::BulkString(s) => assert_eq!(s, "appendonly"),
                            _ => return Err("Expected bulk string".into()),
                        }
                    }
                    _ => return Err("Should be of type array".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_simple_string_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "+OK\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::SimpleString(s), b"")) => {
                assert_eq!(s, "OK");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn parse_simple_error_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "-ERROR message\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::SimpleError(s), b"")) => {
                assert_eq!(s, "ERROR message");
                Ok(())
            }
            _ => Err("Should be of type simple error".into()),
        }
    }

    #[test]
    fn parse_empty_bulk_string_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "$0\r\n\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::BulkString(s), b"")) => {
                assert_eq!(s, "");
                Ok(())
            }
            _ => Err("Should be of type bulk string".into()),
        }
    }

    #[test]
    fn parse_simple_string2_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "+hello world\r\n";
        match Parser::default().parse(input.as_bytes())? {
            Some((Value::SimpleString(s), b"")) => {
                assert_eq!(s, "hello world");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn decoder_parse_null_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n".as_bytes();
        let mut parser = Parser::default();
        match parser.parse(input) {
            Ok(Some((Value::Null, b""))) => Ok(()),
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn decoder_parse_null_split_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = [b'$', b'-', b'1', b'\r', b'\n'];
        let mut parser = Parser::default();

        assert!(parser.parse(&input[0..1])?.is_none());
        assert!(parser.parse(&input[1..2])?.is_none());
        assert!(parser.parse(&input[2..3])?.is_none());
        assert!(parser.parse(&input[3..4])?.is_none());
        assert_eq!(
            Some((Value::Null, b"".as_slice())),
            parser.parse(&input[4..5])?
        );

        Ok(())
    }
}
