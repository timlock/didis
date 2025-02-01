use crate::parser::ring_buffer::RingBuffer;
use std::fmt::{self, Display};
use std::io;
use std::io::Read;
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::{error, str};

#[derive(Debug)]
pub enum Error {
    Utf8(Utf8Error),
    ParseInt(ParseIntError),
    LengthMismatch,
    Io(io::Error),
    UnknownResp(u8),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Utf8(e) => e.fmt(f),
            Error::ParseInt(e) => e.fmt(f),
            Error::LengthMismatch => write!(f, "Length does not match"),
            Error::Io(error) => write!(f, "IO error: {error}"),
            Error::UnknownResp(byte) => write!(f, "Unknown resp type: {}", &byte),
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

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::ParseInt(value)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Resp {
    SimpleString(Vec<u8>),
    SimpleError(Vec<u8>),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<Resp>),
    Null,
}

impl Resp {
    pub fn ok() -> Resp {
        Resp::SimpleString(b"OK".to_vec())
    }
}

impl Display for Resp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl From<&Resp> for String {
    fn from(value: &Resp) -> Self {
        let mut string = String::new();
        const CLRF: &str = "\r\n";
        match value {
            Resp::SimpleString(s) => {
                string += "+";
                string += String::from_utf8_lossy(s).into_owned().as_str();
                string += CLRF;
            }
            Resp::SimpleError(e) => {
                string += "-";
                string += String::from_utf8_lossy(e).into_owned().as_str();
                string += CLRF;
            }
            Resp::Integer(i) => {
                string += ":";
                string += i.to_string().as_str();
                string += CLRF;
            }
            Resp::BulkString(b) => {
                string += "$";
                string += b.len().to_string().as_str();
                string += CLRF;
                string += String::from_utf8_lossy(b).into_owned().as_str();
                string += CLRF;
            }
            Resp::Array(a) => {
                string += "*";
                string += a.len().to_string().as_str();
                string += CLRF;
                for i in a {
                    string += String::from(i).as_str();
                }
            }
            Resp::Null => {
                string += "*";
                string += "-1";
                string += CLRF;
            }
        }
        string
    }
}

impl From<Resp> for Vec<u8> {
    fn from(value: Resp) -> Self {
        let mut bytes = Vec::new();
        const CRLF: &[u8; 2] = b"\r\n";
        match value {
            Resp::SimpleString(s) => {
                bytes.push(b'+');
                bytes.extend_from_slice(s.as_slice());
                bytes.extend_from_slice(CRLF);
            }
            Resp::SimpleError(s) => {
                bytes.push(b'-');
                bytes.extend_from_slice(s.as_slice());
                bytes.extend_from_slice(CRLF);
            }
            Resp::Integer(i) => {
                bytes.push(b':');
                bytes.extend_from_slice(i.to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Resp::BulkString(b) => {
                bytes.push(b'$');
                bytes.extend_from_slice(b.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                bytes.extend_from_slice(b.as_slice());
                bytes.extend_from_slice(CRLF);
            }
            Resp::Array(resps) => {
                bytes.push(b'*');
                bytes.extend_from_slice(resps.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                for resp in resps {
                    let serialized = Vec::from(resp);
                    bytes.extend_from_slice(&serialized);
                }
            }
            Resp::Null => bytes.extend_from_slice(b"*-1\r\n"),
        }
        bytes
    }
}

pub struct RingDecoder<T> {
    buf: RingBuffer<4096>,
    reader: T,
}

impl<T> RingDecoder<T>
where
    T: Read,
{
    pub fn new(reader: T) -> Self {
        Self {
            buf: Default::default(),
            reader,
        }
    }
}

impl<T> Iterator for RingDecoder<T>
where
    T: Read,
{
    type Item = Result<Resp, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = self.buf.populate(&mut self.reader) {
            return Some(Err(err.into()));
        }

        let content = self.buf.peek();
        match parse_resp(content.as_slice()) {
            Ok((Some(resp), r)) => {
                self.buf.add_read_pos(content.len() - r.len());
                Some(Ok(resp))
            }
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

pub struct Decoder<T> {
    buf: Vec<u8>,
    reader: T,
}

impl<T> Decoder<T>
where
    T: Read,
{
    pub fn new(reader: T) -> Self {
        Self {
            buf: Vec::new(),
            reader,
        }
    }
}

impl<T> Iterator for Decoder<T>
where
    T: Read,
{
    type Item = Result<Resp, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = try_read(&mut self.reader, &mut self.buf) {
            return Some(Err(err.into()));
        }

        match parse_resp(&self.buf) {
            Ok((Some(resp), r)) => {
                self.buf = r.to_vec();
                Some(Ok(resp))
            }
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

pub fn try_read(reader: &mut impl Read, buffer: &mut Vec<u8>) -> io::Result<()> {
    const CHUNK_SIZE: usize = 1024 * 4;
    let mut buf = [0; CHUNK_SIZE];
    loop {
        match reader.read(&mut buf) {
            Ok(size) => {
                buffer.extend_from_slice(&buf[..size]);
                if size < CHUNK_SIZE {
                    break;
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub fn parse_resp(value: &[u8]) -> Result<(Option<Resp>, &[u8]), Error> {
    if value.is_empty() {
        return Ok((None, value));
    }
    let result = match value[0] {
        b'+' => parse_simple_string(value),
        b'-' => parse_simple_error(value),
        b':' => parse_integer(value)?,
        b'$' => parse_bulk_string(value)?,
        b'*' => parse_array(value)?,
        _ => return Err(Error::UnknownResp(value[0])),
    };

    Ok(result)
}

fn parse_simple_string(value: &[u8]) -> (Option<Resp>, &[u8]) {
    let cr_pos = match value[1..].iter().position(|b| *b == b'\r') {
        Some(pos) => pos,
        None => return (None, value),
    };

    let (data, remaining) = value[1..].split_at(cr_pos);
    if remaining.len() < 2 || b"\r\n" != &remaining[..2] {
        return (None, value);
    }

    (Some(Resp::SimpleString(data.into())), &remaining[2..])
}

fn parse_simple_error(value: &[u8]) -> (Option<Resp>, &[u8]) {
    match parse_simple_string(value) {
        (Some(Resp::SimpleString(s)), r) => (Some(Resp::SimpleError(s)), r),
        _ => (None, value),
    }
}

fn parse_integer(value: &[u8]) -> Result<(Option<Resp>, &[u8]), Error> {
    match parse_length(&value[1..])? {
        (Some(number), r) => Ok((Some(Resp::Integer(number)), r)),
        _ => Ok((None, value)),
    }
}

fn parse_array(value: &[u8]) -> Result<(Option<Resp>, &[u8]), Error> {
    let (length, remaining) = parse_length(&value[1..])?;
    if length.is_none() {
        return Ok((None, value));
    }

    if length.unwrap() == -1 {
        match parse_null(value)? {
            (Some(null), r) => return Ok((Some(null), r)),
            _ => return Ok((None, value)),
        }
    }

    let length = length.unwrap() as usize;
    let mut array = Vec::with_capacity(length);
    let mut contents = remaining;
    for _ in 0..length {
        match parse_resp(contents)? {
            (Some(resp), r) => {
                array.push(resp);
                contents = r;
            }
            (None, r) => {
                if r.is_empty() && array.len() == length {
                    return Ok((Some(Resp::Array(array)), r));
                }
                return Ok((None, r));
            }
        }
    }
    Ok((Some(Resp::Array(array)), contents))
}

fn parse_bulk_string(value: &[u8]) -> Result<(Option<Resp>, &[u8]), Error> {
    let (length, remaining) = parse_length(&value[1..])?;
    if length.is_none() {
        return Ok((None, value));
    }
    if length.unwrap() == -1 {
        match parse_null(value)? {
            (Some(null), r) => return Ok((Some(null), r)),
            _ => return Ok((None, value)),
        }
    }
    let length = length.unwrap() as usize;
    if length > remaining.len() {
        return Ok((None, value));
    }

    let (data, remaining) = remaining.split_at(length);
    if data.len() != length || remaining.len() < 2 || b"\r\n" != &remaining[..2] {
        return Ok((None, value));
    }
    let text = data.to_vec();

    Ok((Some(Resp::BulkString(text)), &remaining[2..]))
}

fn parse_length(value: &[u8]) -> Result<(Option<i64>, &[u8]), Error> {
    let pos = match value.iter().position(|b| *b == b'\r') {
        Some(pos) => pos,
        None => return Ok((None, value)),
    };

    let (length_bytes, remaining) = value.split_at(pos);
    let length_str = str::from_utf8(length_bytes)?;
    let length = length_str.parse()?;
    if value.len() <= pos + 1 || value[pos + 1] != b'\n' {
        return Ok((None, value));
    }
    Ok((Some(length), &remaining[2..]))
}

fn parse_null(value: &[u8]) -> Result<(Option<Resp>, &[u8]), Error> {
    if value.len() < 5 {
        return Ok((None, value));
    }
    let null = &value[1..5];
    match null {
        b"-1\r\n" => Ok((Some(Resp::Null), &value[5..])),
        _ => Err(Error::LengthMismatch),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::mpsc::{channel, Receiver, Sender};

    use super::*;

    #[test]
    fn parse_null() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::Null), r) => {
                assert!(r.is_empty());
                Ok(())
            }
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn parse_array() -> Result<(), Box<dyn error::Error>> {
        let input = "*1\r\n$4\r\nping\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 1);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, b"ping"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array2() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n";
        let (actual, r) = parse_resp(input.as_bytes())?;
        assert!(actual.is_some());
        assert!(r.is_empty());
        assert_eq!(input, actual.unwrap().to_string());
        match parse_resp(input.as_bytes())? {
            (Some(Resp::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, b"echo"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, b"hello world"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array3() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, b"get"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, b"key"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array4() -> Result<(), Box<dyn error::Error>> {
        let input = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::Array(arr)), r) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, b"CONFIG"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, b"GET"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &arr[2] {
                    Resp::BulkString(s) => assert_eq!(s, b"save"),
                    _ => return Err("Expected bulk string".into()),
                }
                assert!(!r.is_empty());
                match parse_resp(r)? {
                    (Some(Resp::Array(arr)), r) => {
                        assert_eq!(arr.len(), 3);
                        assert!(r.is_empty());
                        match &arr[0] {
                            Resp::BulkString(s) => assert_eq!(s, b"CONFIG"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &arr[1] {
                            Resp::BulkString(s) => assert_eq!(s, b"GET"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &arr[2] {
                            Resp::BulkString(s) => assert_eq!(s, b"appendonly"),
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
    fn parse_simple_string() -> Result<(), Box<dyn error::Error>> {
        let input = "+OK\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::SimpleString(s)), _) => {
                assert_eq!(s, b"OK");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn parse_simple_error() -> Result<(), Box<dyn error::Error>> {
        let input = "-ERROR message\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::SimpleError(s)), _) => {
                assert_eq!(s, b"ERROR message");
                Ok(())
            }
            _ => Err("Should be of type simple error".into()),
        }
    }

    #[test]
    fn parse_empty_bulk_string() -> Result<(), Box<dyn error::Error>> {
        let input = "$0\r\n\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::BulkString(s)), _) => {
                assert_eq!(s, b"");
                Ok(())
            }
            _ => Err("Should be of type bulk string".into()),
        }
    }

    // #[test]
    // fn parse_bulk_string() -> Result<(), Box<dyn error::Error>> {
    //     let mut input = b"2\r\nab\r\n".as_slice();
    //     let mut parser = BulkStringPars::new(()).map_err(|err| err.to_string())?;
    //     match parser.parse(&mut input)? {
    //         Some(Resp::BulkString(s)) => {
    //             assert_eq!(b"ab", s.as_slice());
    //             Ok(())
    //         }
    //         Some(resp) => Err(Box::from(format!("got wrong resp {resp}"))),
    //         None => Err(Box::from("there should be a parsed resp")),
    //     }
    // }

    #[test]
    fn parse_simple_string2() -> Result<(), Box<dyn error::Error>> {
        let input = "+hello world\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Resp::SimpleString(s)), _) => {
                assert_eq!(s, b"hello world");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    struct MockStream {
        receiver: Receiver<Vec<u8>>,
    }

    impl MockStream {
        fn new() -> (MockStream, Sender<Vec<u8>>) {
            let (sender, receiver) = channel();
            (Self { receiver }, sender)
        }
    }

    impl Read for MockStream {
        fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
            let data = self.receiver.try_recv().unwrap_or_default();
            buf.write(&data)
        }
    }

    #[test]
    fn decoder_parse_null() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n".as_bytes();
        let mut parser = Decoder::new(input);
        match parser.next() {
            Some(Ok(Resp::Null)) => Ok(()),
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn decoder_parse_null_split() -> Result<(), Box<dyn error::Error>> {
        let input = [b'$', b'-', b'1', b'\r', b'\n'];
        let (stream, sender) = MockStream::new();
        let mut parser = Decoder::new(stream);

        assert!(parser.next().is_none());
        sender.send(vec![input[0]]).map_err(|_| "Send error")?;
        assert!(parser.next().is_none());
        sender.send(vec![input[1]]).map_err(|_| "Send error")?;
        assert!(parser.next().is_none());
        sender.send(vec![input[2]]).map_err(|_| "Send error")?;
        assert!(parser.next().is_none());
        sender.send(vec![input[3]]).map_err(|_| "Send error")?;
        assert!(parser.next().is_none());

        sender.send(vec![input[4]]).map_err(|_| "Send error")?;
        match parser.next() {
            Some(Ok(Resp::Null)) => Ok(()),
            _ => Err("Should be null".into()),
        }
    }
}
