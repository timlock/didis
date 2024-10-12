use core::error;
use std::fmt::{self, Display};
use std::io;
use std::io::Read;

pub struct Decoder<T> {
    buf: Vec<u8>,
    reader: io::BufReader<T>,
}

impl<T> Decoder<T>
where
    T: io::Read ,
{
    pub fn new(reader: T) -> Self {
        Self {
            buf: Vec::new(),
            reader: io::BufReader::new(reader),
        }
    }
}

impl<T> Iterator for Decoder<T>
where
    T: io::Read,
{
    type Item = Result<Resp, super::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = try_read(&mut self.reader, &mut self.buf) {
            return Some(Err(super::Error::Io(err)));
        }

        let old_len = self.buf.len();
        match parse_resp(&self.buf) {
            (Some(resp), r) => {
                self.buf = r.to_vec();
                Some(Ok(resp))
            }
            (None, r) if r.len() == old_len => None,
            (None, []) => None,
            (None, r) => Some(Err(super::Error::Parse(Box::new(Error::Misc(
                String::from_utf8_lossy(r).into_owned(),
            ))))),
        }
    }
}

pub fn try_read(reader: &mut impl io::Read, buffer: &mut Vec<u8>) -> io::Result<()> {
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

#[derive(Debug)]
pub enum Error {
    LengthMismatch(usize),
    Parse(Box<dyn error::Error>),
    Truncated,
    Misc(String),
}

impl error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::LengthMismatch(len) => write!(f, "Length does not match: {len}"),
            Error::Parse(err) => write!(f, "Parse error: {err}"),
            Error::Truncated => write!(f, "Resp is truncated"),
            Error::Misc(desc) => write!(f, "Unknown error {desc}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Resp {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<Resp>),
    Null,
}

impl Resp {
    pub fn ok() -> Resp {
        Resp::SimpleString(String::from("OK"))
    }
    pub fn unknown_command(command: &str) -> Resp {
        Resp::SimpleError(format!("Unknown command '{command}'"))
    }

    pub fn wrong_number_of_arguments() -> Resp {
        Resp::SimpleError(String::from("ERR wrong number of arguments for command"))
    }
    pub fn invalid_arguments() -> Resp {
        Resp::SimpleError(String::from("ERR wrong number of arguments for command"))
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
                string += s.as_str();
                string += CLRF;
            }
            Resp::SimpleError(e) => {
                string += "-";
                string += e.as_str();
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
                string += b.as_str();
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
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            Resp::SimpleError(s) => {
                bytes.push(b'-');
                bytes.extend_from_slice(s.as_bytes());
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
                bytes.extend_from_slice(b.as_bytes());
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

fn parse_resp(value: &[u8]) -> (Option<Resp>, &[u8]) {
    if value.is_empty() {
        return (None, value);
    }
    match value[0] {
        b'+' => parse_simple_string(value),
        b'-' => parse_simple_error(value),
        b':' => parse_integer(value),
        b'$' => parse_bulk_string(value),
        b'*' => parse_array(value),
        _ => (None, value),
    }
}

fn parse_simple_string(value: &[u8]) -> (Option<Resp>, &[u8]) {
    let pos = value[1..].iter().position(|b| *b == b'\r');
    if pos.is_none() {
        return (None, value);
    }
    let pos = pos.unwrap();
    let (data, remaining) = value[1..].split_at(pos);
    if remaining.len() < 2 || b"\r\n" != &remaining[..2] {
        return (None, value);
    }
    let text = String::from_utf8_lossy(data).to_string();
    (Some(Resp::SimpleString(text)), &remaining[2..])
}

fn parse_simple_error(value: &[u8]) -> (Option<Resp>, &[u8]) {
    match parse_simple_string(value) {
        (Some(Resp::SimpleString(s)), r) => (Some(Resp::SimpleError(s)), r),
        _ => (None, value),
    }
}

fn parse_integer(value: &[u8]) -> (Option<Resp>, &[u8]) {
    match parse_length(&value[1..]) {
        (Some(i), r) => (Some(Resp::Integer(i)), r),
        _ => (None, value),
    }
}

fn parse_array(value: &[u8]) -> (Option<Resp>, &[u8]) {
    let (length, remaining) = parse_length(&value[1..]);
    if length.is_none() {
        return (None, value);
    }
    if length.unwrap() == -1 {
        match parse_null(value) {
            (Some(null), r) => return (Some(null), r),
            _ => return (None, value),
        }
    }
    let length = length.unwrap() as usize;
    let mut array = Vec::with_capacity(length);
    let mut contents = remaining;
    for _ in 0..length {
        match parse_resp(contents) {
            (Some(resp), r) => {
                array.push(resp);
                contents = r;
            }
            (None, r) => {
                if r.is_empty() && array.len() == length {
                    return (Some(Resp::Array(array)), r);
                }
                return (None, r);
            }
        }
    }
    (Some(Resp::Array(array)), contents)
}

fn parse_bulk_string(value: &[u8]) -> (Option<Resp>, &[u8]) {
    let (length, remaining) = parse_length(&value[1..]);
    if length.is_none() {
        return (None, value);
    }
    if length.unwrap() == -1 {
        match parse_null(value) {
            (Some(null), r) => return (Some(null), r),
            _ => return (None, value),
        }
    }
    let length = length.unwrap() as usize;
    let (data, remaining) = remaining.split_at(length);
    let text = String::from_utf8_lossy(data).to_string();
    if text.len() != length || remaining.len() < 2 || b"\r\n" != &remaining[..2] {
        return (None, value);
    }
    (Some(Resp::BulkString(text)), &remaining[2..])
}

fn parse_length(value: &[u8]) -> (Option<i64>, &[u8]) {
    let pos = value.iter().position(|b| *b == b'\r');
    if pos.is_none() {
        return (None, value);
    }
    let pos = pos.unwrap();
    let (binary, remaining) = value.split_at(pos);
    let binary_str = match std::str::from_utf8(binary) {
        Ok(s) => s,
        Err(_) => return (None, value),
    };
    let length = match binary_str.parse() {
        Ok(l) => l,
        Err(_) => return (None, value),
    };
    if value.len() <= pos + 1 || value[pos + 1] != b'\n' {
        return (None, value);
    }
    (Some(length), &remaining[2..])
}

fn parse_null(value: &[u8]) -> (Option<Resp>, &[u8]) {
    if value.len() < 5 {
        return (None, value);
    }
    let null = &value[1..5];
    match null {
        b"-1\r\n" => (Some(Resp::Null), &value[5..]),
        _ => (None, &value[4..]),
    }
}

mod tests {
    use std::io::Write;
    use std::sync::mpsc::{channel, Receiver, Sender};

    use super::*;

    #[test]
    fn parse_null() -> Result<(), &'static str> {
        let input = "$-1\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::Null), r) => {
                assert!(r.is_empty());
                Ok(())
            }
            _ => Err("Should be null"),
        }
    }

    #[test]
    fn parse_array() -> Result<(), &'static str> {
        let input = "*1\r\n$4\r\nping\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 1);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, "ping"),
                    _ => return Err("Array should contain a simple string"),
                }
                Ok(())
            }
            _ => Err("Should be of type array"),
        }
    }

    #[test]
    fn parse_array2() -> Result<(), &'static str> {
        let input = "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n";
        let (actual, r) = parse_resp(input.as_bytes());
        assert!(actual.is_some());
        assert!(r.is_empty());
        assert_eq!(input, actual.unwrap().to_string());
        match parse_resp(input.as_bytes()) {
            (Some(Resp::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, "echo"),
                    _ => return Err("Array should contain a simple string"),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, "hello world"),
                    _ => return Err("Array should contain a simple string"),
                }
                Ok(())
            }
            _ => Err("Should be of type array"),
        }
    }

    #[test]
    fn parse_array3() -> Result<(), &'static str> {
        let input = "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, "get"),
                    _ => return Err("Array should contain a bulk string"),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, "key"),
                    _ => return Err("Array should contain a bulk string"),
                }
                Ok(())
            }
            _ => Err("Should be of type array"),
        }
    }

    #[test]
    fn parse_array4() -> Result<(), &'static str> {
        let input = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::Array(arr)), r) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    Resp::BulkString(s) => assert_eq!(s, "CONFIG"),
                    _ => return Err("Expected bulk string"),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, "GET"),
                    _ => return Err("Expected bulk string"),
                }
                match &arr[2] {
                    Resp::BulkString(s) => assert_eq!(s, "save"),
                    _ => return Err("Expected bulk string"),
                }
                assert!(!r.is_empty());
                match parse_resp(r) {
                    (Some(Resp::Array(arr)), r) => {
                        assert_eq!(arr.len(), 3);
                        assert!(r.is_empty());
                        match &arr[0] {
                            Resp::BulkString(s) => assert_eq!(s, "CONFIG"),
                            _ => return Err("Expected bulk string"),
                        }
                        match &arr[1] {
                            Resp::BulkString(s) => assert_eq!(s, "GET"),
                            _ => return Err("Expected bulk string"),
                        }
                        match &arr[2] {
                            Resp::BulkString(s) => assert_eq!(s, "appendonly"),
                            _ => return Err("Expected bulk string"),
                        }
                    }
                    _ => return Err("Should be of type array"),
                }
                Ok(())
            }
            _ => Err("Should be of type array"),
        }
    }

    #[test]
    fn parse_simple_string() -> Result<(), &'static str> {
        let input = "+OK\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::SimpleString(s)), _) => {
                assert_eq!(s, "OK");
                Ok(())
            }
            _ => Err("Should be of type simple string"),
        }
    }

    #[test]
    fn parse_simple_error() -> Result<(), &'static str> {
        let input = "-ERROR message\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::SimpleError(s)), _) => {
                assert_eq!(s, "ERROR message");
                Ok(())
            }
            _ => Err("Should be of type simple error"),
        }
    }

    #[test]
    fn parse_empty_bulk_string() -> Result<(), &'static str> {
        let input = "$0\r\n\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::BulkString(s)), _) => {
                assert_eq!(s, "");
                Ok(())
            }
            _ => Err("Should be of type bulk string"),
        }
    }

    #[test]
    fn parse_simple_string2() -> Result<(), &'static str> {
        let input = "+hello world\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::SimpleString(s)), _) => {
                assert_eq!(s, "hello world");
                Ok(())
            }
            _ => Err("Should be of type simple string"),
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
    fn decoder_parse_null() -> Result<(), &'static str> {
        let input = "$-1\r\n".as_bytes();
        let mut parser = Decoder::new(input);
        match parser.next() {
            Some(Ok(Resp::Null)) => Ok(()),
            _ => Err("Should be null"),
        }
    }

    #[test]
    fn decoder_parse_null_split() -> Result<(), &'static str> {
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
            _ => Err("Should be null"),
        }
    }
}
