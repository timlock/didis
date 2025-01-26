use std::fmt::{self, Display};
use std::io;
use std::io::Read;
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::{error, str};
use crate::parser::ring_buffer::RingBuffer;

#[derive(Debug)]
pub enum Error {
    LengthMismatch,
    Parse(Box<dyn error::Error>),
    Truncated,
    NotEnoughBytes,
    Misc(String),
    Io(io::Error),
    UnknownResp(u8),
    NotAllowedToken(u8),
    MissingIdentifier,
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::LengthMismatch => write!(f, "Length does not match"),
            Error::Parse(err) => write!(f, "Parse error: {err}"),
            Error::Truncated => write!(f, "Resp is truncated"),
            Error::Misc(desc) => write!(f, "Unknown error: {desc}"),
            Error::NotEnoughBytes => write!(f, "Not enough bytes"),
            Error::Io(error) => write!(f, "IO error: {error}"),
            Error::UnknownResp(byte) => write!(f, "Unknown resp type: {}", &byte),
            Error::NotAllowedToken(byte) => write!(f, "Resp must not contain byte: {}", &byte),
            Error::MissingIdentifier => write!(f, "Resp must start with an identifier"),
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
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

impl<T> crate::parser::resp::RingDecoder<T>
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

impl<T> Iterator for crate::parser::resp::RingDecoder<T>
    where
        T: Read,
{
    type Item = Result<Resp, super::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(err) = self.buf.populate(&mut self.reader) {
            return Some(Err(super::Error::Io(err)));
        }

        let content = self.buf.peek();
        match parse_resp(content.as_slice()) {
            (Some(resp), r) => {
                self.buf.add_read_pos(content.len() - r.len());
                Some(Ok(resp))
            }
            (None, []) => None,
            (None, r) => Some(Err(super::Error::Parse(Box::new(Error::Misc(
                String::from_utf8_lossy(r).into_owned(),
            ))))),
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

// enum SimpleValidator {
//     SimpleString(SimpleStringValidator),
//     SimpleError(SimpleErrorValidator),
//     Integer(IntegerValidator),
// }

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

// fn chain_parsers(parsers: Vec<Box<dyn Parse>>) -> impl FnMut(&mut dyn io::Read)->Result<bool, Error>{
//     return move |reader|{

//     }
// }

// struct ChainedParser {
//     parsers: Vec<Box<dyn Parse>>,
// }
// impl ChainedParser
// {
//     fn new(parsers: Vec<Box<dyn Parse>>) -> Self {
//         Self { parsers }
//     }
// }

// impl Parse for ChainedParser {
//     fn parse(&mut self, reader: &mut dyn io::Read) -> Result<bool, Error> {
//         todo!()
//     }

//     fn consume(self) -> Result<Resp, Error> {
//         todo!()
//     }
// }

// fn test() {
//     let chainedParser =
//         ChainedParser::new(vec![Box::new(ArrayParser::default()), Box::new(BulkStringParser::default())]);
// }

// enum RespParser {
//     SimpleString(SimpleStringParser),
//     SimpleError(SimpleStringParser),
//     Integer(IntegerParser),
//     BulkString(BulkStringParser),
//     Array(Vec<RespParser>),
// }

// #[derive(Default)]
// struct ArrayParser {
//     buf: Vec<Resp>,
//     length_parser: Option<LenghtParser>,
//     length: usize,
//     current: Option<RespParser>,
// }

// impl Parse for ArrayParser {
//     fn parse(&mut self, reader: &mut dyn io::Read) -> Result<bool, Error> {
//         if let Some(mut length_parser) = self.length_parser.take() {
//             let done = length_parser.parse(reader)?;
//             match done {
//                 true => self.length = length_parser.consume()?,
//                 false => {
//                     self.length_parser = Some(length_parser);
//                     return Ok(false);
//                 }
//             };
//         }

//         Ok(false)
//     }

//     fn consume(self) -> Result<Resp, Error> {
//         todo!()
//     }
// }

// trait Parse {
//     fn parse(&mut self, reader: &mut dyn io::Read) -> Result<bool, Error>;
//     fn consume(self) -> Result<Resp, Error>;
// }
// #[derive(Default)]
// struct BulkStringParser {
//     buf: Vec<u8>,
//     index: usize,
//     length_parser: Option<LenghtParser>,
// }

// impl Parse for BulkStringParser {
//     fn parse(&mut self, reader: &mut dyn io::Read) -> Result<bool, Error> {
//         if let Some(mut length_parser) = self.length_parser.take() {
//             let done = length_parser.parse(reader)?;
//             match done {
//                 true => {
//                     let length = length_parser.consume()?;
//                     self.buf.resize(length, 0u8);
//                 }
//                 false => {
//                     self.length_parser = Some(length_parser);
//                     return Ok(false);
//                 }
//             }
//         }

//         match reader.read(&mut self.buf[self.index..]) {
//             Ok(read_bytes) => self.index += read_bytes,
//             Err(err) => return Err(Error::Io(err)),
//         }

//         if self.index != self.buf.len() {
//             return Ok(false);
//         }

//         if &self.buf[self.buf.len() - 2..] != b"\r\n" {
//             Err(Error::LengthMismatch)
//         } else {
//             Ok(true)
//         }
//     }

//     fn consume(self) -> Result<Resp, Error> {
//         if self.index != self.buf.len() {
//             return Err(Error::LengthMismatch);
//         }

//         Ok(Resp::BulkString(self.buf))
//     }
// }

// enum ParserResult<T>{
//     Done((T, u8)),
//     Incomplete
// }

fn read_line(reader: &mut impl io::Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut cursor = 0;

    while let Some(token) = reader.bytes().next() {
        let token = token?;

        buf[cursor] = token;
        cursor += 1;

        if buf[cursor - 1] == b'\r' && token == b'\n' {
            break;
        }

        if buf.len() == cursor {
            break;
        }
    }

    Ok(cursor)
}

enum ParserResult<T, R> {
    Ok((T, R)),
    Incomplete,
}

trait Parse<R, A> {
    fn new(arg: A) -> Result<Self, Error>
        where
            Self: std::marker::Sized;
    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<R>, Error> {
        while let Some(token) = reader.bytes().next() {
            let token = token?;
            if let Some(result) = self.parse_token(token)? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }
    fn parse_token(&mut self, token: u8) -> Result<Option<R>, Error>;
}

struct DigitParser {
    buf: [u8; 8],
    cursor: usize,
}

impl DigitParser {
    fn convert(&self) -> Result<usize, Error> {
        let utf8 = std::str::from_utf8(&self.buf[..self.cursor])
            .map_err(|err| Error::Misc(err.to_string()))?;
        let length = utf8
            .parse()
            .map_err(|err: ParseIntError| Error::Misc(err.to_string()))?;
        Ok(length)
    }
}

impl Parse<(usize, u8), ()> for DigitParser {
    fn new(_: ()) -> Result<Self, Error> {
        Ok(Self {
            buf: Default::default(),
            cursor: Default::default(),
        })
    }

    fn parse_token(&mut self, token: u8) -> Result<Option<(usize, u8)>, Error> {
        if self.cursor == 8 || !token.is_ascii_digit() {
            let digit = self.convert()?;
            return Ok(Some((digit, token)));
        }
        self.buf[self.cursor] = token;
        self.cursor += 1;
        Ok(None)
    }
}

struct CRLfParser<T> {
    cr: bool,
    inner: Option<T>,
}

impl<T> CRLfParser<T> {
    fn new(inner: T) -> Self {
        Self {
            cr: false,
            inner: Some(inner),
        }
    }
    fn inner_parse(&mut self, token: u8) -> Result<Option<T>, Error> {
        match token {
            b'\r' => match self.cr {
                true => Err(Error::NotAllowedToken(token)),
                false => {
                    self.cr = true;
                    Ok(None)
                }
            },
            b'\n' => match self.cr {
                true => Ok(Some(self.inner.take().expect("inner should be present"))),
                false => Err(Error::NotAllowedToken(token)),
            },
            _ => Err(Error::NotAllowedToken(token)),
        }
    }
}

impl<T> Parse<T, (T, u8)> for CRLfParser<T> {
    fn new(arg: (T, u8)) -> Result<Self, Error> {
        let mut s = Self {
            cr: false,
            inner: Some(arg.0),
        };
        let result = s.inner_parse(arg.1)?;
        if result.is_some() {
            panic!("CRLF parser should not be complete only one token has been parsed")
        }

        Ok(s)
    }

    fn parse_token(&mut self, token: u8) -> Result<Option<T>, Error> {
        self.inner_parse(token)
    }
}

impl<T> Parse<T, T> for CRLfParser<T> {
    fn new(arg: T) -> Result<Self, Error> {
        Ok(Self {
            cr: false,
            inner: Some(arg),
        })
    }

    fn parse_token(&mut self, token: u8) -> Result<Option<T>, Error> {
        self.inner_parse(token)
    }
}

struct ChainedParser<P1, R1, A1, P2, R2> {
    first: Option<P1>,
    second: Option<P2>,

    first_result_type: PhantomData<R1>,
    first_arg_type: PhantomData<A1>,
    second_result_type: PhantomData<R2>,
}

impl<T1, R1, A1, T2, R2> Parse<R2, A1> for ChainedParser<T1, R1, A1, T2, R2>
    where
        T1: Parse<R1, A1>,
        T2: Parse<R2, R1>,
{
    fn new(arg: A1) -> Result<Self, Error> {
        let first = T1::new(arg)?;
        Ok(Self {
            first: Some(first),
            second: None,
            first_result_type: PhantomData,
            first_arg_type: PhantomData,
            second_result_type: PhantomData,
        })
    }

    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<R2>, Error> {
        if let Some(ref mut first) = self.first {
            if let Some(result) = first.parse(reader)? {
                let second = T2::new(result)?;
                self.second = Some(second);
                self.first = None;
            }
        }

        if let Some(ref mut second) = self.second {
            if let Some(result) = second.parse(reader)? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }
    fn parse_token(&mut self, token: u8) -> Result<Option<R2>, Error> {
        if let Some(ref mut first) = self.first {
            if let Some(result) = first.parse_token(token)? {
                let second = T2::new(result)?;
                self.second = Some(second);
                self.first = None;
            }
            return Ok(None);
        }

        if let Some(ref mut second) = self.second {
            if let Some(result) = second.parse_token(token)? {
                return Ok(Some(result));
            }
        }

        Ok(None)
    }
}

type LengthPars = ChainedParser<DigitParser, (usize, u8), (), CRLfParser<usize>, usize>;
// type BytePars = ChainedParser<ByteParser, Vec<u8>, usize, CRLfParser<Vec<u8>>, Vec<u8>>;
// struct BulkStringPars {
//     inner: ChainedParser<LengthPars, usize, (), BytePars, Vec<u8>>,
// }
// impl Parse<Resp, ()> for BulkStringPars {
//     fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Resp>, Error> {
//         match self.inner.parse(reader)? {
//             Some(bytes) => Ok(Some(Resp::BulkString(bytes))),
//             None => Ok(None),
//         }
//     }

//     fn new(_: ()) -> Result<Self, Error> {
//         let parser = ChainedParser::new(())?;
//         Ok(Self { inner: parser })
//     }

//     fn parse_token(&mut self, token: u8) -> Result<Option<Resp>, Error> {
//         match self.inner.parse_token(token)? {
//             Some(bytes) => Ok(Some(Resp::BulkString(bytes))),
//             None => Ok(None),
//         }
//     }
// }

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

    (Some(Resp::SimpleString(data.into())), &remaining[2..])
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
    let text = data.to_vec();
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

#[cfg(test)]
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
                    Resp::BulkString(s) => assert_eq!(s, b"ping"),
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
                    Resp::BulkString(s) => assert_eq!(s, b"echo"),
                    _ => return Err("Array should contain a simple string"),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, b"hello world"),
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
                    Resp::BulkString(s) => assert_eq!(s, b"get"),
                    _ => return Err("Array should contain a bulk string"),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, b"key"),
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
                    Resp::BulkString(s) => assert_eq!(s, b"CONFIG"),
                    _ => return Err("Expected bulk string"),
                }
                match &arr[1] {
                    Resp::BulkString(s) => assert_eq!(s, b"GET"),
                    _ => return Err("Expected bulk string"),
                }
                match &arr[2] {
                    Resp::BulkString(s) => assert_eq!(s, b"save"),
                    _ => return Err("Expected bulk string"),
                }
                assert!(!r.is_empty());
                match parse_resp(r) {
                    (Some(Resp::Array(arr)), r) => {
                        assert_eq!(arr.len(), 3);
                        assert!(r.is_empty());
                        match &arr[0] {
                            Resp::BulkString(s) => assert_eq!(s, b"CONFIG"),
                            _ => return Err("Expected bulk string"),
                        }
                        match &arr[1] {
                            Resp::BulkString(s) => assert_eq!(s, b"GET"),
                            _ => return Err("Expected bulk string"),
                        }
                        match &arr[2] {
                            Resp::BulkString(s) => assert_eq!(s, b"appendonly"),
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
                assert_eq!(s, b"OK");
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
                assert_eq!(s, b"ERROR message");
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
                assert_eq!(s, b"");
                Ok(())
            }
            _ => Err("Should be of type bulk string"),
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
    fn parse_simple_string2() -> Result<(), &'static str> {
        let input = "+hello world\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::SimpleString(s)), _) => {
                assert_eq!(s, b"hello world");
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
