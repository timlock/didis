use crate::parser::ring_buffer::RingBuffer;
use std::borrow::{Borrow, Cow};
use std::cmp::min;
use std::fmt::{self, Display, Formatter};
use std::io::Read;
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::{error, str};
use std::{io, mem};

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
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::SimpleString(value) => write!(f, "+{}\\r\\n", value),
            Value::SimpleError(value) => write!(f, "-{}\\r\\n", value),
            Value::Integer(value) => write!(f, ":{}\\r\\n", value),
            Value::BulkString(value) => {
                if value.len() < 32 {
                    write!(f, "${}\\r\\n{}\\r\\n",value.len(), value)
                }else{
                    write!(f, "${}\\r\\n{}...(shortened)\\r\\n",value.len(), &value[..32])
                }
            },
            Value::Array(values) => {
                write!(f, "*{}\\r\\n", values.len())?;
                for value in values {
                    write!(f, "{}", value)?;
                }
                write!(f, "\\r\\n",)
            }
            Value::Null => write!(f, "_\\r\\n")
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
    type Item = Result<Value, Error>;

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
    type Item = Result<Value, Error>;

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

#[derive(Debug, PartialEq, Eq)]
pub enum RespRef<'a> {
    SimpleString(Cow<'a, str>),
    SimpleError(Cow<'a, str>),
    Integer(i64),
    BulkString(Cow<'a, str>),
    Array(Vec<RespRef<'a>>),
    Null,
}

impl<'a> RespRef<'a> {
    pub fn ok() -> RespRef<'a> {
        RespRef::SimpleString(Cow::Borrowed("OK"))
    }

    pub fn to_bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();
        const CRLF: &[u8; 2] = b"\r\n";
        match self {
            RespRef::SimpleString(s) => {
                bytes.push(b'+');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            RespRef::SimpleError(s) => {
                bytes.push(b'-');
                bytes.extend_from_slice(s.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            RespRef::Integer(i) => {
                bytes.push(b':');
                bytes.extend_from_slice(i.to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            RespRef::BulkString(b) => {
                bytes.push(b'$');
                bytes.extend_from_slice(b.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                bytes.extend_from_slice(b.as_bytes());
                bytes.extend_from_slice(CRLF);
            }
            RespRef::Array(resps) => {
                bytes.push(b'*');
                bytes.extend_from_slice(resps.len().to_string().as_bytes());
                bytes.extend_from_slice(CRLF);
                for resp in resps {
                    let serialized = resp.to_bytes();
                    bytes.extend_from_slice(&serialized);
                }
            }
            RespRef::Null => bytes.extend_from_slice(b"*-1\r\n"),
        }
        bytes
    }
}
impl<'a> Display for RespRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl<'a> From<&RespRef<'a>> for String {
    fn from(value: &RespRef<'a>) -> Self {
        let mut string = String::new();
        const CLRF: &str = "\r\n";
        match value {
            RespRef::SimpleString(s) => {
                string += "+";
                string += s;
                string += CLRF;
            }
            RespRef::SimpleError(e) => {
                string += "-";
                string += e;
                string += CLRF;
            }
            RespRef::Integer(i) => {
                string += ":";
                string += i.to_string().as_str();
                string += CLRF;
            }
            RespRef::BulkString(b) => {
                string += "$";
                string += b.len().to_string().as_str();
                string += CLRF;
                string += b;
                string += CLRF;
            }
            RespRef::Array(a) => {
                string += "*";
                string += a.len().to_string().as_str();
                string += CLRF;
                for i in a {
                    string += String::from(i).as_str();
                }
            }
            RespRef::Null => {
                string += "*";
                string += "-1";
                string += CLRF;
            }
        }
        string
    }
}
fn parse_line_ref(value: &[u8]) -> Result<(Option<&str>, &[u8]), Error> {
    match value.iter().position(|b| *b == b'\r') {
        Some(pos) => match value.get(pos) {
            Some(b'\r') => {
                let (line, remaining) = match value.split_at_checked(pos + 2) {
                    Some(split) => split,
                    None => (value, Default::default()),
                };
                let line_str = str::from_utf8(line)?;
                Ok((Some(line_str), remaining))
            }
            Some(other) => Err(Error::InvalidToken(*other)),
            None => Ok((None, value)),
        },
        None => Ok((None, value)),
    }
}

fn parse_length_ref(value: &[u8]) -> Result<(Option<(i64, &str)>, &[u8]), Error> {
    match parse_line_ref(value)? {
        (Some(line), remaining) => Ok((Some((*&line[..line.len() - 2].parse()?, line)), remaining)),
        (None, remaining) => Ok((None, remaining)),
    }
}

fn parse_simple_string_ref(value: &[u8]) -> Result<(Option<RespRef>, &[u8]), Error> {
    match parse_line_ref(value)? {
        (Some(line), remaining) => {
            let resp = RespRef::SimpleString(Cow::Borrowed(&line[..line.len() - 2]));
            Ok((Some(resp), remaining))
        }
        (None, remaining) => Ok((None, remaining)),
    }
}

fn parse_simple_error_ref(value: &[u8]) -> Result<(Option<RespRef>, &[u8]), Error> {
    match parse_line_ref(value)? {
        (Some(line), remaining) => Ok((
            Some(RespRef::SimpleError(Cow::Borrowed(&line[..line.len() - 2]))),
            remaining,
        )),
        (None, remaining) => Ok((None, remaining)),
    }
}

fn parse_integer_ref(value: &[u8]) -> Result<(Option<RespRef>, &[u8]), Error> {
    match parse_length_ref(value)? {
        (Some((integer, line)), remaining) => Ok((Some(RespRef::Integer(integer)), remaining)),
        (None, remaining) => Ok((None, remaining)),
    }
}

fn parse_bulk_string_ref(value: &[u8]) -> Result<(Option<RespRef>, &[u8]), Error> {
    let (length, parsed, remaining) = match parse_length_ref(value)? {
        (Some((length, line)), remaining) => (length, line, remaining),
        (None, remaining) => return Ok((None, remaining)),
    };

    if length == -1 {
        return Ok((Some(RespRef::Null), remaining));
    }

    if length + 2 > remaining.len() as i64 {
        return Ok((None, remaining));
    }

    let cr_pos = length as usize;

    let cr = remaining[cr_pos];
    if cr != b'\r' {
        return Err(Error::InvalidToken(cr));
    }
    let lf = remaining[cr_pos + 1];
    if lf != b'\n' {
        return Err(Error::InvalidToken(lf));
    }

    let (source, remaining) = remaining.split_at(cr_pos + 2);
    let source = str::from_utf8(source)?;
    let string = Cow::Borrowed(&source[..source.len() - 2]);

    Ok((Some(RespRef::BulkString(string)), remaining))
}

fn parse_array_ref(value: &[u8]) -> Result<(Option<RespRef>, &[u8]), Error> {
    let (length, parsed, mut remaining) = match parse_length_ref(value)? {
        (Some((length, line)), remaining) => (length, line, remaining),
        (None, remaining) => return Ok((None, remaining)),
    };

    if length == -1 {
        return Ok((Some(RespRef::Null), remaining));
    }

    let length = length as usize;
    let mut items = Vec::with_capacity(length);
    for _ in 0..length {
        match parse_borrowed_resp(remaining)? {
            (Some(resp), r) => {
                items.push(resp);
                remaining = r;
            }
            (None, r) => return Ok((None, r)),
        }
    }

    Ok((Some(RespRef::Array(items)), remaining))
}

fn parse_borrowed_resp(value: &[u8]) -> Result<(Option<RespRef>, &[u8]), Error> {
    if value.is_empty() {
        return Ok((None, value));
    }

    let type_prefix = value[0];
    let value = &value[1..];

    let result = match type_prefix {
        b'+' => parse_simple_string_ref(value)?,
        b'-' => parse_simple_error_ref(value)?,
        b':' => parse_integer_ref(value)?,
        b'$' => parse_bulk_string_ref(value)?,
        b'*' => parse_array_ref(value)?,
        _ => return Err(Error::UnknownResp(value[0])),
    };

    Ok(result)
}

#[derive(Debug, Default)]
struct LineParser {
    buffer: Vec<u8>,
    cr: bool,
}

impl LineParser {
    fn next(&mut self, value: &[u8]) -> Result<(Option<String>, usize), Error> {
        let mut bytes_read = 0;

        if !self.cr {
            match value.iter().position(|b| *b == b'\r') {
                Some(pos) => {
                    self.buffer.extend_from_slice(&value[..pos]);
                    bytes_read += pos + 1;
                    self.cr = true;
                }
                None => {
                    self.buffer.extend_from_slice(value);
                    return Ok((None, value.len()));
                }
            }
        }

        if self.cr {
            return match value.get(bytes_read) {
                Some(b'\n') => {
                    let string = String::from_utf8(mem::take(&mut self.buffer))?;
                    self.cr = false;
                    Ok((Some(string), bytes_read + 1))
                }
                Some(other) => Err(Error::InvalidToken(*other)),
                None => Ok((None, value.len())),
            };
        }

        Ok((None, value.len()))
    }
}

#[derive(Debug, Default)]
struct IntegerParser {
    line_parser: LineParser,
}

impl IntegerParser {
    fn next(&mut self, value: &[u8]) -> Result<(Option<i64>, usize), Error> {
        match self.line_parser.next(value)? {
            (None, n) => Ok((None, n)),
            (Some(string), n) => {
                let integer = string.parse()?;
                Ok((Some(integer), n))
            }
        }
    }
}

#[derive(Debug, Default)]
struct BulkStringParser {
    integer_parser: IntegerParser,
    buffer: Vec<u8>,
    size: Option<i64>,
}

impl BulkStringParser {
    fn next(&mut self, value: &[u8]) -> Result<(Option<Value>, usize), Error> {
        let mut read_bytes = 0;
        if self.size.is_none() {
            let (integer, n) = self.integer_parser.next(value)?;
            if let Some(integer) = integer {
                self.size = Some(integer as _);
            }
            read_bytes += n;
        }

        if let Some(size) = self.size {
            if size == -1 {
                return Ok((Some(Value::Null), read_bytes));
            }

            let size = size as _;

            if size > self.buffer.len() {
                let remaining = size - self.buffer.len();
                let end = min(value.len(), remaining + read_bytes);
                let copy_range = read_bytes..end;
                read_bytes += copy_range.len();
                self.buffer.extend_from_slice(&value[copy_range]);
            }

            if self.buffer.len() == size && value.len() > read_bytes + 1 {
                let cr = value[read_bytes];
                if cr != b'\r' {
                    return Err(Error::InvalidToken(cr));
                }
                let lf = value[read_bytes + 1];
                if lf != b'\n' {
                    return Err(Error::InvalidToken(lf));
                }
                read_bytes += 2;

                let string = String::from_utf8(mem::take(&mut self.buffer))?;
                self.size = None;

                return Ok((Some(Value::BulkString(string)), read_bytes));
            }
        }

        Ok((None, read_bytes))
    }
}

#[derive(Debug)]
struct ArrayParser {
    integer_parser: IntegerParser,
    item_parser: Box<Parser>,
    size: Option<i64>,
    items: Vec<Value>,
}

impl ArrayParser {
    fn next(&mut self, value: &[u8]) -> Result<(Option<Value>, usize), Error> {
        let mut read_bytes = 0;
        if self.size.is_none() {
            let (integer, n) = self.integer_parser.next(value)?;
            if let Some(integer) = integer {
                self.size = Some(integer as _);
            }
            read_bytes += n;
        }

        if let Some(size) = self.size {
            if size == -1 {
                return Ok((Some(Value::Null), read_bytes));
            }

            while self.items.len() < size as _ {
                match self.item_parser.parse(&value[read_bytes..])? {
                    (Some(resp), n) => {
                        self.items.push(resp);
                        read_bytes += n;
                    }
                    (None, n) => {
                        return Ok((None, read_bytes + n));
                    }
                };
            }

            return Ok((Some(Value::Array(mem::take(&mut self.items))), read_bytes));
        }

        Ok((None, read_bytes))
    }
}

impl Default for ArrayParser {
    fn default() -> Self {
        Self {
            integer_parser: Default::default(),
            item_parser: Box::new(Parser::default()),
            size: None,
            items: vec![],
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

impl Parser {
    pub fn parse(&mut self, value: &[u8]) -> Result<(Option<Value>, usize), Error> {
        let (resp, n) = match &mut self.state {
            ParserState::SimpleString(parser) => match parser.next(value)? {
                (Some(string), n) => (Some(Value::SimpleString(string)), n),
                (None, n) => (None, n),
            },
            ParserState::SimpleError(parser) => match parser.next(value)? {
                (Some(string), n) => (Some(Value::SimpleError(string)), n),
                (None, n) => (None, n),
            },
            ParserState::Integer(parser) => match parser.next(value)? {
                (Some(integer), n) => (Some(Value::Integer(integer)), n),
                (None, n) => (None, n),
            },
            ParserState::BulkString(parser) => match parser.next(value)? {
                (Some(resp), n) => (Some(resp), n),
                (None, n) => (None, n),
            },
            ParserState::Array(parser) => match parser.next(value)? {
                (Some(resp), n) => (Some(resp), n),
                (None, n) => (None, n),
            },
            ParserState::None => match value.get(0) {
                Some(identifier) => {
                    self.state = ParserState::try_from(*identifier)?;
                    let (resp, n) = self.parse(&value[1..])?;
                    (resp, n + 1)
                }
                None => (None, 0),
            },
        };

        if resp.is_some() {
            self.state = ParserState::None;
        }

        Ok((resp, n))
    }

    pub fn parse_all(&mut self, value: &[u8]) -> (Vec<Result<Value, Error>>, usize) {
        let mut read = 0;
        let mut parsed = vec![];
        while read < value.len() {
            match self.parse(&value[read..]) {
                Ok((resp, n)) => {
                    read += n;
                    match resp {
                        Some(resp) => parsed.push(Ok(resp)),
                        None => break,
                    }
                }
                Err(err) => {
                    parsed.push(Err(err));
                    break;
                }
            }
        }

        (parsed, read)
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self {
            state: ParserState::None,
        }
    }
}

pub fn parse_resp(value: &[u8]) -> Result<(Option<Value>, &[u8]), Error> {
    if value.is_empty() {
        return Ok((None, value));
    }
    let result = match value[0] {
        b'+' => parse_simple_string(value)?,
        b'-' => parse_simple_error(value)?,
        b':' => parse_integer(value)?,
        b'$' => parse_bulk_string(value)?,
        b'*' => parse_array(value)?,
        _ => return Err(Error::UnknownResp(value[0])),
    };

    Ok(result)
}

fn parse_simple_string(value: &[u8]) -> Result<(Option<Value>, &[u8]), Error> {
    let cr_pos = match value[1..].iter().position(|b| *b == b'\r') {
        Some(pos) => pos,
        None => return Ok((None, value)),
    };

    let (data, remaining) = value[1..].split_at(cr_pos);
    if remaining.len() < 2 || b"\r\n" != &remaining[..2] {
        return Ok((None, value));
    }

    let redis_str = String::from_utf8(data.into())?;

    Ok((Some(Value::SimpleString(redis_str)), &remaining[2..]))
}

fn parse_simple_error(value: &[u8]) -> Result<(Option<Value>, &[u8]), Error> {
    match parse_simple_string(value)? {
        (Some(Value::SimpleString(s)), r) => Ok((Some(Value::SimpleError(s)), r)),
        _ => Ok((None, value)),
    }
}

fn parse_integer(value: &[u8]) -> Result<(Option<Value>, &[u8]), Error> {
    match parse_length(&value[1..])? {
        (Some(number), r) => Ok((Some(Value::Integer(number)), r)),
        _ => Ok((None, value)),
    }
}

fn parse_array(value: &[u8]) -> Result<(Option<Value>, &[u8]), Error> {
    let (length, remaining) = parse_length(&value[1..])?;
    if length.is_none() {
        return Ok((None, value));
    }

    if length.unwrap() == -1 {
        return match parse_null(value)? {
            (Some(null), r) => Ok((Some(null), r)),
            _ => Ok((None, value)),
        };
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
                    return Ok((Some(Value::Array(array)), r));
                }
                return Ok((None, r));
            }
        }
    }
    Ok((Some(Value::Array(array)), contents))
}

fn parse_bulk_string(value: &[u8]) -> Result<(Option<Value>, &[u8]), Error> {
    let (length, remaining) = parse_length(&value[1..])?;
    if length.is_none() {
        return Ok((None, value));
    }
    if length.unwrap() == -1 {
        return match parse_null(value)? {
            (Some(null), r) => Ok((Some(null), r)),
            _ => Ok((None, value)),
        };
    }
    let length = length.unwrap() as usize;
    if length > remaining.len() {
        return Ok((None, value));
    }

    let (data, remaining) = remaining.split_at(length);
    if data.len() != length || remaining.len() < 2 || b"\r\n" != &remaining[..2] {
        return Ok((None, value));
    }
    let text = String::from_utf8(data.into())?;

    Ok((Some(Value::BulkString(text)), &remaining[2..]))
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

fn parse_null(value: &[u8]) -> Result<(Option<Value>, &[u8]), Error> {
    if value.len() < 5 {
        return Ok((None, value));
    }
    let null = &value[1..5];
    match null {
        b"-1\r\n" => Ok((Some(Value::Null), &value[5..])),
        _ => Err(Error::LengthMismatch),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::mpsc::{Receiver, Sender, channel};

    use super::*;

    #[test]
    fn parse_null() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Value::Null), r) => {
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
            (Some(Value::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 1);
                match &arr[0] {
                    Value::BulkString(s) => assert_eq!(s, "ping"),
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
            (Some(Value::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Value::BulkString(s) => assert_eq!(s, "echo"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                match &arr[1] {
                    Value::BulkString(s) => assert_eq!(s, "hello world"),
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
            (Some(Value::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Value::BulkString(s) => assert_eq!(s, "get"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                match &arr[1] {
                    Value::BulkString(s) => assert_eq!(s, "key"),
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
            (Some(Value::Array(arr)), r) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    Value::BulkString(s) => assert_eq!(s, "CONFIG"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &arr[1] {
                    Value::BulkString(s) => assert_eq!(s, "GET"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &arr[2] {
                    Value::BulkString(s) => assert_eq!(s, "save"),
                    _ => return Err("Expected bulk string".into()),
                }
                assert!(!r.is_empty());
                match parse_resp(r)? {
                    (Some(Value::Array(arr)), r) => {
                        assert_eq!(arr.len(), 3);
                        assert!(r.is_empty());
                        match &arr[0] {
                            Value::BulkString(s) => assert_eq!(s, "CONFIG"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &arr[1] {
                            Value::BulkString(s) => assert_eq!(s, "GET"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &arr[2] {
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
    fn parse_simple_string() -> Result<(), Box<dyn error::Error>> {
        let input = "+OK\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Value::SimpleString(s)), _) => {
                assert_eq!(s, "OK");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn parse_simple_error() -> Result<(), Box<dyn error::Error>> {
        let input = "-ERROR message\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Value::SimpleError(s)), _) => {
                assert_eq!(s, "ERROR message");
                Ok(())
            }
            _ => Err("Should be of type simple error".into()),
        }
    }

    #[test]
    fn parse_empty_bulk_string() -> Result<(), Box<dyn error::Error>> {
        let input = "$0\r\n\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Value::BulkString(s)), _) => {
                assert_eq!(s, "");
                Ok(())
            }
            _ => Err("Should be of type bulk string".into()),
        }
    }

    #[test]
    fn parse_simple_string2() -> Result<(), Box<dyn error::Error>> {
        let input = "+hello world\r\n";
        match parse_resp(input.as_bytes())? {
            (Some(Value::SimpleString(s)), _) => {
                assert_eq!(s, "hello world");
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
            Some(Ok(Value::Null)) => Ok(()),
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
            Some(Ok(Value::Null)) => Ok(()),
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn parse_null_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n";
        match Parser::default().parse(input.as_bytes())? {
            (Some(Value::Null), r) => {
                assert_eq!(input.len(), r);
                Ok(())
            }
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn parse_array_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "*1\r\n$4\r\nping\r\n";
        match Parser::default().parse(input.as_bytes())? {
            (Some(Value::Array(arr)), r) => {
                assert_eq!(input.len(), r);
                assert_eq!(arr.len(), 1);
                match &arr[0] {
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
            (Some(Value::Array(arr)), r) => {
                assert_eq!(input.len(), r);
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Value::BulkString(s) => assert_eq!(s, "echo"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                match &arr[1] {
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
            (Some(Value::Array(arr)), r) => {
                assert_eq!(input.len(), r);
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Value::BulkString(s) => assert_eq!(s, "get"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                match &arr[1] {
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
            (Some(Value::Array(arr)), r) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    Value::BulkString(s) => assert_eq!(s, "CONFIG"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &arr[1] {
                    Value::BulkString(s) => assert_eq!(s, "GET"),
                    _ => return Err("Expected bulk string".into()),
                }
                match &arr[2] {
                    Value::BulkString(s) => assert_eq!(s, "save"),
                    _ => return Err("Expected bulk string".into()),
                }
                assert_eq!(35, r);
                match parser.parse(&input[r..].as_bytes())? {
                    (Some(Value::Array(arr)), r) => {
                        assert_eq!(arr.len(), 3);
                        assert_eq!(42, r);
                        match &arr[0] {
                            Value::BulkString(s) => assert_eq!(s, "CONFIG"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &arr[1] {
                            Value::BulkString(s) => assert_eq!(s, "GET"),
                            _ => return Err("Expected bulk string".into()),
                        }
                        match &arr[2] {
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
            (Some(Value::SimpleString(s)), _) => {
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
            (Some(Value::SimpleError(s)), _) => {
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
            (Some(Value::BulkString(s)), _) => {
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
            (Some(Value::SimpleString(s)), _) => {
                assert_eq!(s, "hello world");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn decoder_parse_null_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n".as_bytes();
        let mut parser = Decoder::new(input);
        match parser.next() {
            Some(Ok(Value::Null)) => Ok(()),
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn decoder_parse_null_split_stateful() -> Result<(), Box<dyn error::Error>> {
        let input = [b'$', b'-', b'1', b'\r', b'\n'];
        let mut parser = Parser::default();

        let (resp, _) = parser.parse(&input[0..1])?;
        assert!(resp.is_none());
        let (resp, _) = parser.parse(&input[1..2])?;
        assert!(resp.is_none());
        let (resp, _) = parser.parse(&input[2..3])?;
        assert!(resp.is_none());
        let (resp, _) = parser.parse(&input[3..4])?;
        assert!(resp.is_none());
        let (resp, _) = parser.parse(&input[4..5])?;

        match resp {
            Some(Value::Null) => Ok(()),
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn parse_null_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "$-1\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::Null), r) => {
                assert!(r.is_empty());
                Ok(())
            }
            _ => Err("Should be null".into()),
        }
    }

    #[test]
    fn parse_array_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "*1\r\n$4\r\nping\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 1);
                match arr[0].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "ping"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array2_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match arr[0].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "echo"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                match arr[1].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "hello world"),
                    _ => return Err("Array should contain a simple string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array3_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "*2\r\n$3\r\nget\r\n$3\r\nkey\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::Array(arr)), r) => {
                assert!(r.is_empty());
                assert_eq!(arr.len(), 2);
                match arr[0].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "get"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                match arr[1].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "key"),
                    _ => return Err("Array should contain a bulk string".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_array4_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::Array(arr)), r) => {
                assert_eq!(arr.len(), 3);
                match arr[0].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "CONFIG"),
                    _ => return Err("Expected bulk string".into()),
                }
                match arr[1].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "GET"),
                    _ => return Err("Expected bulk string".into()),
                }
                match arr[2].borrow() {
                    RespRef::BulkString(s) => assert_eq!(s, "save"),
                    _ => return Err("Expected bulk string".into()),
                }
                assert!(!r.is_empty());
                match parse_borrowed_resp(r)? {
                    (Some(resp), r) => match resp {
                        RespRef::Array(arr) => {
                            assert_eq!(arr.len(), 3);
                            assert!(r.is_empty());
                            match arr[0].borrow() {
                                RespRef::BulkString(s) => assert_eq!(s, "CONFIG"),
                                _ => return Err("Expected bulk string".into()),
                            }
                            match arr[1].borrow() {
                                RespRef::BulkString(s) => assert_eq!(s, "GET"),
                                _ => return Err("Expected bulk string".into()),
                            }
                            match arr[2].borrow() {
                                RespRef::BulkString(s) => assert_eq!(s, "appendonly"),
                                _ => return Err("Expected bulk string".into()),
                            }
                        }
                        _ => return Err("Should be of type array".into()),
                    },
                    _ => return Err("Should be of type array".into()),
                }
                Ok(())
            }
            _ => Err("Should be of type array".into()),
        }
    }

    #[test]
    fn parse_simple_string_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "+OK\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::SimpleString(s)), _) => {
                assert_eq!(s, "OK");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }

    #[test]
    fn parse_simple_error_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "-ERROR message\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::SimpleError(s)), _) => {
                assert_eq!(s, "ERROR message");
                Ok(())
            }
            _ => Err("Should be of type simple error".into()),
        }
    }

    #[test]
    fn parse_empty_bulk_string_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "$0\r\n\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::BulkString(s)), _) => {
                assert_eq!(s, "");
                Ok(())
            }
            _ => Err("Should be of type bulk string".into()),
        }
    }

    #[test]
    fn parse_simple_string2_ref() -> Result<(), Box<dyn error::Error>> {
        let input = "+hello world\r\n";
        match parse_borrowed_resp(input.as_bytes())? {
            (Some(RespRef::SimpleString(s)), _) => {
                assert_eq!(s, "hello world");
                Ok(())
            }
            _ => Err("Should be of type simple string".into()),
        }
    }
}
