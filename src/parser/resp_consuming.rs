use std::io::{Read, Write};
use std::num::ParseIntError;
use std::sync::mpsc::{channel, Sender};
use std::{io, sync::mpsc::Receiver};

use super::resp::{self, Resp};
pub struct Decoder<T> {
    parser: Option<Parser>,
    reader: io::BufReader<T>,
}

impl<T> Decoder<T>
where
    T: io::Read,
{
    pub fn new(reader: T) -> Self {
        Self {
            parser: None,
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
        let parser = match &mut self.parser {
            Some(parser) => parser,
            None => match Parser::new(&mut self.reader) {
                Ok(Some(parser)) => {
                    self.parser = Some(parser);
                    self.parser.as_mut().unwrap()
                }
                Ok(None) => return None,
                Err(resp::Error::Io(err)) => return Some(Err(super::Error::Io(err))),
                Err(err) => return Some(Err(super::Error::Parse(Box::new(err)))),
            },
        };

        match parser.parse(&mut self.reader) {
            Ok(Some(resp)) => {
                self.parser = None;
                Some(Ok(resp))
            }
            Ok(None) => None,
            Err(err) => Some(Err(super::Error::Parse(Box::new(err)))),
        }
    }
}
struct CRLFParser {
    cr: bool,
    lf: bool,
}
impl CRLFParser {
    fn new() -> Self {
        Self {
            cr: false,
            lf: false,
        }
    }

    fn parse(&mut self, token: u8) -> Result<bool, resp::Error> {
        assert!(!self.cr || !self.lf);

        match token {
            b'\r' => {
                if self.cr {
                    return Err(resp::Error::UnallowedToken(token));
                }

                self.cr = true;
            }
            b'\n' => {
                if !self.cr {
                    return Err(resp::Error::UnallowedToken(token));
                }

                self.lf = true;
                return Ok(true);
            }
            _ => return Err(resp::Error::UnallowedToken(token)),
        }

        Ok(false)
    }
}

pub enum Parser {
    SimpleString(SimpleStringParser),
    SimpleError(SimpleErrorParser),
    Integer(IntegerParser),
    BulkString(BulkStringParser),
    Array(ArrayParser),
}

impl Parser {
    pub fn new(reader: &mut impl io::Read) -> Result<Option<Parser>, resp::Error> {
        let token = reader.bytes().next();
        if token.is_none() {
            return Ok(None);
        }

        let token = token.unwrap()?;
        let parser = match token {
            b'+' => Parser::SimpleString(SimpleStringParser::new()),
            b'-' => Parser::SimpleError(SimpleErrorParser::new()),
            b':' => Parser::Integer(IntegerParser::new()),
            b'$' => Parser::BulkString(BulkStringParser::new()),
            b'*' => Parser::Array(ArrayParser::new()),
            _ => return Err(resp::Error::UnkownResp(token)),
        };

        Ok(Some(parser))
    }

    pub fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Resp>, resp::Error> {
        match self {
            Parser::SimpleString(simple_string_parser) => simple_string_parser.parse(reader),
            Parser::SimpleError(simple_error_parser) => simple_error_parser.parse(reader),
            Parser::Integer(integer_parser) => integer_parser.parse(reader),
            Parser::BulkString(bulk_string_parser) => bulk_string_parser.parse(reader),
            Parser::Array(array_parser) => array_parser.parse(reader),
        }
    }
}

struct ArrayParser {
    length_parser: LenghtParser,
    parser: Option<Box<Parser>>,
    resps: Option<Vec<Resp>>,
}

impl ArrayParser {
    fn new() -> Self {
        Self {
            length_parser: LenghtParser::new(),
            parser: None,
            resps: None,
        }
    }

    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Resp>, resp::Error> {
        if self.resps.is_none() {
            match self.length_parser.read(reader)? {
                Some(number) => self.resps = Some(Vec::with_capacity(number)),
                None => return Ok(None),
            }
        }

        assert!(self.resps.as_ref().unwrap().len() < self.resps.as_ref().unwrap().capacity());

        if self.parser.is_none() {
            match Parser::new(reader)? {
                Some(parser) => self.parser = Some(Box::new(parser)),
                None => return Ok(None),
            }
        }

        assert!(self.parser.is_some());

        while let Some(resp) = self.parser.as_mut().unwrap().as_mut().parse(reader)? {
            self.resps.as_mut().unwrap().push(resp);

            let resps = self.resps.as_ref().unwrap();
            if resps.len() == resps.capacity() {
                let resps = self.resps.take().expect("resps should not be None");
                return Ok(Some(Resp::Array(resps)));
            }

            match Parser::new(reader)? {
                Some(parser) => self.parser = Some(Box::new(parser)),
                None => self.parser = None,
            }
        }

        Ok(None)
    }
}
struct BulkStringParser {
    length_parser: LenghtParser,
    buf: Option<Vec<u8>>,
    cursor: usize,
}

impl BulkStringParser {
    fn new() -> Self {
        Self {
            length_parser: LenghtParser::new(),
            buf: None,
            cursor: 0,
        }
    }

    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Resp>, resp::Error> {
        if self.buf.is_none() {
            if let Some(number) = self.length_parser.read(reader)? {
                self.buf = Some(vec![0; number + 2])
            }
        }

        if let Some(buf) = &mut self.buf {
            let n = reader.read(&mut buf[self.cursor..])?;
            self.cursor += n;

            if self.cursor >= buf.len() {
                let mut buf = self.buf.take().expect("buf should not be None");
                if buf.pop().unwrap() != b'\n' || buf.pop().unwrap() != b'\r' {
                    return Err(resp::Error::LengthMismatch);
                }
                return Ok(Some(Resp::BulkString(buf)));
            }
        }

        Ok(None)
    }
}

struct IntegerParser {
    plus: Option<bool>,
    length_parser: LenghtParser,
}
impl IntegerParser {
    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Resp>, resp::Error> {
        if self.plus.is_none() {
            match reader.bytes().next() {
                Some(byte) => {
                    let byte = byte?;
                    match byte {
                        b'+' => self.plus = Some(true),
                        b'-' => self.plus = Some(false),
                        _ => {
                            self.plus = Some(true);
                            self.length_parser.parse(byte)?;
                        }
                    }
                }
                None => return Ok(None),
            }
        }
        if let Some(number) = self.length_parser.read(reader)? {
            let plus = self.plus.as_ref().expect("prefix should be some");
            let number = if !plus {
                -(number as i64)
            } else {
                number as i64
            };
            return Ok(Some(Resp::Integer(number)));
        }

        Ok(None)
    }

    fn new() -> Self {
        Self {
            plus: None,
            length_parser: LenghtParser::new(),
        }
    }
}

struct LenghtParser {
    cr: bool,
    buf: Option<[u8; 8]>,
    index: usize,
}

impl LenghtParser {
    fn parse(&mut self, token: u8) -> Result<Option<usize>, resp::Error> {
        match token {
            b'\r' => match self.cr {
                false => self.cr = true,
                true => return Err(resp::Error::UnallowedToken(token)),
            },
            b'\n' => match self.cr {
                true => {
                    let buf = self.buf.as_ref().expect("buffer should not be empty");

                    let number = convert(&buf[..self.index])?;
                    return Ok(Some(number));
                }
                false => return Err(resp::Error::UnallowedToken(token)),
            },
            b'0'..=b'9' => match self.cr {
                false => {
                    if self.index == 8 {
                        return Err(resp::Error::LengthMismatch);
                    }

                    let buf = self
                        .buf
                        .as_mut()
                        .expect("buffer should not be empty in init state");
                    buf[self.index] = token;
                    self.index += 1;
                }
                true => return Err(resp::Error::UnallowedToken(token)),
            },
            _ => return Err(resp::Error::UnallowedToken(token)),
        }
        Ok(None)
    }

    fn read(&mut self, reader: &mut impl io::Read) -> Result<Option<usize>, resp::Error> {
        for byte in reader.bytes() {
            let token = match byte {
                Ok(byte) => byte,
                Err(err) => return Err(resp::Error::Io(err)),
            };

            if let Some(s) = self.parse(token)? {
                return Ok(Some(s));
            }
        }

        Ok(None)
    }

    fn new() -> Self {
        Self {
            cr: false,
            buf: Some([0; 8]),
            index: 0,
        }
    }
}

struct SimpleStringParser {
    cr: bool,
    buf: Option<Vec<u8>>,
}

impl SimpleStringParser {
    fn new() -> Self {
        Self {
            cr: false,
            buf: Some(Vec::new()),
        }
    }

    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Resp>, resp::Error> {
        assert!(self.buf.is_some());

        for byte in reader.bytes() {
            let token = match byte {
                Ok(byte) => byte,
                Err(err) => return Err(resp::Error::Io(err)),
            };

            match token {
                b'\r' => match self.cr {
                    false => self.cr = true,
                    true => return Err(resp::Error::UnallowedToken(token)),
                },
                b'\n' => match self.cr {
                    true => {
                        return Ok(Some(Resp::SimpleString(
                            self.buf.take().expect("buf should not be None"),
                        )))
                    }
                    false => return Err(resp::Error::UnallowedToken(token)),
                },
                _ => match self.cr {
                    false => self
                        .buf
                        .as_mut()
                        .expect("buffer should not be empty in init state")
                        .push(token),
                    true => return Err(resp::Error::UnallowedToken(token)),
                },
            }
        }

        Ok(None)
    }
}

struct SimpleErrorParser {
    simple_string_parser: SimpleStringParser,
}

impl SimpleErrorParser {
    fn new() -> Self {
        Self {
            simple_string_parser: SimpleStringParser::new(),
        }
    }

    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Resp>, resp::Error> {
        match self.simple_string_parser.parse(reader)? {
            Some(Resp::SimpleString(s)) => Ok(Some(Resp::SimpleError(s))),
            Some(resp) => panic!("Unexpected resp {resp}"),
            None => Ok(None),
        }
    }
}

fn convert(buf: &[u8]) -> Result<usize, resp::Error> {
    let utf8 = std::str::from_utf8(buf).map_err(|err| resp::Error::Misc(err.to_string()))?;
    let length = utf8
        .parse()
        .map_err(|err: ParseIntError| resp::Error::Misc(err.to_string()))?;
    Ok(length)
}

fn parse_resp(value: &[u8]) -> (Option<Resp>, &[u8]) {
    if value.is_empty() {
        return (None, value);
    }

    let mut bytes = &value[1..];

    let result = match value[0] {
        b'+' => SimpleStringParser::new().parse(&mut bytes),
        b'-' => SimpleErrorParser::new().parse(&mut bytes),
        b':' => IntegerParser::new().parse(&mut bytes),
        b'$' => BulkStringParser::new().parse(&mut bytes),
        b'*' => ArrayParser::new().parse(&mut bytes),
        _ => Err(resp::Error::UnkownResp(value[0])),
    };

    match result {
        Ok(result) => (result, Default::default()),
        Err(err) => (None, Default::default()),
    }
}

mod tests {
    use std::fmt;
    use std::io::Write;
    use std::sync::mpsc::{channel, Receiver, Sender};

    use fmt::format;

    use super::*;

    #[test]
    fn test_convert() -> Result<(), String> {
        let bytes = b"1";
        let result = convert(bytes).map_err(|err| err.to_string())?;
        assert_eq!(1, result);
        Ok(())
    }

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
        let input = "*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$4\r\nsave\r\n*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nappendonly\r\n".as_bytes();
        match parse_resp(input) {
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
                assert!(!input.is_empty());
                match parse_resp(input) {
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

    #[test]
    fn parse_bulk_string() -> Result<(), &'static str> {
        let input = "$2\r\nab\r\n";
        match parse_resp(input.as_bytes()) {
            (Some(Resp::BulkString(s)), _) => {
                assert_eq!(s, b"ab");
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

    // #[test]
    // fn decoder_parse_null() -> Result<(), &'static str> {
    //     let input = "$-1\r\n".as_bytes();
    //     let mut parser = Decoder::new(input);
    //     match parser.next() {
    //         Some(Ok(Resp::Null)) => Ok(()),
    //         _ => Err("Should be null"),
    //     }
    // }

    // #[test]
    // fn decoder_parse_null_split() -> Result<(), &'static str> {
    //     let input = [b'$', b'-', b'1', b'\r', b'\n'];
    //     let (stream, sender) = MockStream::new();
    //     let mut parser = Decoder::new(stream);

    //     assert!(parser.next().is_none());
    //     sender.send(vec![input[0]]).map_err(|_| "Send error")?;
    //     assert!(parser.next().is_none());
    //     sender.send(vec![input[1]]).map_err(|_| "Send error")?;
    //     assert!(parser.next().is_none());
    //     sender.send(vec![input[2]]).map_err(|_| "Send error")?;
    //     assert!(parser.next().is_none());
    //     sender.send(vec![input[3]]).map_err(|_| "Send error")?;
    //     assert!(parser.next().is_none());

    //     sender.send(vec![input[4]]).map_err(|_| "Send error")?;
    //     match parser.next() {
    //         Some(Ok(Resp::Null)) => Ok(()),
    //         _ => Err("Should be null"),
    //     }
    // }
}
