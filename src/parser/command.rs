use super::resp;
use crate::parser::resp::{parse, ParsedValue, Reference, ValOrRef, Value};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt::{self, Debug, Display, Formatter};
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::time::{Duration, SystemTime};
use std::{error, io};

#[derive(Debug)]
pub enum Error {
    UnknownCommand(String),
    UnknownArguments(Vec<Value>),
    Resp(resp::Error),
    Io(io::Error),
    ExpectedArray(Value),
    UnexpectedEnd,
    UnexpectedResp(Value),
    ParseInt(ParseIntError),
    FromUtf8(FromUtf8Error),
    Utf8Error(Utf8Error),
}

impl error::Error for Error {}
impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownCommand(name) => write!(f, "Command {name} is unknown"),
            Error::Resp(err) => write!(f,"{}", err),
            Error::Io(err) => write!(f,"{}", err),
            Error::FromUtf8(err) => write!(f,"{}", err),
            Error::Utf8Error(err) => write!(f,"{}", err),
            Error::UnknownArguments(arguments) => {write!(f,"Command contains unknown arguments {:?}", arguments)}
            Error::ExpectedArray(other) => {write!(f, "Command should be of the RESP type array but is {}", other)}
            Error::UnexpectedEnd => {write!(f, "Command ended abruptly")}
            Error::UnexpectedResp(other) => {write!(f, "Command contains unexpected RESP value {}", other)}
            Error::ParseInt(err) => write!(f,"{}", err),
        }
    }
}
impl From<resp::Error> for Error {
    fn from(value: resp::Error) -> Self {
        match value {
            resp::Error::Io(err) => Error::Io(err),
            _ => Error::Resp(value),
        }
    }
}

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::ParseInt(value)
    }
}
impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::FromUtf8(value)
    }
}
impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::Utf8Error(value)
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Command<'a> {
    Ping(Option<Cow<'a, str>>),
    Echo(Cow<'a, str>),
    Get(Cow<'a, str>),
    Set {
        key: Cow<'a, str>,
        value: Cow<'a, str>,
        overwrite_rule: Option<OverwriteRule>,
        get: bool,
        expire_rule: Option<ExpireRule>,
    },
    ConfigGet(Vec<Cow<'a, str>>),
    Client,
    Exists(Vec<Cow<'a, str>>),
}

impl<'a> Command<'a> {
    pub fn to_resp(self) -> Value {
        match self {
            Command::Ping(message) => {
                let mut command_parts = vec![Value::BulkString(String::from("PING"))];
                if let Some(message) = message {
                    command_parts.push(Value::BulkString(message.into_owned()));
                }
                Value::Array(command_parts)
            }
            Command::Echo(message) => Value::Array(vec![
                Value::BulkString(String::from("ECHO")),
                Value::BulkString(message.into_owned()),
            ]),
            Command::Get(key) => Value::Array(vec![
                Value::BulkString(String::from("GET")),
                Value::BulkString(key.into_owned()),
            ]),
            Command::Set {
                key,
                value,
                overwrite_rule,
                get,
                expire_rule,
            } => {
                let mut command_parts = vec![
                    Value::BulkString(String::from("SET")),
                    Value::BulkString(key.into_owned()),
                    Value::BulkString(value.into_owned()),
                ];
                if let Some(overwrite_rule) = overwrite_rule {
                    command_parts.push(Value::from(overwrite_rule));
                }
                if get {
                    command_parts.push(Value::BulkString(String::from("GET")))
                }
                if let Some(expire_rule) = expire_rule {
                    command_parts.push(Value::from(expire_rule));
                }

                Value::Array(command_parts)
            }
            Command::ConfigGet(config_params) => {
                let mut command_parts = vec![
                    Value::BulkString(String::from("CONFIG")),
                    Value::BulkString(String::from("GET")),
                ];
                for config_param in config_params {
                    command_parts.push(Value::BulkString(config_param.into_owned()));
                }
                Value::Array(command_parts)
            }
            Command::Client => Value::Array(vec![Value::BulkString(String::from("CLIENT"))]),
            Command::Exists(keys) => {
                let mut command_parts = vec![Value::BulkString(String::from("EXISTS"))];
                for key in keys {
                    command_parts.push(Value::BulkString(key.into_owned()));
                }

                Value::Array(command_parts)
            }
        }
    }
    pub fn to_bytes(self) -> Vec<u8> {
        Value::from(self).to_bytes()
    }
}

impl<'a> Display for Command<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Command::Ping(value) => match value {
                None => write!(f, "PING"),
                Some(value) => write!(f, "PING {}", value),
            },
            Command::Echo(value) => write!(f, "ECHO {}", value),
            Command::Get(key) => write!(f, "GET {}", key),
            Command::Set {
                key,
                value,
                overwrite_rule,
                get,
                expire_rule,
            } => {
                write!(f, "SET {}", key)?;
                if let Some(overwrite_rule) = overwrite_rule {
                    write!(f, " {:?}", overwrite_rule)?;
                }
                if value.len() < 32 {
                    write!(f, "{}", value)?;
                } else {
                    write!(f, "{}...(shortened)", &value[..32])?;
                }
                if *get {
                    write!(f, " GET")?;
                }
                if let Some(expire_rule) = expire_rule {
                    write!(f, " {:?}", expire_rule)?;
                }
                Ok(())
            }

            Command::ConfigGet(values) => {
                write!(f, "CONFIG GET {:?}", values)
            }
            Command::Client => {
                write!(f, "CLIENT")
            }
            Command::Exists(keys) => {
                write!(f, "KEYS {:?}", keys)
            }
        }
    }
}

impl<'a> From<Command<'a>> for Value {
    fn from(value: Command) -> Self {
        value.to_resp()
    }
}

impl<'a> From<Command<'a>> for Vec<u8> {
    fn from(value: Command) -> Self {
        value.to_bytes()
    }
}

impl<'a> TryFrom<ValOrRef<'a>> for Command<'a> {
    type Error = Error;

    fn try_from(value: ValOrRef<'a>) -> Result<Self, Self::Error> {
        let mut segment_iter: VecDeque<ValOrRef> = match value {
            ValOrRef::Val(Value::Array(values)) => {
                values.into_iter().map(|v| ValOrRef::Val(v)).collect()
            }
            ValOrRef::Ref(Reference::Array(references)) => {
                references.into_iter().map(|r| ValOrRef::Ref(r)).collect()
            }
            other => return Err(Error::ExpectedArray(other.to_value())),
        };

        let name = expect_string(&mut segment_iter)?;

        let command = match name.as_ref() {
            "PING" => parse_ping(&mut segment_iter)?,
            "ECHO" => parse_echo(&mut segment_iter)?,
            "GET" => parse_get(&mut segment_iter)?,
            "SET" => parse_set(&mut segment_iter)?,
            "CONFIG" => parse_config_get(&mut segment_iter)?,
            "CLIENT" => Command::Client,
            "EXISTS" => parse_exists(&mut segment_iter)?,
            _ => return Err(Error::UnknownCommand(name.into_owned())),
        };
        if !segment_iter.is_empty() {
            let remaining_args = segment_iter.into_iter().map(ValOrRef::to_value).collect();
            return Err(Error::UnknownArguments(remaining_args));
        }

        Ok(command)
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum ExpireRule {
    ExpiresInSecs(Duration),
    ExpiresInMillis(Duration),
    ExpiresAtSecs(Duration),
    ExpiresAtMillis(Duration),
    KeepTTL,
}
impl ExpireRule {
    pub fn calculate_expire_time(&self) -> Option<SystemTime> {
        match self {
            ExpireRule::ExpiresInSecs(s) => SystemTime::now().checked_add(*s),
            ExpireRule::ExpiresInMillis(ms) => SystemTime::now().checked_add(*ms),
            ExpireRule::ExpiresAtSecs(s) => Some(SystemTime::UNIX_EPOCH.checked_add(*s)?),
            ExpireRule::ExpiresAtMillis(ms) => Some(SystemTime::UNIX_EPOCH.checked_add(*ms)?),
            ExpireRule::KeepTTL => None,
        }
    }
}
impl TryFrom<&[u8]> for ExpireRule {
    type Error = Error;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.starts_with(b"EX") {
            let trimmed = &value[2..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed)?;
            let secs = ascii_number.parse()?;
            let dur = Duration::from_secs(secs);
            return Ok(ExpireRule::ExpiresInSecs(dur));
        }
        if value.starts_with(b"PX") {
            let trimmed = &value[2..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed)?;
            let millis = ascii_number.parse()?;
            let dur = Duration::from_millis(millis);
            return Ok(ExpireRule::ExpiresInMillis(dur));
        }
        if value.starts_with(b"EXAT") {
            let trimmed = &value[4..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed)?;
            let secs = ascii_number.parse()?;
            return Ok(ExpireRule::ExpiresAtSecs(Duration::from_secs(secs)));
        }
        if value.starts_with(b"PXAT") {
            let trimmed = &value[4..].trim_ascii_start();
            let ascii_number = std::str::from_utf8(trimmed)?;
            let millis = ascii_number.parse()?;
            return Ok(ExpireRule::ExpiresAtMillis(Duration::from_millis(millis)));
        }
        if value == b"KEEPTTL" {
            return Ok(ExpireRule::KeepTTL);
        }

        Err(Error::UnknownArguments(vec![Value::BulkString(String::from_utf8_lossy(value).into_owned())]))
    }
}

impl TryFrom<&str> for ExpireRule {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        ExpireRule::try_from(value.as_bytes())
    }
}

impl From<ExpireRule> for Value {
    fn from(value: ExpireRule) -> Self {
        match value {
            ExpireRule::ExpiresInSecs(duration) => {
                Value::BulkString(format!("EX {}", duration.as_secs()))
            }
            ExpireRule::ExpiresInMillis(duration) => {
                Value::BulkString(format!("PX {}", duration.as_millis()))
            }
            ExpireRule::ExpiresAtSecs(duration) => {
                Value::BulkString(format!("EXAT {}", duration.as_secs()))
            }
            ExpireRule::ExpiresAtMillis(duration) => {
                Value::BulkString(format!("PXAT {}", duration.as_millis()))
            }
            ExpireRule::KeepTTL => Value::BulkString(String::from("KEEPTTL")),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum OverwriteRule {
    NotExists,
    Exists,
}

impl TryFrom<&str> for OverwriteRule {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "NX" => Ok(OverwriteRule::NotExists),
            "XX" => Ok(OverwriteRule::Exists),
            other =>
                Err(Error::UnknownArguments(vec![Value::BulkString(other.to_string())]))

        }
    }
}

impl From<OverwriteRule> for Value {
    fn from(value: OverwriteRule) -> Self {
        match value {
            OverwriteRule::NotExists => Value::BulkString(String::from("NX")),
            OverwriteRule::Exists => Value::BulkString(String::from("XX")),
        }
    }
}

#[derive(Debug, Default)]
pub struct Parser {
    resp_parser: Option<resp::Parser>,
}

impl<'a> Parser {
    pub fn parse_all(&mut self, buf: &'a [u8]) -> Vec<Result<Command<'a>, Error>> {
        let mut buffer = buf;
        let mut commands = Vec::new();

        while !buffer.is_empty() {
            let parse_result: Result<Option<(ValOrRef, &[u8])>, Error> = match &mut self.resp_parser
            {
                Some(parser) => parser
                    .parse(buffer)
                    .map(|o| o.map(|(value, remaining)| (ValOrRef::Val(value), remaining)))
                    .map_err(|err| err.into()),
                None => match parse(buffer) {
                    Ok(ParsedValue::Complete(reference, remaining)) => {
                        Ok(Some((ValOrRef::Ref(reference), remaining)))
                    }
                    Ok(ParsedValue::Incomplete(parser)) => {
                        self.resp_parser = Some(parser);
                        Ok(None)
                    }
                    Err(err) => Err(err.into()),
                },
            };

            match parse_result {
                Ok(Some((valOrRef, remaining))) => {
                    let command = Command::try_from(valOrRef);
                    commands.push(command);
                    buffer = remaining;
                }
                Ok(None) => break,
                Err(err) => {
                    commands.push(Err(err.into()));
                }
            };
        }

        commands
    }
}

fn expect_string<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Cow<'a, str>, Error> {
    match try_string(queue)? {
        Some(string) => Ok(string),
        None => Err(Error::UnexpectedEnd),
    }
}

fn try_string<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Option<Cow<'a, str>>, Error> {
    match queue.pop_front() {
        Some(ValOrRef::Ref(Reference::BulkString(string))) => Ok(Some(Cow::Borrowed(string))),
        Some(ValOrRef::Val(Value::BulkString(string))) => Ok(Some(Cow::Owned(string))),
        Some(other) => Err(Error::UnexpectedResp(other.to_value())),
        None => Ok(None),
    }
}

fn parse_ping<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Command<'a>, Error> {
    match try_string(queue)? {
        Some(string) => Ok(Command::Ping(Some(string))),
        None => Ok(Command::Ping(None)),
    }
}

fn parse_echo<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Command<'a>, Error> {
    let string = expect_string(queue)?;
    Ok(Command::Echo(string))
}

fn parse_get<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Command<'a>, Error> {
    let string = expect_string(queue)?;
    Ok(Command::Get(string))
}

fn parse_set<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Command<'a>, Error> {
    let key = expect_string(queue)?;
    let value = expect_string(queue)?;

    let mut overwrite_rule = None;
    let mut get = false;
    let mut expire_rule = None;
    while let Some(string) = try_string(queue)? {
        if let Ok(r) = OverwriteRule::try_from(string.as_ref()) {
            overwrite_rule = Some(r);
        } else if string == "GET" {
            get = true;
        } else if let Ok(p) = ExpireRule::try_from(string.as_ref()) {
            expire_rule = Some(p);
        } else {
            return Err(Error::UnknownArguments(vec![Value::BulkString(string.into_owned())]));
        }
    }

    Ok(Command::Set {
        key,
        value,
        overwrite_rule,
        get,
        expire_rule,
    })
}

fn parse_config_get<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Command<'a>, Error> {
    let config_param = expect_string(queue)?;
    let mut config_params = vec![config_param];
    while let Some(config_param) = try_string(queue)? {
        config_params.push(config_param);
    }

    Ok(Command::ConfigGet(config_params))
}

fn parse_exists<'a>(queue: &mut VecDeque<ValOrRef<'a>>) -> Result<Command<'a>, Error> {
    let key = expect_string(queue)?;
    let mut keys = vec![key];
    while let Some(key) = try_string(queue)? {
        keys.push(key);
    }

    Ok(Command::Exists(keys))
}

mod tests {
    use super::*;

    #[test]
    fn parse_ping() -> Result<(), Box<dyn error::Error>> {
        let name = "PING".to_string();
        let resp = ValOrRef::Val(Value::Array(vec![Value::BulkString(name)]));
        let command = Command::try_from(resp)?;
        assert_eq!(Command::Ping(None), command);
        Ok(())
    }

    #[test]
    fn parse_echo() -> Result<(), Box<dyn error::Error>> {
        let name = "ECHO".to_string();
        let arg = "test".to_string();
        let resp = Value::Array(vec![
            Value::BulkString(name),
            Value::BulkString(arg.clone()),
        ]);
        let command = Command::try_from(ValOrRef::Val(resp))?;
        assert_eq!(Command::Echo(Cow::Owned(arg)), command);
        Ok(())
    }

    #[test]
    fn parse_set() -> Result<(), Box<dyn error::Error>> {
        let resp = Value::Array(vec![
            Value::BulkString("SET".to_string()),
            Value::BulkString("key".to_string()),
            Value::BulkString("value".to_string()),
            Value::BulkString("XX".to_string()),
            Value::BulkString("GET".to_string()),
            Value::BulkString("EX 10".to_string()),
        ]);
        let command = Command::try_from(ValOrRef::Val(resp))?;
        assert_eq!(
            Command::Set {
                key: Cow::Owned("key".to_string()),
                value: Cow::Owned("value".to_string()),
                overwrite_rule: Some(OverwriteRule::Exists),
                get: true,
                expire_rule: Some(ExpireRule::ExpiresInSecs(Duration::from_secs(10))),
            },
            command
        );
        Ok(())
    }

    #[test]
    fn parse_set_references() -> Result<(), Box<dyn error::Error>> {
        let resp = Reference::Array(vec![
            Reference::BulkString("SET"),
            Reference::BulkString("key"),
            Reference::BulkString("value"),
            Reference::BulkString("XX"),
            Reference::BulkString("GET"),
            Reference::BulkString("EX 10"),
        ]);
        let command = Command::try_from(ValOrRef::Ref(resp))?;
        assert_eq!(
            Command::Set {
                key: Cow::Borrowed("key"),
                value: Cow::Borrowed("value"),
                overwrite_rule: Some(OverwriteRule::Exists),
                get: true,
                expire_rule: Some(ExpireRule::ExpiresInSecs(Duration::from_secs(10))),
            },
            command
        );
        Ok(())
    }

    #[test]
    fn parse_set_no_options() -> Result<(), Box<dyn error::Error>> {
        let resp = Value::Array(vec![
            Value::BulkString("SET".to_string()),
            Value::BulkString("key".to_string()),
            Value::BulkString("value".to_string()),
        ]);
        let command = Command::try_from(ValOrRef::Val(resp))?;
        assert_eq!(
            Command::Set {
                key: Cow::Owned("key".to_string()),
                value: Cow::Owned("value".to_string()),
                overwrite_rule: None,
                get: false,
                expire_rule: None,
            },
            command
        );
        Ok(())
    }
}
