use crate::dictionary::Timestamp;
use lzf::LzfError;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::io;
use std::iter::Peekable;
use std::num::ParseIntError;
use std::string::FromUtf8Error;

const MAGIC_NUMBER: &'static [u8] = b"REDIS";

const EOF: u8 = 0xFF;
const SELECTDB: u8 = 0xFE;
const EXPIRETIME: u8 = 0xFD;
const EXPIRETIMEMS: u8 = 0xFC;
const RESIZEDB: u8 = 0xFB;
const AUX: u8 = 0xFA;

#[derive(Debug, PartialEq)]
pub struct RDB {
    version: u32,
    auxiliary_fields: HashMap<String, String>,
   pub db_hash_maps: HashMap<u8, HashMap<String, Value>>,
}

impl RDB {
    pub fn new(
        auxiliary_fields: HashMap<String, String>,
        db_hash_maps: HashMap<u8, HashMap<String, Value>>,
    ) -> RDB {
        RDB {
            version: 7,
            auxiliary_fields,
            db_hash_maps,
        }
    }
}

impl TryFrom<&RDB> for Vec<u8> {
    type Error = Error;

    fn try_from(rdb: &RDB) -> Result<Self, Self::Error> {
        let mut bytes = Vec::new();

        bytes.extend_from_slice(MAGIC_NUMBER);

        let mut version_string = rdb.version.to_string();
        while version_string.len() < 4 {
            version_string.insert(0, '0');
        }
        bytes.extend_from_slice(version_string.as_bytes());

        for (key, value) in &rdb.auxiliary_fields {
            bytes.push(AUX);
            bytes.extend_from_slice(encode_string(key)?.as_slice());
            bytes.extend_from_slice(encode_string(value)?.as_slice());
        }

        for (db, hash_map) in &rdb.db_hash_maps {
            bytes.push(SELECTDB);
            bytes.push(*db);
            bytes.push(RESIZEDB);
            bytes.extend_from_slice(Size::Length(hash_map.len()).encode().as_slice());
            let keys_with_expires = hash_map
                .values()
                .filter(|value| value.expires_at.is_some())
                .count();
            bytes.extend_from_slice(Size::Length(keys_with_expires).encode().as_slice());

            for (key, value) in hash_map {
                match value.expires_at {
                    Some(Timestamp::Milliseconds(ms)) => {
                        bytes.push(EXPIRETIMEMS);
                        bytes.extend_from_slice(ms.to_be_bytes().as_slice());
                    }
                    Some(Timestamp::Seconds(s)) => {
                        bytes.push(EXPIRETIME);
                        bytes.extend_from_slice(s.to_be_bytes().as_slice());
                    }
                    None => {}
                }

                let value_type = ValueTypeIdentifier::from(&value.value_type);
                bytes.push(value_type.into());
                bytes.extend_from_slice(encode_string(key)?.as_slice());
                bytes.extend_from_slice(encode_value(value)?.as_slice());
            }
        }

        bytes.push(EOF);
        let checksum = crc64::crc64(0, bytes.as_slice());
        bytes.extend_from_slice(checksum.to_le_bytes().as_slice());

        Ok(bytes)
    }
}

impl TryFrom<Vec<u8>> for RDB {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        validate_checksum(&bytes)?;
        println!("validated checksum");

        let mut iter = bytes.into_iter().peekable();

        let magic_number: Vec<u8> = iter.by_ref().take(5).collect();
        if MAGIC_NUMBER != magic_number.as_slice() {
            return Err(Error::WrongMagicNumber(magic_number));
        }
        println!("validated magic number");

        let version_raw: Vec<u8> = iter.by_ref().take(4).collect();
        let version_string = String::from_utf8(version_raw)?;
        let version = version_string.parse()?;
        println!("rdb version is {}", version_string);

        let auxiliary_fields = parse_auxiliary_fields(&mut iter)?;
        println!("auxiliary fields are {:?}", auxiliary_fields);

        let mut db_hash_maps = HashMap::new();
        while &SELECTDB == iter.peek().ok_or(Error::Truncated)? {
            iter.next()
                .expect("when peek() returns Some(..) next() should also return Some(..)");

            let db_number = iter.next().ok_or(Error::Truncated)?;
            println!("parsed database number {}", db_number);

            let hash_map = match iter.peek() {
                Some(&RESIZEDB) => {
                    iter.next()
                        .expect("when peek() returns Some(..) next() should also return Some(..)");

                    let hash_table_size = decode_length(&mut iter)?;
                    let expire_hash_table_size = decode_length(&mut iter)?;
                    println!(
                        "database {} has in total keys {} of which {} have an expire timestamp",
                        db_number, hash_table_size, expire_hash_table_size
                    );
                    HashMap::with_capacity(hash_table_size)
                }
                _ => HashMap::new(),
            };

            db_hash_maps.insert(db_number, hash_map);
            let hash_map = db_hash_maps.get_mut(&db_number).expect(
                "get(key) should return Some(..) after an entry has been inserted for key ",
            );

            while {
                let next = *iter.peek().ok_or(Error::Truncated)?;
                next != SELECTDB && next != EOF
            } {
                let expires_at = try_timestamp(&mut iter)?;
                let value_type = decode_value_type_identifier(&mut iter)?;
                let key = decode_string(&mut iter)?;
                let value = match value_type {
                    ValueTypeIdentifier::String => ValueType::String(decode_string(&mut iter)?),
                    ValueTypeIdentifier::List => ValueType::List(decode_list(&mut iter)?),
                    ValueTypeIdentifier::Set => ValueType::Set(decode_set(&mut iter)?),
                };

                hash_map.insert(
                    key,
                    Value {
                        expires_at,
                        value_type: value,
                    },
                );
            }
        }

        expect_op_code(EOF, &mut iter)?;
        println!("reached EOF op_code");

        Ok(RDB {
            version,
            auxiliary_fields,
            db_hash_maps,
        })
    }
}
fn validate_checksum(bytes: &[u8]) -> Result<u64, Error> {
    let (bytes, checksum_bytes) = bytes.split_at(bytes.len() - 8);
    if checksum_bytes.len() < 8 {
        return Err(Error::Truncated);
    }

    let checksum = u64::from_le_bytes([
        checksum_bytes[0],
        checksum_bytes[1],
        checksum_bytes[2],
        checksum_bytes[3],
        checksum_bytes[4],
        checksum_bytes[5],
        checksum_bytes[6],
        checksum_bytes[7],
    ]);
    if checksum == 0 {
        return Ok(0);
    }
    let computed_checksum = crc64::crc64(0, bytes);
    if checksum != computed_checksum {
        return Err(Error::ChecksumMismatch(checksum, computed_checksum));
    }
    Ok(computed_checksum)
}

fn parse_auxiliary_fields<I: Iterator<Item = u8>>(
    iter: &mut Peekable<I>,
) -> Result<HashMap<String, String>, Error> {
    let mut auxiliary_fields = HashMap::new();
    while let Some(&AUX) = iter.peek() {
        iter.next()
            .expect("when peek() returns Some(..) next() should also return Some(..)");

        let key = decode_string(iter)?;
        let value = decode_string(iter)?;
        auxiliary_fields.insert(key, value);
    }

    Ok(auxiliary_fields)
}

fn try_timestamp<I: Iterator<Item = u8>>(
    iter: &mut Peekable<I>,
) -> Result<Option<Timestamp>, Error> {
    match iter.peek().ok_or(Error::Truncated)? {
        &EXPIRETIME => {
            iter.next()
                .expect("when peek() returns Some(..) next() should also return Some(..)");
            let bytes = [
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
            ];
            let seconds = u32::from_be_bytes(bytes);
            Ok(Some(Timestamp::Seconds(seconds)))
        }
        &EXPIRETIMEMS => {
            iter.next()
                .expect("when peek() returns Some(..) next() should also return Some(..)");
            let bytes = [
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
            ];
            let milliseconds = u64::from_be_bytes(bytes);
            Ok(Some(Timestamp::Milliseconds(milliseconds)))
        }
        _ => Ok(None),
    }
}

fn expect_op_code(expected_op_code: u8, iter: &mut impl Iterator<Item = u8>) -> Result<(), Error> {
    let op_code = iter.next().ok_or(Error::Truncated)?;
    if op_code != expected_op_code {
        return Err(Error::UnexpectedOpCode(op_code));
    }

    Ok(())
}

#[derive(Debug, PartialEq)]
pub struct Value {
    pub expires_at: Option<Timestamp>,
    pub value_type: ValueType,
}

impl Value {
    pub fn new(expires_at: Option<Timestamp>, value_type: ValueType) -> Value {
        Value {
            expires_at,
            value_type,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ValueType {
    String(String),
    List(Vec<String>),
    Set(HashSet<String>),
}

#[derive(Debug, PartialEq)]
pub enum ValueTypeIdentifier {
    String,
    List,
    Set,
}
impl From<&ValueType> for ValueTypeIdentifier {
    fn from(value: &ValueType) -> Self {
        match value {
            ValueType::String(_) => ValueTypeIdentifier::String,
            ValueType::List(_) => ValueTypeIdentifier::List,
            ValueType::Set(_) => ValueTypeIdentifier::Set,
        }
    }
}

impl From<ValueTypeIdentifier> for u8 {
    fn from(value: ValueTypeIdentifier) -> Self {
        match value {
            ValueTypeIdentifier::String => 0,
            ValueTypeIdentifier::List => 1,
            ValueTypeIdentifier::Set => 2,
        }
    }
}

impl TryFrom<u8> for ValueTypeIdentifier {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ValueTypeIdentifier::String),
            1 => Ok(ValueTypeIdentifier::List),
            2 => Ok(ValueTypeIdentifier::Set),
            _ => Err(Error::InvalidValueType(value)),
        }
    }
}

fn decode_value_type_identifier(
    iter: &mut impl Iterator<Item = u8>,
) -> Result<ValueTypeIdentifier, Error> {
    ValueTypeIdentifier::try_from(iter.next().ok_or(Error::Truncated)?)
}

fn encode_value(value: &Value) -> Result<Vec<u8>, LzfError> {
    let result = match &value.value_type {
        ValueType::String(string) => encode_string(string)?,
        ValueType::List(list) => encode_list(list)?,
        ValueType::Set(set) => encode_set(set)?,
    };

    Ok(result)
}

fn encode_string(string: &str) -> Result<Vec<u8>, LzfError> {
    if string.len() > 20 {
        let mut bytes = Size::Compressed.encode();
        let compressed_string = lzf::compress(string.as_bytes())?;
        bytes.extend_from_slice(Size::Length(compressed_string.len()).encode().as_slice());
        bytes.extend_from_slice(Size::Length(string.len()).encode().as_slice());
        bytes.extend_from_slice(compressed_string.as_slice());

        Ok(bytes)
    } else if let Ok(i) = string.parse::<i32>() {
        return Ok(encode_integer(i));
    } else {
        let mut bytes = Size::Length(string.len()).encode();
        bytes.extend_from_slice(string.as_bytes());
        Ok(bytes)
    }
}

fn encode_integer(value: i32) -> Vec<u8> {
    match value {
        -128..=127 => {
            let mut bytes = Size::EightBits.encode();
            bytes.push(value as u8);
            bytes
        }
        -32768..=32767 => {
            let mut bytes = Size::SixteenBits.encode();
            bytes.extend_from_slice((value as i16) .to_be_bytes().as_slice());
            bytes
        }
        _ => {
            let mut bytes = Size::ThirtyTwoBits.encode();
            bytes.extend_from_slice(value.to_be_bytes().as_slice());
            bytes
        }
    }
}

fn encode_list(list: &Vec<String>) -> Result<Vec<u8>, LzfError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(Size::Length(list.len()).encode().as_slice());
    for item in list {
        bytes.extend_from_slice(encode_string(item)?.as_slice());
    }

    Ok(bytes)
}

fn encode_set(set: &HashSet<String>) -> Result<Vec<u8>, LzfError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(Size::Length(set.len()).encode().as_slice());
    for item in set {
        bytes.extend_from_slice(encode_string(item)?.as_slice());
    }

    Ok(bytes)
}

fn decode_string(iter: &mut impl Iterator<Item = u8>) -> Result<String, Error> {
    match decode_size(iter)? {
        Size::Length(len) => {
            let bytes = iter.by_ref().take(len).collect();
            Ok(String::from_utf8(bytes)?)
        }
        Size::EightBits => {
            let integer = iter.next().ok_or(Error::Truncated)? as i8;
            Ok(integer.to_string())
        }
        Size::SixteenBits => {
            let bytes = [
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
            ];
            Ok(i16::from_be_bytes(bytes).to_string())
        }
        Size::ThirtyTwoBits => {
            let bytes = [
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
            ];
            Ok(i32::from_be_bytes(bytes).to_string())
        }
        Size::Compressed => {
            let compressed_length = decode_length(iter)?;
            let uncompressed_length = decode_length(iter)?;
            let compressed_bytes: Vec<u8> = iter.by_ref().take(compressed_length).collect();
            let uncompressed_bytes =
                lzf::decompress(compressed_bytes.as_slice(), uncompressed_length)?;
            Ok(String::from_utf8(uncompressed_bytes)?)
        }
    }
}

fn decode_list(iter: &mut impl Iterator<Item = u8>) -> Result<Vec<String>, Error> {
    let len = decode_length(iter)?;
    let mut list = Vec::with_capacity(len);
    for _ in 0..len {
        let string = decode_string(iter)?;
        list.push(string);
    }

    Ok(list)
}

fn decode_set(iter: &mut impl Iterator<Item = u8>) -> Result<HashSet<String>, Error> {
    let len = decode_length(iter)?;
    let mut set = HashSet::with_capacity(len);
    for _ in 0..len {
        let string = decode_string(iter)?;
        set.insert(string);
    }

    Ok(set)
}

enum Size {
    Length(usize),
    EightBits,
    SixteenBits,
    ThirtyTwoBits,
    Compressed,
}

impl Size {
    fn encode(&self) -> Vec<u8> {
        match self {
            Size::Length(len) => {
                if *len < 64 {
                    vec![*len as u8]
                } else if *len < 16384 {
                    vec![((*len >> 8) | 0b0100_0000) as u8, *len as u8]
                } else {
                    let mut bytes = vec![0b0100_0000];
                    bytes.extend_from_slice(&len.to_be_bytes());

                    bytes
                }
            }
            Size::EightBits => {
                vec![0b1100_0000]
            }
            Size::SixteenBits => {
                vec![1 | 0b1100_0000]
            }
            Size::ThirtyTwoBits => {
                vec![2 | 0b1100_0000]
            }
            Size::Compressed => {
                vec![3 | 0b1100_0000]
            }
        }
    }
}

fn decode_length(iter: &mut impl Iterator<Item = u8>) -> Result<usize, Error> {
    match decode_size(iter)? {
        Size::Length(len) => Ok(len),
        _ => Err(Error::ExpectedLength),
    }
}

fn decode_size(iter: &mut impl Iterator<Item = u8>) -> Result<Size, Error> {
    let first = iter.next().ok_or(Error::Truncated)?;
    match first >> 6 {
        0b0000_0000 => Ok(Size::Length((first & 0b0011_1111) as usize)),
        0b0000_0001 => {
            let second = iter.next().ok_or(Error::Truncated)?;
            let length_parts = [0, 0, 0, 0, 0, 0, first & 0b0011_1111, second];
            Ok(Size::Length(usize::from_be_bytes(length_parts)))
        }
        0b0000_0010 => {
            let length_parts = [
                0,
                0,
                0,
                0,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
                iter.next().ok_or(Error::Truncated)?,
            ];
            Ok(Size::Length(usize::from_be_bytes(length_parts)))
        }
        0b0000_0011 => match first & 0b0011_1111 {
            0 => Ok(Size::EightBits),
            1 => Ok(Size::SixteenBits),
            2 => Ok(Size::ThirtyTwoBits),
            3 => Ok(Size::Compressed),
            _ => Err(Error::InvalidLengthEncoding(first)),
        },
        _ => panic!("it should not be possible that a byte right shifted by 6 is greater than 3"),
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    WrongMagicNumber(Vec<u8>),
    FromUtf8(FromUtf8Error),
    UnexpectedOpCode(u8),
    Truncated,
    InvalidLengthEncoding(u8),
    ExpectedLength,
    LzfError(LzfError),
    InvalidValueType(u8),
    ChecksumMismatch(u64, u64),
    ParseIntError(ParseIntError),
}
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::FromUtf8(value)
    }
}

impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::ParseIntError(value)
    }
}

impl From<LzfError> for Error {
    fn from(value: LzfError) -> Self {
        Error::LzfError(value)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(err) => err.fmt(f),
            Error::WrongMagicNumber(number) => {
                write!(f, "wrong magic number {:?}", number)
            }
            Error::FromUtf8(err) => err.fmt(f),
            Error::UnexpectedOpCode(opCode) => {
                write!(f, "unexpected op code {:?}", opCode)
            }
            Error::Truncated => write!(f, "rdb file is truncated"),
            Error::InvalidLengthEncoding(length_byte) => {
                write!(f, "invalid length encoded byte {}", length_byte)
            }
            Error::ExpectedLength => write!(f, "expected length but got different size format"),
            Error::ParseIntError(err) => err.fmt(f),
            Error::LzfError(err) => write!(f, "LFZ compression error {}", err),
            Error::InvalidValueType(invalid_type) => {
                write!(f, "invalid value type {}", invalid_type)
            }
            Error::ChecksumMismatch(expected, actual) => write!(
                f,
                "file contains checksum {} but the computed checksum of the file is {}",
                actual, expected
            ),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod test {
    use super::*;
    use std::error;

    #[test]
    fn import_file() -> Result<(), Box<dyn error::Error>> {
        let mut want = RDB {
            version: 7,
            auxiliary_fields: HashMap::from([
                (String::from("redis-ver"), String::from("1")),
                (String::from("redis-bits"), String::from("64")),
                (String::from("ctime"), String::from("2")),
                (String::from("used-mem"), String::from("3")),
            ]),
            db_hash_maps: HashMap::new(),
        };

        let hash_map = HashMap::from([
            (
                String::from("string1"),
                Value {
                    expires_at: None,
                    value_type: ValueType::String(String::from("value")),
                },
            ),
            (
                String::from("string2"),
                Value {
                    expires_at: Some(Timestamp::Milliseconds(1000000)),
                    value_type: ValueType::String(String::from(
                        "loooooooooooooooooooooooooooooooooooooooooooong value",
                    )),
                },
            ),
            (
                String::from("string3"),
                Value {
                    expires_at: Some(Timestamp::Seconds(2)),
                    value_type: ValueType::String(String::from("123456")),
                },
            ),
            (
                String::from("string4"),
                Value {
                    expires_at: None,
                    value_type: ValueType::String(String::from("")),
                },
            ),
            (
                String::from("list"),
                Value {
                    expires_at: None,
                    value_type: ValueType::List(vec![
                        String::from("127"),
                        String::from("32767"),
                        String::from("2147483647"),
                    ]),
                },
            ),
            (
                String::from("set"),
                Value {
                    expires_at: None,
                    value_type: ValueType::Set(HashSet::from([
                        String::from("-128"),
                        String::from("-32768"),
                        String::from("-2147483648"),
                    ])),
                },
            ),
        ]);
        want.db_hash_maps.insert(0, hash_map);
        want.db_hash_maps.insert(
            1,
            HashMap::from([(
                String::from("key"),
                Value {
                    expires_at: None,
                    value_type: ValueType::String(String::from("value")),
                },
            )]),
        );

        let bytes = Vec::<u8>::try_from(&want)?;

        let got = RDB::try_from(bytes)?;

        assert_eq!(want, got);

        Ok(())
    }
}
