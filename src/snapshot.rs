use lzf::LzfError;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::io;
use std::iter::Peekable;
use std::ops::Add;
use std::string::FromUtf8Error;
use std::time::{Duration, SystemTime};

const MAGIC_NUMBER: &'static [u8] = b"REDIS";

const EOF: u8 = 0xFF;
const SELECTDB: u8 = 0xFE;
const EXPIRETIME: u8 = 0xFD;
const EXPIRETIMEMS: u8 = 0xFC;
const RESIZEDB: u8 = 0xFB;
const AUX: u8 = 0xFA;

pub struct RDB {
    version: String,
    auxiliary_fields: HashMap<String, String>,
    checksum: u64,
}

impl TryFrom<Vec<u8>> for RDB {
    type Error = Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let checksum = checksum(bytes.as_slice())?;

        let mut iter = bytes.into_iter().peekable();

        let magic_number: Vec<u8> = iter.by_ref().take(5).collect();
        if MAGIC_NUMBER != magic_number.as_slice() {
            return Err(Error::WrongMagicNumber(magic_number));
        }

        let version_raw: Vec<u8> = iter.by_ref().take(4).collect();
        let version = String::from_utf8(version_raw)?;

        let mut auxiliary_fields = parse_auxiliary_fields(&mut iter)?;

        let mut db_hash_maps = HashMap::new();
        while let Some(SELECTDB) = iter.next() {
            let db_number = iter.next().ok_or(Error::Truncated)?;

            let hash_map = match iter.peek() {
                Some(&RESIZEDB) => {
                    iter.next()
                        .expect("when peek() returns Some(..) next() should also return Some(..)");

                    let hash_table_size = parse_length(&mut iter)?;
                    let expire_hash_table_size = parse_length(&mut iter)?;
                    HashMap::with_capacity(hash_table_size + expire_hash_table_size)
                }
                _ => HashMap::new(),
            };

            db_hash_maps.insert(db_number, hash_map);
            let hash_map = db_hash_maps.get_mut(&db_number).expect(
                "get(key) should return Some(..) after an entry has been inserted for key ",
            );

            while &SELECTDB != iter.peek().ok_or(Error::Truncated)? {
                let expires_at = try_timestamp(&mut iter)?;
                let value_type = parse_value_type(&mut iter)?;
                let key = parse_string(&mut iter)?;
                let value = match value_type {
                    ValueTypeIdentifier::String => ValueType::String(parse_string(&mut iter)?),
                    ValueTypeIdentifier::List => ValueType::List(parse_list(&mut iter)?),
                    ValueTypeIdentifier::Set => ValueType::Set(parse_set(&mut iter)?),
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

        Ok(RDB {
            version,
            auxiliary_fields,
            checksum,
        })
    }
}

fn checksum(bytes: &[u8]) -> Result<u64, Error> {
    if bytes.len() < 8 {
        return Err(Error::Truncated);
    }

    let checksum = u64::from_be_bytes([
        bytes[bytes.len() - 8],
        bytes[bytes.len() - 7],
        bytes[bytes.len() - 6],
        bytes[bytes.len() - 5],
        bytes[bytes.len() - 4],
        bytes[bytes.len() - 3],
        bytes[bytes.len() - 2],
        bytes[bytes.len() - 1],
    ]);
    if checksum == 0 {
        return Ok(0);
    }
    let computed_checksum = crc64::crc64(checksum, bytes);
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

        let key = parse_string(iter)?;
        let value = parse_string(iter)?;
        auxiliary_fields.insert(key, value);
    }

    Ok(auxiliary_fields)
}

fn try_timestamp<I: Iterator<Item = u8>>(
    iter: &mut Peekable<I>,
) -> Result<Option<SystemTime>, Error> {
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
            let seconds = u32::from_be_bytes(bytes) as u64;
            Ok(Some(
                SystemTime::UNIX_EPOCH.add(Duration::from_secs(seconds)),
            ))
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
            let seconds = u64::from_be_bytes(bytes);
            Ok(Some(
                SystemTime::UNIX_EPOCH.add(Duration::from_millis(seconds)),
            ))
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

struct Value {
    expires_at: Option<SystemTime>,
    value_type: ValueType,
}

enum ValueType {
    String(String),
    List(Vec<String>),
    Set(HashSet<String>),
}

enum ValueTypeIdentifier {
    String,
    List,
    Set,
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

fn parse_value_type(iter: &mut impl Iterator<Item = u8>) -> Result<ValueTypeIdentifier, Error> {
    ValueTypeIdentifier::try_from(iter.next().ok_or(Error::Truncated)?)
}

fn parse_string(iter: &mut impl Iterator<Item = u8>) -> Result<String, Error> {
    match parse_size(iter)? {
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
            let compressed_length = parse_length(iter)?;
            let uncompressed_length = parse_length(iter)?;
            let compressed_bytes: Vec<u8> = iter.by_ref().take(compressed_length).collect();
            let uncompressed_bytes =
                lzf::decompress(compressed_bytes.as_slice(), uncompressed_length)?;
            Ok(String::from_utf8(uncompressed_bytes)?)
        }
    }
}

fn parse_list(iter: &mut impl Iterator<Item = u8>) -> Result<Vec<String>, Error> {
    let len = parse_length(iter)?;
    let mut list = Vec::with_capacity(len);
    for _ in 0..len {
        let string = parse_string(iter)?;
        list.push(string);
    }

    Ok(list)
}

fn parse_set(iter: &mut impl Iterator<Item = u8>) -> Result<HashSet<String>, Error> {
    let len = parse_length(iter)?;
    let mut set = HashSet::with_capacity(len);
    for _ in 0..len {
        let string = parse_string(iter)?;
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

fn parse_length(iter: &mut impl Iterator<Item = u8>) -> Result<usize, Error> {
    match parse_size(iter)? {
        Size::Length(len) => Ok(len),
        _ => Err(Error::ExpectedLength),
    }
}

fn parse_size(iter: &mut impl Iterator<Item = u8>) -> Result<Size, Error> {
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
