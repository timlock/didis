mod heap;
mod manifest;
mod sstable;
pub mod storage;
mod wal;

use std::array::TryFromSliceError;
use std::fmt::{Debug, Display};
use std::fs::File;
use std::io::{BufRead, BufWriter, IntoInnerError, Seek, Write};
use std::num::ParseIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;
use std::{error, fmt, io};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    FromUtf8(FromUtf8Error),
    Utf8(Utf8Error),
    TryFromSliceError(TryFromSliceError),
    ParseIntError(ParseIntError),
    UnknownOperation(u8),
    Truncated,
    InvalidTableName(String),
    InvalidManifestEntry(String),
    NoManifestEntryForLevel(usize),
    IntoInner(IntoInnerError<BufWriter<File>>),
    UnexpectedCharacter { want: char, got: char },
    UnexpectedByte { want: u8, got: u8 },
    ChecksumMismatch { want: u64, got: u64 },
}

impl error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FromUtf8(e) => write!(f, "{}", e),
            Error::Utf8(e) => write!(f, "{}", e),
            Error::Io(error) => write!(f, "IO error: {error}"),
            Error::TryFromSliceError(e) => write!(f, "{}", e),
            Error::ParseIntError(e) => write!(f, "{}", e),
            Error::UnknownOperation(op_code) => write!(f, "Unknown op code {}", op_code),
            Error::Truncated => write!(f, "Truncated"),
            Error::InvalidManifestEntry(name) => {
                write!(f, "Manifest contains invalid entry {}", name)
            }
            Error::IntoInner(e) => write!(f, "{}", e),
            Error::UnexpectedCharacter { want, got } => {
                write!(f, "Expected character '{want}' got '{got}'")
            }
            Error::UnexpectedByte { want, got } => {
                write!(f, "Expected byte '{want}' got '{got}'")
            }
            Error::NoManifestEntryForLevel(level) => {
                write!(f, "No manifest exists for level {level}")
            }
            Error::InvalidTableName(value) => write!(f, "Invalid table name {value}"),
            Error::ChecksumMismatch { want, got } => {
                write!(f, "Checksum mismatch want {} got {}", want, got)
            }
        }
    }
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

impl From<Utf8Error> for Error {
    fn from(value: Utf8Error) -> Self {
        Error::Utf8(value)
    }
}

impl From<TryFromSliceError> for Error {
    fn from(value: TryFromSliceError) -> Self {
        Error::TryFromSliceError(value)
    }
}
impl From<ParseIntError> for Error {
    fn from(value: ParseIntError) -> Self {
        Error::ParseIntError(value)
    }
}

impl From<IntoInnerError<BufWriter<File>>> for Error {
    fn from(value: IntoInnerError<BufWriter<File>>) -> Self {
        Error::IntoInner(value)
    }
}

#[derive(Debug, PartialOrd, PartialEq)]
pub enum Operation {
    Insert(String, String),
    Delete(String),
}

impl Operation {
    pub fn key(&self) -> &str {
        match self {
            Operation::Insert(key, _) => key,
            Operation::Delete(key) => key,
        }
    }
}

impl From<&Operation> for OperationCode {
    fn from(value: &Operation) -> Self {
        match value {
            Operation::Insert(_, _) => OperationCode::Insert,
            Operation::Delete(_) => OperationCode::Delete,
        }
    }
}

#[derive(Debug)]
enum OperationCode {
    Insert,
    Delete,
}

impl From<&OperationCode> for u8 {
    fn from(value: &OperationCode) -> Self {
        match value {
            OperationCode::Insert => 1,
            OperationCode::Delete => 2,
        }
    }
}

impl TryFrom<u8> for OperationCode {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(OperationCode::Insert),
            2 => Ok(OperationCode::Delete),
            _ => Err(Error::UnknownOperation(value)),
        }
    }
}

fn read_length_prefixed_string<R: BufRead + Seek>(source: &mut R) -> Result<String, Error> {
    expect_byte(source, b'$')?;
    let len = read_length(source)?;

    let mut buf = vec![0;len as usize];
    source.read_exact(&mut buf)?;
    let string = String::from_utf8(buf)?;

    Ok(string)
}

fn write_length_prefixed_string(destination: &mut impl Write, value: &str) -> io::Result<()> {
    destination.write_all(b"$")?;
    write_length(destination, value.len() as u64)?;

    write!(destination, "{}", value)?;
    Ok(())
}

fn write_integer(destination: &mut impl Write, integer: u64) -> io::Result<usize> {
    destination.write_all(b":")?;
    let bytes = integer.to_le_bytes();
    destination.write_all(&bytes)?;
    destination.write_all(b";")?;

    Ok(bytes.len() + 2)
}

const FOURTEEN_BIT_LEN: u8 = 0b0100_0000;

const FOUR_BYTE_LEN: u8 = 0b1000_0000;
const EIGHT_BYTE_LEN: u8 = 0b1000_0001;

fn write_length(destination: &mut impl Write, len: u64) -> io::Result<()> {
    let buf: &[u8] = if len < 1 << 6 {
        &[len as u8]
    } else if len < 1 << 14 {
        &[(len >> 8) as u8 | FOURTEEN_BIT_LEN, len as u8]
    } else if len <= u32::MAX as u64 {
        let bytes = (len as u32).to_le_bytes();
        &[FOUR_BYTE_LEN, bytes[0], bytes[1], bytes[2], bytes[3]]
    } else {
        let bytes = len.to_le_bytes();
        &[
            EIGHT_BYTE_LEN,
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[4],
            bytes[5],
            bytes[6],
            bytes[7],
        ]
    };

    destination.write_all(&buf)
}

fn read_length(mut source: &mut impl BufRead) -> io::Result<u64> {
    let mut first_part = [0; 1];
    source.read_exact(&mut first_part)?;

    let len = match first_part[0] >> 6 {
        0 => first_part[0] as u64,
        1 => {
            let mut second_part = [0; 1];
            source.read_exact(&mut second_part)?;
            u16::from_le_bytes([first_part[0] & 0b0011_1111, second_part[0]]) as u64
        }
        3 if first_part[0] == FOUR_BYTE_LEN => {
            let mut remaining_parts = [0; 4];
            source.read_exact(&mut remaining_parts)?;
            u32::from_le_bytes(remaining_parts) as u64
        }
        3 if first_part[0] == EIGHT_BYTE_LEN => {
            let mut remaining_parts = [0; 8];
            source.read_exact(&mut remaining_parts)?;
            u64::from_le_bytes(remaining_parts)
        }
        _ => panic!("it should not be possible that a byte right shifted by 6 is greater than 3"),
    };

    Ok(len)
}

fn read_integer(mut source: &mut impl BufRead) -> Result<u64, Error> {
    expect_byte(&mut source, b':')?;

    let mut buf = Vec::new();
    if source.read_until(b';', &mut buf)? == 0 {
        return Err(Error::Truncated);
    };
    match buf.pop() {
        Some(b';') => {}
        Some(_) | None => return Err(Error::Truncated),
    }

    let integer = u64::from_le_bytes(buf.as_slice().try_into()?);

    Ok(integer)
}

fn read_operation<R: BufRead + Seek>(source: &mut R) -> Result<Operation, Error> {
    let mut op_code_bytes = [0u8; 1];
    let n = source.read(&mut op_code_bytes)?;
    if n == 0 {
        return Err(Error::Truncated);
    }

    let op_code = OperationCode::try_from(op_code_bytes[0])?;

    match op_code {
        OperationCode::Insert => {
            let key = read_length_prefixed_string(source)?;
            let value = read_length_prefixed_string(source)?;

            Ok(Operation::Insert(key, value))
        }
        OperationCode::Delete => {
            let key = read_length_prefixed_string(source)?;

            Ok(Operation::Delete(key))
        }
    }
}

fn write_operation(destination: &mut impl Write, operation: &Operation) -> io::Result<()> {
    let operation_code = OperationCode::from(operation);
    destination.write_all(&[u8::from(&operation_code)])?;

    match operation {
        Operation::Insert(key, value) => {
            write_length_prefixed_string(destination, key)?;
            write_length_prefixed_string(destination, value)?;
        }
        Operation::Delete(key) => {
            write_length_prefixed_string(destination, key)?;
        }
    }
    Ok(())
}

fn expect_byte(source: &mut impl BufRead, want: u8) -> Result<(), Error> {
    let mut buf = [0; 1];
    source.read_exact(&mut buf)?;
    if buf[0] != want {
        return Err(Error::UnexpectedByte { want, got: buf[0] });
    }

    Ok(())
}
