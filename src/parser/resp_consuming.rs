use std::io;
use std::io::Read;
use std::num::ParseIntError;

use super::resp;

struct BulkStringParser {
    length_parser: LenghtParser,
    buf: Option<Vec<u8>>,
    cursor: usize,
}
impl BulkStringParser {
    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Vec<u8>>, resp::Error> {
        match self.buf{
            None =>
         {
            if let Some(number) = self.length_parser.read(reader)? {
                self.buf = Some(vec![0; number])
            }
        }
        Some(buf) =>{
        let n = reader.read(&mut self.buf[self.cursor..])?;
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
    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<i64>, resp::Error> {
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
            return Ok(Some(number));
        }

        Ok(None)
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

                    let offset = buf.len() - self.index;

                    let mut b = [0; 8];
                    b[offset..].copy_from_slice(&buf[..self.index]);
                    let number = convert(&b)?;
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
}

// struct IntegerParser {
//     cr: bool,
//     lf: bool,
//     buf: [u8; 8],
//     index: usize,
// }

// impl IntegerParser {
//     fn parse(&mut self, reader: &mut impl io::Read) -> Result<bool, Error> {
//         for byte in reader.bytes() {
//             let token = match byte {
//                 Ok(byte) => byte,
//                 Err(err) => return Err(Error::Io(err)),
//             };
//             match token {
//                 b'\r' => self.cr = true,
//                 b'\n' => {
//                     if !self.cr {
//                         return Err(Error::UnallowedToken(token));
//                     }

//                     return Ok(true);
//                 }
//                 b'+' | b'-' => {
//                     if self.index != 0 {
//                         return Err(Error::UnallowedToken(token));
//                     }

//                     self.buf[0] = token;
//                     self.index += 1;
//                 }
//                 b'0'..=b'9' => {
//                     if self.cr {
//                         return Err(Error::UnallowedToken(token));
//                     } else {
//                         self.buf[self.index] = token;
//                         self.index += 1;
//                     }
//                 }
//                 _ => {
//                     return Err(Error::UnallowedToken(token));
//                 }
//             }
//         }
//         Ok(false)
//     }
// }

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

    fn parse(&mut self, reader: &mut impl io::Read) -> Result<Option<Vec<u8>>, resp::Error> {
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
                    true => return Ok(self.buf.take()),
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

fn convert(buf: &[u8]) -> Result<usize, resp::Error> {
    let utf8 = std::str::from_utf8(buf).map_err(|err| resp::Error::Misc(err.to_string()))?;
    let length = utf8
        .parse()
        .map_err(|err: ParseIntError| resp::Error::Misc(err.to_string()))?;
    Ok(length)
}
