use std::io;
use std::io::BufRead;

use crate::parser::command::Command;
use crate::parser::resp::Resp;

pub mod resp;
pub mod command;

pub fn parse(data: &[u8]) -> Result<Vec<Command>, Resp> {
    let resps = Resp::parse(data)?;
    let mut commands = Vec::new();
    for resp in resps {
        let command = Command::try_from(resp)?;
        commands.push(command);
    }
    Ok(commands)
}

pub fn try_read(buf_reader: &mut impl BufRead) -> io::Result<Vec<u8>> {
    const CHUNK_SIZE: usize = 1024;
    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    let mut buf = [0; CHUNK_SIZE];
    loop {
        match buf_reader.read(&mut buf) {
            Ok(size) => {
                buffer.extend_from_slice(&buf[..size]);
                if size < CHUNK_SIZE {
                    break;
                }
            }
            Err(err) => return Err(err),
        }
    }
    Ok(buffer)
}
