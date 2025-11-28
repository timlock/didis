use crate::parser::command::Command;
use crate::parser::resp;
use crate::parser::resp::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

pub struct Client {
    tcp_stream: TcpStream,
    resp_parser: resp::Parser,
}

impl Client {
    pub fn new(tcp_stream: TcpStream) -> Self {
        Self {
            tcp_stream,
            resp_parser: resp::Parser::default(),
        }
    }

    pub fn send(&mut self, command: Command) -> Result<Value, resp::Error> {
        println!("Sending command to server {}", command);
        let mut bytes = Vec::from(command);

        let start = Instant::now();

        self.tcp_stream.write_all(bytes.as_slice())?;

        bytes.clear();
        let mut buf = [0u8; 1024];
        loop {
            let n = self.tcp_stream.read(&mut buf)?;
            bytes.extend_from_slice(&buf[..n]);
            if n < buf.len() {
                break;
            }
        }

        let (response, _) = self.resp_parser.parse(bytes.as_slice())?.ok_or(resp::Error::LengthMismatch)?;

        println!(
            "Received response from server  bytes duration={:?} {}",
            start.elapsed(),
            response
        );

        Ok(response)
    }

    pub fn send_batch(
        &mut self,
        commands: Vec<Command>,
    ) -> Result<Vec<Value>, resp::Error> {

        println!("Sending command batch to server {:?}", commands);
        let mut bytes: Vec<u8> = commands.into_iter().flat_map(Vec::from).collect();

        let start = Instant::now();

        self.tcp_stream.write_all(bytes.as_slice())?;

        bytes.clear();
        let mut buf = [0u8; 1024];
        loop {
            let n = self.tcp_stream.read(&mut buf)?;
            bytes.extend_from_slice(&buf[..n]);
            if n < buf.len() {
                break;
            }
        }

        let values = self.resp_parser.parse_all(bytes.as_slice())?;

        println!(
            "Received response batch of size {} from server duration={:?} {:?}",
            values.len(),
            start.elapsed(),
            values
        );

        Ok(values)
    }
}
