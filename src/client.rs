use crate::parser::command::Command;
use crate::parser::resp;
use crate::parser::resp::Resp;
use std::io::{Read, Write};
use std::net::TcpStream;

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

    pub fn send(&mut self, command: Command) -> Result<Resp, resp::Error> {
        println!("Sending command to server {:?}", command);
        let mut bytes = Vec::from(command);
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

        let (response, size) = self.resp_parser.parse(bytes.as_slice())?;

        println!("Received response from server size {} {:?}", size, response);

        Ok(response.expect("TODO error handling"))
    }
}
