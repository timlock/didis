use crate::async_io::{AsyncIO, Completion, IO};
use crate::controller::Controller;
use crate::parser::command::Parser;
use crate::parser::resp::Resp;
use std::cmp::min;
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::time::Duration;
use std::{
    collections::HashMap,
    io::{self},
    net::SocketAddr,
};


const BUFFER_SIZE: usize = 4096;

pub struct Server {
    connections: HashMap<RawFd, Connection>,
    controller: Controller,
    io: IO,
}

impl Server {
    pub fn new(io: IO) -> Self {
        let connections = HashMap::new();
        Self {
            connections,
            controller: Default::default(),
            io,
        }
    }

    pub fn run(&mut self, address: &str) -> Result<(), io::Error> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;

        self.io.accept(listener);

        loop {
            for completion in self.io.poll_timeout(Duration::from_secs(1))? {
                match completion {
                    Completion::Accept(listener, result) => {
                        self.handle_accept(result)?;
                        self.io.accept(listener);
                    }
                    Completion::Send(stream, buf, result) => match result {
                        Ok(sent) => self.handle_send(stream, buf, sent),
                        Err(err) => {
                            self.connections.remove(&stream.as_raw_fd());
                            println!("Closed connection {stream:?}: {err}");
                        }
                    },
                    Completion::Receive(stream, buf, res) => match res {
                        Ok(received) => self.handle_receive(stream, buf, received),
                        Err(err) => {
                            self.connections.remove(&stream.as_raw_fd());
                            println!("Closed connection {stream:?} IO error: {err}");
                        }
                    },
                }
            }
        }
    }

    fn handle_accept(&mut self, result: io::Result<(TcpStream, SocketAddr)>) -> io::Result<()> {
        let (stream, address) = result?;
        println!["New client connected with address {}", address];
        let cloned = stream.try_clone()?;
        let client = Connection::new(cloned, address);
        self.connections.insert(stream.as_raw_fd(), client);

        let buf = Box::new([0u8; BUFFER_SIZE]);
        self.io.receive(stream, buf);
        Ok(())
    }

    fn handle_send(&mut self, stream: TcpStream, mut buf: Box<[u8]>, sent: usize) {
        let connection = self.connections.get_mut(&stream.as_raw_fd()).expect(
            format![
                "Send data to unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            ]
            .as_str(),
        );
        connection.handle_sent(sent, buf.as_mut());
        if connection.to_send > 0 {
            self.io.send(stream, buf, connection.to_send);
        } else {
            self.io.receive(stream, buf);
        }
    }

    fn handle_receive(&mut self, stream: TcpStream, mut buffer: Box<[u8]>, received: usize) {
        let connection = self.connections.get_mut(&stream.as_raw_fd()).expect(
            format![
                "Received data from unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            ]
            .as_str(),
        );

        match received {
            0 => {
                self.connections.remove(&stream.as_raw_fd());
                println!("Closed connection {stream:?}");
            }
            len => {
                let (commands, read) = connection.command_parser.parse_all(&buffer[..len]);
                connection.remaining_in.extend(&buffer[read..len]);

                for command in commands {
                    let bytes = match command {
                        Ok(command) => {
                            println!("Received command: {:?}", command);
                            let response = self.controller.handle_command(command);
                            println!("Sending response {response}");

                            response.to_bytes()
                        }
                        Err(err) => {
                            eprintln!("Received faulty command: {:?}", err);
                            Resp::SimpleError(err.to_string()).to_bytes()
                        }
                    };

                    if connection.to_send + bytes.len() > buffer.len() {
                        connection.remaining_out.extend(bytes);
                    } else {
                        buffer[connection.to_send..(connection.to_send + bytes.len())]
                            .copy_from_slice(bytes.as_slice());
                        connection.to_send += bytes.len();
                    }
                }

                self.io.send(stream, buffer, connection.to_send);
            }
        }
    }
}

struct Connection {
    remaining_out: Vec<u8>,
    to_send: usize,
    remaining_in: Vec<u8>,
    command_parser: Parser,
    address: SocketAddr,
    stream: TcpStream,
}

impl Connection {
    fn new(stream: TcpStream, address: SocketAddr) -> Self {
        Self {
            remaining_out: Default::default(),
            to_send: 0,
            remaining_in: Default::default(),
            command_parser: Default::default(),
            address,
            stream,
        }
    }

    fn handle_sent(&mut self, sent: usize, buffer: &mut [u8]) {
        if self.to_send > 0 {
            buffer.copy_within(sent..self.to_send, 0);
        }

        self.to_send -= sent;

        if !self.remaining_out.is_empty() && self.to_send != buffer.len() {
            let to_copy = min(buffer.len() - self.to_send, self.remaining_out.len());
            let copy_range = self.to_send..(self.to_send + to_copy);
            buffer[copy_range].copy_from_slice(&self.remaining_out.as_slice()[..to_copy]);
        }
    }
}
