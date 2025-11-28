use crate::async_io::{AsyncIO, Completion, IO};
use crate::controller::Controller;
use crate::parser::command::{Parser};
use crate::parser::resp::Value;
use std::cmp::min;
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::{
    collections::HashMap,
    io::{self},
    net::SocketAddr,
};

const BUFFER_SIZE: usize = 4096;

pub struct Server {
    address: SocketAddr,
    connections: HashMap<RawFd, Connection>,
    controller: Controller,
    done: Arc<AtomicBool>,
}

impl Server {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            connections: Default::default(),
            controller: Default::default(),
            done: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn run(&mut self, io: &mut impl AsyncIO) -> io::Result<()> {
        println!("Server starts listening on {}", self.address);

        let listener = TcpListener::bind(self.address)?;
        listener.set_nonblocking(true)?;
        io.accept(listener);

        loop {
            for completion in io.poll_timeout(Duration::from_secs(1))? {
                match completion {
                    Completion::Accept(listener, result) => {
                        self.handle_accept(io, result)?;
                        io.accept(listener);
                    }
                    Completion::Send(stream, buf, result) => match result {
                        Ok(sent) => self.handle_send(io, stream, buf, sent),
                        Err(err) => {
                            self.connections.remove(&stream.as_raw_fd());
                            println!("Closed connection {stream:?}: {err}");
                        }
                    },
                    Completion::Receive(stream, buf, result) => match result {
                        Ok(received) => self.handle_receive(io, stream, buf, received),
                        Err(err) => {
                            self.connections.remove(&stream.as_raw_fd());
                            println!("Closed connection {stream:?} IO error: {err}");
                        }
                    },
                }
            }

            if self.done.load(Ordering::SeqCst) {
                println!("Server stopped");
                return Ok(());
            }
        }
    }

    pub fn handle(&self) -> Arc<AtomicBool> {
        self.done.clone()
    }
    pub fn stop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
    }

    fn handle_accept(
        &mut self,
        io: &mut impl AsyncIO,
        result: io::Result<(TcpStream, SocketAddr)>,
    ) -> io::Result<()> {
        let (stream, address) = result?;
        println!["New client connected with address {}", address];
        let client = Connection::new(stream.try_clone()?, address);
        self.connections.insert(stream.as_raw_fd(), client);

        let buf = Box::new([0u8; BUFFER_SIZE]);
        io.receive(stream, buf);
        Ok(())
    }

    fn handle_send(
        &mut self,
        io: &mut impl AsyncIO,
        stream: TcpStream,
        mut buf: Box<[u8]>,
        sent: usize,
    ) {
        let connection = self.connections.get_mut(&stream.as_raw_fd()).expect(
            format![
                "Send data to unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            ]
            .as_str(),
        );

        connection.handle_sent(sent, &mut buf);
        println!(
            "Sent {} bytes to client, remaining bytes {}",
            sent, connection.to_send
        );
        if connection.to_send > 0 {
            io.send(stream, buf, connection.to_send);
            return;
        }
        io.receive(stream, buf);
    }

    fn handle_receive(
        &mut self,
        io: &mut impl AsyncIO,
        stream: TcpStream,
        mut buffer: Box<[u8]>,
        received: usize,
    ) {
        let connection = self.connections.get_mut(&stream.as_raw_fd()).expect(
            format![
                "Received data from unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            ]
            .as_str(),
        );

        if received == 0 {
            self.connections.remove(&stream.as_raw_fd());
            println!("Closed connection {stream:?}");
            return;
        }

        println!("Received {} bytes from client", received);

        let commands = connection.command_parser.parse_all(&buffer[..received]);
        let mut serialized_response = Vec::new();
        for command in commands {
            let response = match command {
                Ok(command) => {
                    println!("Received command: {}", command);
                    let response = self.controller.handle_command(command);
                    println!("Sending response {response}");

                    response.to_bytes()
                }
                Err(err) => {
                    eprintln!("Received faulty command: {:?}", err);
                    Value::SimpleError(err.to_string()).to_bytes()
                }
            };
            serialized_response.extend(response);
        }
        connection.handle_sending_response(serialized_response, &mut buffer);

        if connection.to_send > 0 {
            io.send(stream, buffer, connection.to_send);
            return;
        }

        println!("Received not enough bytes to handle message, receiving more bytes");
        io.receive(stream, buffer);
    }
}

struct Connection {
    remaining_out: Vec<u8>,
    to_send: usize,
    command_parser: Parser,
    address: SocketAddr,
    stream: TcpStream,
}

impl Connection {
    fn new(stream: TcpStream, address: SocketAddr) -> Self {
        Self {
            remaining_out: Default::default(),
            to_send: 0,
            command_parser: Default::default(),
            address,
            stream,
        }
    }

    fn handle_sent(&mut self, sent: usize, buffer: &mut [u8]) {
        if self.to_send > 0 {
            buffer.copy_within(sent..self.to_send, 0);
            self.to_send -= sent;
        }

        if !self.remaining_out.is_empty() && self.to_send < buffer.len() {
            let to_copy = min(buffer.len() - self.to_send, self.remaining_out.len());
            let copy_range = self.to_send..(self.to_send + to_copy);

            buffer[copy_range].copy_from_slice(self.remaining_out.drain(..to_copy).as_slice());

            self.to_send += to_copy;
        }
    }

    fn handle_sending_response(&mut self, mut response: Vec<u8>, buffer: &mut [u8]) {
        let to_send = min(buffer.len() - self.to_send, response.len());

        buffer[self.to_send..self.to_send + to_send]
            .copy_from_slice(response.drain(..to_send).as_slice());
        self.remaining_out.extend_from_slice(response.as_slice());
        self.to_send += to_send;
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::client::Client;
    use crate::parser::command::Command;
    use std::borrow::Cow;
    use std::str::FromStr;
    use std::thread;
    use std::thread::JoinHandle;

    fn launch_server(address: SocketAddr) -> (Arc<AtomicBool>, JoinHandle<()>) {
        let mut server = Server::new(address);
        let server_handle = server.handle();

        let thread_launched = Arc::new(AtomicBool::new(false));
        let thread_launched_clone = thread_launched.clone();

        let thread_handle = thread::spawn(move || {
            println!("Server thread launched");

            let mut io = IO::new(256).unwrap();

            thread_launched_clone.store(true, Ordering::SeqCst);

            server.run(&mut io).unwrap();

            println!("Server thread closed");
        });

        while !thread_launched.load(Ordering::SeqCst) {}

        (server_handle, thread_handle)
    }

    #[test]
    fn set_value() -> Result<(), Box<dyn std::error::Error>> {
        let address = SocketAddr::from_str("127.0.0.1:10001")?;

        let (server_handle, thread_handle) = launch_server(address);

        println!("Connecting to server on {}", address);
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
        let mut client = Client::new(stream);

        let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
        let response = client.send(get_cmd)?;

        assert_eq!(Value::Null, response);

        let set_cmd = Command::Set {
            key: Cow::Owned("Key".to_string()),
            value: Cow::Owned("Value".to_string()),
            overwrite_rule: None,
            get: false,
            expire_rule: None,
        };
        let response = client.send(set_cmd)?;
        assert_eq!(Value::ok(), response);

        let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
        let response = client.send(get_cmd)?;
        assert_eq!(Value::BulkString(String::from("Value")), response);

        server_handle.store(true, Ordering::SeqCst);
        thread_handle.join().unwrap();

        Ok(())
    }
    #[test]
    fn set_value_batch() -> Result<(), Box<dyn std::error::Error>> {
        let address = SocketAddr::from_str("127.0.0.1:10002")?;

        let (server_handle, thread_handle) = launch_server(address);

        println!("Connecting to server on {}", address);
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
        let mut client = Client::new(stream);

        let cmd_batch = vec![
            Command::Set {
                key: Cow::Owned("Key1".to_string()),
                value: Cow::Owned("Value1".to_string()),
                overwrite_rule: None,
                get: false,
                expire_rule: None,
            },
            Command::Set {
                key: Cow::Owned("Key2".to_string()),
                value: Cow::Owned("Value2".to_string()),
                overwrite_rule: None,
                get: false,
                expire_rule: None,
            },
            Command::Get(Cow::Owned(String::from("Key1"))),
            Command::Get(Cow::Owned(String::from("Key2"))),
        ];
        let mut response = client.send_batch(cmd_batch)?;
        assert_eq!(4, response.len());
        let first_result = response.remove(0);
        assert_eq!(Value::ok(), first_result);
        let second_result = response.remove(0);
        assert_eq!(Value::ok(), second_result);
        let third_result = response.remove(0);
        assert_eq!(Value::BulkString(String::from("Value1")), third_result);
        let fourth_result = response.remove(0);
        assert_eq!(Value::BulkString(String::from("Value2")), fourth_result);

        server_handle.store(true, Ordering::SeqCst);
        thread_handle.join().unwrap();

        Ok(())
    }

    #[test]
    fn set_large_value() -> Result<(), Box<dyn std::error::Error>> {
        let address = SocketAddr::from_str("127.0.0.1:10003")?;

        let (server_handle, thread_handle) = launch_server(address);

        println!("Connecting to server on {}", address);
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
        let mut client = Client::new(stream);

        let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
        let response = client.send(get_cmd)?;

        assert_eq!(Value::Null, response);

        let mut large_value = String::new();
        for i in 0..BUFFER_SIZE * 1000 {
            large_value.push(char::from_digit((i % 10) as u32, 10).unwrap())
        }

        let set_cmd = Command::Set {
            key: Cow::Owned("Key".to_string()),
            value: Cow::Owned(large_value.clone()),
            overwrite_rule: None,
            get: false,
            expire_rule: None,
        };
        let response = client.send(set_cmd)?;
        assert_eq!(Value::ok(), response);

        let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
        let response = client.send(get_cmd)?;
        assert_eq!(Value::BulkString(String::from(large_value)), response);

        server_handle.store(true, Ordering::SeqCst);
        thread_handle.join().unwrap();

        Ok(())
    }
}
