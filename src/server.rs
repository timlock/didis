use crate::async_io::{AsyncIO, Completion, IO};
use crate::controller::Controller;
use crate::parser::command::Parser;
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
    connections: HashMap<u64, Connection>,
    controller: Controller,
    done: Arc<AtomicBool>,
    id_counter: u64,
}

impl Server {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            connections: Default::default(),
            controller: Default::default(),
            done: Arc::new(AtomicBool::new(false)),
            id_counter: 0,
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
                    Completion::Send(stream, buf, result, client_id) => match result {
                        Ok(sent) => self.handle_send(io, stream, buf, sent,client_id),
                        Err(err) => {
                            self.connections.remove(&client_id);
                            println!("Closed connection {stream:?}: {err}");
                        }
                    },
                    Completion::Receive(stream, buf, result,client_id) => match result {
                        Ok(received) => self.handle_receive(io, stream, buf, received, client_id),
                        Err(err) => {
                            self.connections.remove(&client_id);
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

        let client_id = self.id_counter;
        self.id_counter += 1;
        println!["New client connected with id {}", client_id];

        let buffer_in = Box::new([0u8; BUFFER_SIZE]);
        let buffer_out = Box::new([0u8; BUFFER_SIZE]);

        let client = Connection::new(client_id, None, Some(buffer_out));
        self.connections.insert(client_id, client);

        io.receive(stream, buffer_in, client_id);
        Ok(())
    }

    fn handle_send(
        &mut self,
        io: &mut impl AsyncIO,
        stream: TcpStream,
        buffer_out: Box<[u8]>,
        sent: usize,
        client_id: u64
    ) {
        let connection = self.connections.get_mut(&client_id).expect(
            format![
                "Send data to unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            ]
            .as_str(),
        );

        println!(
            "Sent {} bytes to client, remaining bytes {}",
            sent, connection.buffer_out_len
        );

        connection.handle_sent(sent, buffer_out);
        if connection.buffer_out_len > 0
            && let Some(buffer_out) = connection.buffer_out.take()
        {
            io.send(stream, buffer_out, connection.buffer_out_len, client_id);
            return;
        }

        if let Some(buffer_in) = connection.buffer_in.take() {
            io.receive(stream, buffer_in, client_id);
        }
    }

    fn handle_receive(
        &mut self,
        io: &mut impl AsyncIO,
        stream: TcpStream,
        buffer_in: Box<[u8]>,
        received: usize,
        client_id: u64,
    ) {
        let connection = self.connections.get_mut(&client_id).expect(
            format![
                "Received data from unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            ]
            .as_str(),
        );

        if received == 0 {
            self.connections.remove(&client_id);
            println!("Closed connection {stream:?}");
            return;
        }

        println!("Received {} bytes from client", received);

        let commands = connection.command_parser.parse_all(&buffer_in[..received]);
        for command in commands {
            let response = match command {
                Ok(command) => {
                    println!("Received command: {}", command);
                    if let Some(response) = self.controller.handle_command(connection.id, command) {
                        println!("Sending response {response}");

                        response.to_bytes()
                    } else {
                        Default::default()
                    }
                }
                Err(err) => {
                    println!("Received faulty command: {:?}", err);
                    Value::SimpleError(err.to_string()).to_bytes()
                }
            };
            connection.fill_buffer_out(response);
        }
        connection.buffer_in = Some(buffer_in);

        if connection.buffer_out_len > 0
            && let Some(buffer_out) = connection.buffer_out.take()
        {
            io.send(stream, buffer_out, connection.buffer_out_len, client_id);
            return;
        }

        if let Some(buffer_in) = connection.buffer_in.take() {
            io.receive(stream, buffer_in, client_id);
        }
    }
}

struct Connection {
    id: u64,
    remaining_out: Vec<u8>,
    buffer_out_len: usize,
    command_parser: Parser,
    buffer_in: Option<Box<[u8]>>,
    buffer_out: Option<Box<[u8]>>,
}

impl Connection {
    fn new(id: u64, buffer_in: Option<Box<[u8]>>, buffer_out: Option<Box<[u8]>>) -> Self {
        Self {
            id,
            remaining_out: Default::default(),
            buffer_out_len: 0,
            command_parser: Default::default(),
            buffer_in,
            buffer_out,
        }
    }

    fn handle_sent(&mut self, sent: usize, mut buffer_out: Box<[u8]>) {
        if self.buffer_out_len > 0 {
            buffer_out.copy_within(sent..self.buffer_out_len, 0);
            self.buffer_out_len -= sent;
        }

        if !self.remaining_out.is_empty() && self.buffer_out_len < buffer_out.len() {
            let to_copy = min(
                buffer_out.len() - self.buffer_out_len,
                self.remaining_out.len(),
            );
            let copy_range = self.buffer_out_len..(self.buffer_out_len + to_copy);

            buffer_out[copy_range].copy_from_slice(self.remaining_out.drain(..to_copy).as_slice());

            self.buffer_out_len += to_copy;
        }

        self.buffer_out = Some(buffer_out);
    }

    fn fill_buffer_out(&mut self, mut response: Vec<u8>) {
        match &mut self.buffer_out {
            Some(buffer) => {
                let to_send = min(buffer.len() - self.buffer_out_len, response.len());

                buffer[self.buffer_out_len..self.buffer_out_len + to_send]
                    .copy_from_slice(response.drain(..to_send).as_slice());
                self.remaining_out.extend_from_slice(response.as_slice());
                self.buffer_out_len += to_send;
            }
            None => self.remaining_out.extend_from_slice(response.as_slice()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::client::Client;
    use crate::parser::command::{Command, OverwriteRule};
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
                key: Cow::Borrowed("Key1"),
                value: Cow::Borrowed("Value1"),
                overwrite_rule: None,
                get: false,
                expire_rule: None,
            },
            Command::Set {
                key: Cow::Borrowed("Key2"),
                value: Cow::Borrowed("Value2"),
                overwrite_rule: None,
                get: false,
                expire_rule: None,
            },
            Command::Get(Cow::Borrowed("Key1")),
            Command::Get(Cow::Borrowed("Key2")),
            Command::Set {
                key: Cow::Borrowed("Key1"),
                value: Cow::Borrowed("should not be applied"),
                overwrite_rule: Some(OverwriteRule::NotExists),
                get: true,
                expire_rule: None,
            },
            Command::Set {
                key: Cow::Borrowed("Key2"),
                value: Cow::Borrowed("Value2 updated"),
                overwrite_rule: None,
                get: true,
                expire_rule: None,
            },
            Command::Get(Cow::Borrowed("Key2")),
        ];
        let mut response = client.send_batch(cmd_batch)?;
        assert_eq!(7, response.len());
        assert_eq!(Value::ok(), response[0]);
        assert_eq!(Value::ok(), response[1]);
        assert_eq!(Value::BulkString(String::from("Value1")), response[2]);
        assert_eq!(Value::BulkString(String::from("Value2")), response[3]);
        assert_eq!(Value::Null, response[4]);
        assert_eq!(Value::BulkString(String::from("Value2")), response[5]);
        assert_eq!(
            Value::BulkString(String::from("Value2 updated")),
            response[6]
        );

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
