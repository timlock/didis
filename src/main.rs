use didis::async_io::{AsyncIO, Completion, IO};
use didis::controller::Controller;
use didis::dictionary::Dictionary;
use didis::parser::command;
use didis::parser::command::Parser;
use didis::parser::resp::{Resp, RespRef};
use didis::server::{AsyncServer, Server};
use std::borrow::Cow;
use std::cmp::min;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::Duration;

const QUEUE_DEPTH: usize = 256;
const BUFFER_SIZE: usize = 4096;

fn main() -> Result<(), Box<dyn Error>> {
    let address = "127.0.0.1:6379";
    // let server = Server::new(address)?;
    let worker = Controller::new(Dictionary::new());

    println!("Starting server on {}", address);
    // let result = run(server, worker);

    let io = IO::new(QUEUE_DEPTH)?;
    let mut server = AsyncServer::new(io);
    let result = server.run(address);
    // let result = run_async(address, worker);
    println!("Server stopped");
    result.map_err(|e| e.into())
}

fn run(mut server: Server, mut controller: Controller) -> Result<(), io::Error> {
    loop {
        server.accept_connections()?;

        let mut disconnected = Vec::new();
        for (address, connection) in server.connections.iter_mut() {
            loop {
                match connection.incoming.next() {
                    Some(Ok(command)) => {
                        println!("Received command: {:?}", command);
                        let response = controller.handle_command(command);
                        println!("Sending response {response}");
                        let serialized = (response).to_bytes();
                        if let Err(err) = connection.outgoing.write_all(&serialized) {
                            disconnected.push(address.clone());
                            eprintln!("{err}");
                            break;
                        }
                    }
                    Some(Err(err)) => {
                        if let command::Error::Io(err) = err {
                            eprintln!("Closed connection {address} due to IO error: {err}");
                            disconnected.push(address.clone());
                            break;
                        }

                        println!("Closed connection {address}: {err}");
                        let resp_error = Resp::SimpleError(err.to_string());
                        if let Err(err) = connection.outgoing.write_all(&Vec::from(resp_error)) {
                            disconnected.push(address.clone());
                            eprintln!("{err}");
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        for address in disconnected {
            server.connections.remove(&address);
        }
    }
}

fn run_async(address: &str, mut controller: Controller) -> Result<(), io::Error> {
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;

    let mut io = IO::new(QUEUE_DEPTH)?;
    io.accept(listener);

    let mut connections = HashMap::new();

    loop {
        for completion in io.poll_timeout(Duration::from_secs(1))? {
            match completion {
                Completion::Accept(listener, result) => {
                    let (stream, address) = result?;
                    println!["New client connected with address {}", address];
                    let cloned = stream.try_clone()?;
                    let client = Connection::new(cloned, address);
                    connections.insert(stream.as_raw_fd(), client);

                    let buf = Box::new([0u8; BUFFER_SIZE]);
                    io.receive(stream, buf);

                    io.accept(listener);
                }
                Completion::Send(stream, mut buf, result) => {
                    let connection = connections.get_mut(&stream.as_raw_fd()).expect(
                        format![
                            "Send data to unknown socket with file descriptor: {}",
                            stream.as_raw_fd()
                        ]
                        .as_str(),
                    );
                    match result {
                        Ok(sent) => {
                            connection.handle_sent(sent, buf.as_mut());
                            if connection.to_send > 0 {
                                io.send(stream, buf, connection.to_send)
                            } else {
                                io.receive(stream, buf)
                            }
                        }
                        Err(err) => {
                            connections.remove(&stream.as_raw_fd());
                            println!("Closed connection {stream:?}: {err}");
                        }
                    }
                }
                Completion::Receive(stream, mut buf, res) => {
                    let connection = connections.get_mut(&stream.as_raw_fd()).expect(
                        format![
                            "Received data from unknown socket with file descriptor: {}",
                            stream.as_raw_fd()
                        ]
                        .as_str(),
                    );
                    match res {
                        Ok(0) => {
                            connections.remove(&stream.as_raw_fd());
                            println!("Closed connection {stream:?}");
                        }
                        Ok(len) => {
                            let (commands, read) = connection.command_parser.parse_all(&buf[..len]);
                            connection.remaining_in.extend(&buf[read..len]);

                            for command in commands {
                                let bytes = match command {
                                    Ok(command) => {
                                        println!("Received command: {:?}", command);
                                        let response = controller.handle_command(command);
                                        println!("Sending response {response}");

                                        response.to_bytes()
                                    }
                                    Err(err) => {
                                        eprintln!("Received faulty command: {:?}", err);
                                        let resp_err =
                                            RespRef::SimpleError(Cow::Owned(err.to_string()));

                                        resp_err.to_bytes()
                                    }
                                };

                                if connection.to_send + bytes.len() > buf.len() {
                                    connection.remaining_out.extend(bytes);
                                } else {
                                    buf[connection.to_send..(connection.to_send + bytes.len())]
                                        .copy_from_slice(bytes.as_slice());
                                    connection.to_send += bytes.len();
                                }
                            }

                            io.send(stream, buf, connection.to_send);
                        }
                        Err(err) => {
                            connections.remove(&stream.as_raw_fd());
                            println!("Closed connection {stream:?} IO error: {err}");
                        }
                    }
                }
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
