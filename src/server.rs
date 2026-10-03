use crate::async_io::{AsyncIO, Completion};
use crate::controller::Controller;
use crate::parser::command::Parser;
use crate::parser::resp::Value;
use log::{error, info, warn};
use std::cmp::min;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};
use std::{
    collections::HashMap,
    fs,
    io::{self},
    net::SocketAddr,
};

pub const BUFFER_SIZE: usize = 4096;

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
            controller: Controller::new(SystemTime::now()),
            done: Arc::new(AtomicBool::new(false)),
            id_counter: 0,
        }
    }

    pub fn run(&mut self, io: &mut impl AsyncIO) -> io::Result<()> {
        if fs::exists("save.rdb")? {
            info!("Loading save.rdb file");
            if let Err(err) = self.controller.restore_from_snapshot("save.rdb") {
                error!("Failed to restore database from save.rdb {err}");
                return Ok(());
            }

            info!("Restored database from save.rdb");
        }

        info!("Server starts listening on {}", self.address);

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
                        Ok(sent) => self.handle_send(stream, buf, sent, client_id),
                        Err(err) => {
                            self.connections.remove(&client_id);
                            self.controller.remove_client(&client_id);
                            info!("Closed connection {stream:?}: {err}");
                        }
                    },
                    Completion::Receive(stream, buf, result, client_id) => match result {
                        Ok(received) => self.handle_receive(stream, buf, received, client_id)?,
                        Err(err) => {
                            self.connections.remove(&client_id);
                            self.controller.remove_client(&client_id);
                            warn!("Closed connection {stream:?} IO error: {err}");
                        }
                    },
                }
            }

            if self.controller.has_messages() {
                self.publish_messages()?;
            }

            for connection in self.connections.values_mut() {
                connection.flush(io)?;
            }

            self.controller.do_jobs()?;

            if self.done.load(Ordering::SeqCst) {
                info!("Server stopped");
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
        let (stream, _) = result?;

        let client_id = self.id_counter;
        self.id_counter += 1;
        info!("New client connected with id {}", client_id);

        let buffer_in = Box::new([0u8; BUFFER_SIZE]);
        let buffer_out = Box::new([0u8; BUFFER_SIZE]);

        let client = Connection::new(client_id, stream.try_clone()?, None, Some(buffer_out));
        self.connections.insert(client_id, client);

        io.receive(stream, buffer_in, client_id);
        Ok(())
    }

    fn handle_send(
        &mut self,
        stream: TcpStream,
        buffer_out: Box<[u8]>,
        sent: usize,
        client_id: u64,
    ) {
        let connection = self.connections.get_mut(&client_id).unwrap_or_else(|| {
            panic!(
                "Send data to unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            )
        });

        info!(
            "Sent {} bytes to client, remaining bytes {}",
            sent, connection.buffer_out_len
        );

        connection.handle_sent(sent, buffer_out);
    }

    fn handle_receive(
        &mut self,
        stream: TcpStream,
        buffer_in: Box<[u8]>,
        received: usize,
        client_id: u64,
    ) -> io::Result<()> {
        let connection = self.connections.get_mut(&client_id).unwrap_or_else(|| {
            panic!(
                "Received data from unknown socket with file descriptor: {}",
                stream.as_raw_fd()
            )
        });

        if received == 0 {
            self.connections.remove(&client_id);
            self.controller.remove_client(&client_id);
            info!("Closed connection {stream:?}");
            return Ok(());
        }

        info!("Received {} bytes from client", received);

        let commands = connection.command_parser.parse_all(&buffer_in[..received]);
        for command in commands {
            let response = match command {
                Ok(command) => {
                    let start = Instant::now();
                    info!("Received command: {}", command);
                    if let Some(response) = self.controller.handle_command(connection.id, command) {
                        info!("Sending response {response} took {:?}", start.elapsed());

                        response.to_bytes()
                    } else {
                        info!("Took {:?}", start.elapsed());
                        Default::default()
                    }
                }
                Err(err) => {
                    warn!("Received faulty command: {:?}", err);
                    Value::SimpleError(err.to_string()).to_bytes()
                }
            };
            connection.send(response);
        }
        connection.buffer_in = Some(buffer_in);

        Ok(())
    }

    fn publish_messages(&mut self) -> io::Result<()> {
        let messages = self.controller.messages();
        for message in messages {
            let bytes = message.value.to_bytes();

            for receiver_id in &message.receivers {
                if let Some(connection) = self.connections.get_mut(receiver_id) {
                    connection.send(bytes.clone());
                }
            }
        }

        Ok(())
    }
}

struct Connection {
    id: u64,
    remaining_out: Vec<u8>,
    buffer_out_len: usize,
    command_parser: Parser,
    buffer_in: Option<Box<[u8]>>,
    buffer_out: Option<Box<[u8]>>,
    tcp_stream: TcpStream,
}

impl Connection {
    fn new(
        id: u64,
        tcp_stream: TcpStream,
        buffer_in: Option<Box<[u8]>>,
        buffer_out: Option<Box<[u8]>>,
    ) -> Self {
        Self {
            id,
            tcp_stream,
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

    fn send(&mut self, mut response: Vec<u8>) {
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

    fn flush(&mut self, io: &mut dyn AsyncIO) -> io::Result<()> {
        if self.buffer_out_len > 0
            && let Some(buffer_out) = self.buffer_out.take()
        {
            io.send(
                self.tcp_stream.try_clone()?,
                buffer_out,
                self.buffer_out_len,
                self.id,
            );
        } else if self.buffer_out_len == 0
            && let Some(buffer_in) = self.buffer_in.take()
        {
            io.receive(self.tcp_stream.try_clone()?, buffer_in, self.id);
        }
        Ok(())
    }
}
