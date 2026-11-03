use crate::async_io::mpsc::{Receiver, Sender, channel};
use crate::async_io::net::{AsyncAccept, AsyncReceive, AsyncSend};
use crate::async_io::runtime::spawn;
use crate::command::Parser;
use crate::controller::Controller;
use crate::resp::Value;
use log::{error, info, warn};
use std::cmp::min;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvError;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};
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
    controller: Arc<Mutex<Controller>>,
    should_stop: Arc<AtomicBool>,
    is_listening_sender: Option<Sender<()>>,
    id_counter: u64,
}

impl Server {
    pub fn new(address: SocketAddr) -> Self {
        Self {
            address,
            connections: Default::default(),
            controller: Arc::new(Mutex::new(Controller::new(SystemTime::now()))),
            is_listening_sender: None,
            should_stop: Arc::new(AtomicBool::new(false)),
            id_counter: 0,
        }
    }

    pub async fn run(&mut self) -> io::Result<()> {
        if fs::exists("save.rdb")? {
            info!("Loading save.rdb file");
            if let Err(err) = self
                .controller
                .lock()
                .unwrap()
                .restore_from_snapshot("save.rdb")
            {
                error!("Failed to restore database from save.rdb {err}");
                return Ok(());
            }

            info!("Restored database from save.rdb");
        }

        info!("Server starts listening on {}", self.address);

        let listener = TcpListener::bind(self.address)?;
        listener.set_nonblocking(true)?;

        if let Some(sender) = self.is_listening_sender.as_ref()
            && let Err(err) = sender.send(())
        {
            warn!("Could not signal observers that server is listening: {err}");
        }

        loop {
            let (stream, address) = listener.async_accept().await?;

            let client_id = self.id_counter;
            self.id_counter += 1;
            let controller = self.controller.clone();

            info!("New client connected with id {}", client_id);

            spawn(async move {
                let mut buffer_in: Box<[u8]> = Box::new([0u8; BUFFER_SIZE]);
                let buffer_out: Box<[u8]> = Box::new([0u8; BUFFER_SIZE]);

                let mut client =
                    Connection::new(client_id, stream.try_clone()?, None, Some(buffer_out));

                loop {
                    let (received, buffer) = client.tcp_stream.async_receive(buffer_in).await;
                    let received = received?;
                    if received == 0 {
                        info!("Closed connection {stream:?}");
                        return io::Result::Ok(());
                    }
                    buffer_in = buffer;

                    info!("Received {} bytes from client", received);

                    let commands = client.command_parser.parse_all(&buffer_in[..received]);

                    for command in commands {
                        let response = match command {
                            Ok(command) => {
                                let start = Instant::now();
                                info!("Received command: {}", command);
                                if let Some(response) = controller
                                    .lock()
                                    .unwrap()
                                    .handle_command(client_id, command)
                                {
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
                        client.send(response);
                    }
                    client.flush().await?;
                }
            });

            if self.controller.lock().unwrap().has_messages() {
                self.publish_messages()?;
            }

            self.controller.lock().unwrap().do_jobs()?;

            if self.should_stop.load(Ordering::Relaxed) {
                info!("Server stopped");
                return Ok(());
            }
        }
    }

    pub fn handle(&mut self) -> ServerHandle {
        let (tx, rx) = channel();
        self.is_listening_sender = Some(tx);

        ServerHandle {
            is_listening: false,
            is_listening_receiver: rx,
            should_stop: self.should_stop.clone(),
        }
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

    fn publish_messages(&mut self) -> io::Result<()> {
        let messages = self.controller.lock().unwrap().messages();
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

pub struct ServerHandle {
    should_stop: Arc<AtomicBool>,
    is_listening: bool,
    is_listening_receiver: Receiver<()>,
}

impl ServerHandle {
    pub async fn wait_until_listening(&mut self) -> Result<(), RecvError> {
        if self.is_listening {
            return Ok(());
        }

        self.is_listening_receiver.recv().await?;
        self.is_listening = true;

        Ok(())
    }

    pub fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
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

    async fn flush(&mut self) -> io::Result<()> {
        if self.buffer_out_len > 0
            && let Some(buffer_out) = self.buffer_out.take()
        {
            let (sent, buffer_out) = self
                .tcp_stream
                .async_send(buffer_out, self.buffer_out_len)
                .await;
            self.handle_sent(sent?, buffer_out);
        }
        Ok(())
    }
}
