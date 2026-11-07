use crate::async_io::buf::BufWriter;
use crate::async_io::mpsc::{Receiver, Sender, channel};
use crate::async_io::runtime::{
    SelectTripleResult, select_triple, spawn,
};
use crate::async_io::slice::{AsSlice, AsSliceMut};
use crate::async_io::{AsyncAccept, AsyncRead, AsyncWrite};
use crate::command::{Command, Parser};
use crate::controller::Controller;
use crate::pubsub::Message;
use crate::resp::Value;
use log::{error, info, warn};
use std::net::{TcpListener, TcpStream};
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
    pub_sub_receiver: Receiver<Message>,
    stop_sender: Sender<()>,
    stop_receiver: Receiver<()>,
    is_listening_sender: Option<Sender<()>>,
    id_counter: u64,
}

impl Server {
    pub fn new(address: SocketAddr) -> Self {
        let (stop_sender, stop_receiver) = channel();

        let (controller, pub_sub_receiver) = Controller::new(SystemTime::now());

        Self {
            address,
            connections: Default::default(),
            controller: Arc::new(Mutex::new(controller)),
            pub_sub_receiver,
            is_listening_sender: None,
            stop_sender,
            stop_receiver,
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
            match select_triple(
                listener.async_accept(),
                self.stop_receiver.recv(),
                self.pub_sub_receiver.recv(),
            )
            .await
            {
                SelectTripleResult::A(accept_result) => {
                    let (stream, address) = accept_result?;
                    self.handle_connection(stream, address);
                }
                SelectTripleResult::B(Err(RecvError)) => {
                    warn!("server handle got dropped");
                }
                SelectTripleResult::B(Ok(())) => {
                    info!("Server stopped");
                    return Ok(());
                }
                SelectTripleResult::C(Err(RecvError)) => {
                    warn!("pub sub sender got dropped");
                }
                SelectTripleResult::C(Ok(message)) => {
                    self.publish_messages(message).await?;
                }
            };

            self.controller.lock().unwrap().do_jobs()?;
        }
    }

    pub fn handle(&mut self) -> ServerHandle {
        let (tx, rx) = channel();
        self.is_listening_sender = Some(tx);

        ServerHandle {
            is_listening: false,
            is_listening_receiver: rx,
            stop_sender: self.stop_sender.clone(),
        }
    }

    fn handle_connection(&mut self, stream: TcpStream, address: SocketAddr) {
        let controller = self.controller.clone();
        let client_id = self.id_counter;
        self.id_counter += 1;

        info!("New client connected with id {}", client_id);

        spawn(async move {
            let mut buffer_in = vec![0u8; BUFFER_SIZE];

            let mut client = Connection::new(client_id, stream);

            loop {
                let (received, buffer) = client
                    .buf_writer
                    .get_mut()
                    .async_read(buffer_in.slice_mut(..))
                    .await;
                let received = received?;
                if received == 0 {
                    info!("Closed connection {address:?}");
                    return io::Result::Ok(());
                }
                buffer_in = buffer.into_inner();

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

                                Some(response.to_bytes())
                            } else {
                                info!("Took {:?}", start.elapsed());
                                None
                            }
                        }
                        Err(err) => {
                            warn!("Received faulty command: {:?}", err);
                            Some(Value::SimpleError(err.to_string()).to_bytes())
                        }
                    };
                    if let Some(response) = response {
                        let (result, buf) =
                            client.buf_writer.async_write_all(response.slice(..)).await;
                        result?;
                    }
                }
                client.buf_writer.flush().await?;
            }
        });
    }

    async fn publish_messages(&mut self, message: Message) -> io::Result<()> {
        let mut bytes = message.value.to_bytes().slice(..);

        for receiver_id in &message.receivers {
            if let Some(connection) = self.connections.get_mut(receiver_id) {
                let (result, buf) = connection.buf_writer.async_write_all(bytes).await;
                bytes = buf;
                result?;
            }
        }

        Ok(())
    }
}

pub struct ServerHandle {
    stop_sender: Sender<()>,
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
        self.stop_sender.send(()).unwrap(); // TODO requires receiver to be Send check if the pattern makes sense
    }
}

struct Connection {
    id: u64,
    command_parser: Parser,
    buf_writer: BufWriter<TcpStream>,
}

impl Connection {
    fn new(id: u64, tcp_stream: TcpStream) -> Self {
        Self {
            id,
            buf_writer: BufWriter::new(tcp_stream),
            command_parser: Default::default(),
        }
    }
}
