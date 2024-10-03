use std::{
    collections::HashMap,
    io::{self, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
    thread::{self, JoinHandle},
};

use crate::{command::Command, resp::Resp, worker::Worker};

pub struct ServerThread {
    server: Option<Server>,
    sender: Option<Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl ServerThread {
    pub fn new(server: Server) -> Self {
        Self {
            server: Some(server),
            sender: None,
            join_handle: None,
        }
    }
    pub fn start(&mut self) {
        if let Some(mut server) = self.server.take() {
            let (sender, receiver) = mpsc::channel();
            self.sender = Some(sender);
            self.join_handle = Some(thread::spawn(move || server.start(receiver)));
            println!("started server");
        }
    }
}

impl Drop for ServerThread {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(join_handle) = self.join_handle.take() {
            join_handle.join().unwrap();
            println!("stopped server");
        }
    }
}

pub struct Server {
    listener: TcpListener,
    connections: HashMap<SocketAddr, BufReader<TcpStream>>,
    worker: Worker,
}

impl Server {
    pub fn new(address: &str, worker: Worker) -> io::Result<Self> {
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        Ok(Server {
            listener,
            connections: HashMap::new(),
            worker,
        })
    }
    pub fn start(&mut self, receiver: Receiver<()>) {
        loop {
            if let Err(mpsc::TryRecvError::Disconnected) = receiver.try_recv() {
                break;
            }
            let result = try_accept(&self.listener);
            if let Some((stream, address)) = result {
                println!("new connection: {address}");
                stream.set_nonblocking(true).unwrap();
                self.connections.insert(address, BufReader::new(stream));
            }
            let mut disconnected = Vec::new();
            for (address, stream) in self.connections.iter_mut() {
                match try_read(stream) {
                    Ok(bytes) => match parse(&bytes) {
                        Ok(commands) => {
                            for command in commands {
                                println!("Received command {command:?}");
                                let response = self.worker.handle_command(command);
                                println!("Sending response {response}");
                                let serialized = Vec::from(response);
                                if let Err(err) = stream.get_mut().write_all(&serialized) {
                                    println!("{err}");
                                }
                            }
                        }
                        Err(err) => {
                            disconnected.push(*address);
                            println!("{err}");
                        }
                    },
                    Err(err) => {
                        disconnected.push(*address);
                        println!("{err}");
                    }
                }
            }
            for address in disconnected {
                self.connections.remove(&address);
            }
        }
    }
}

fn try_accept(listener: &TcpListener) -> Option<(TcpStream, SocketAddr)> {
    listener
        .accept()
        .map_err(|err| {
            if err.kind() != io::ErrorKind::WouldBlock {
                println!("{err}")
            }
        })
        .ok()
}

struct RespStream {
    stream: BufReader<TcpStream>,
    buffer: [u8; 1024],
}

impl RespStream {
    fn new(stream: TcpStream) -> Self {
        Self { stream: BufReader::new(stream), buffer: [0; 1024] }
    }
    fn next(&mut self) -> io::Result<Result<Vec<Resp>, Resp>> {
        let n = self.stream.read(&mut self.buffer)?;

        Ok(Ok(Vec::new()))
    }
}

//TODO check if data is incomplete buffer incomplete data and discard corrupted data
fn parse(data: &[u8]) -> Result<Vec<Command>, Resp> {
    let resps = Resp::parse(data)?;
    let mut commands = Vec::new();
    for resp in resps {
        let command = Command::try_from(resp)?;
        commands.push(command);
    }
    Ok(commands)
}

fn try_read(buf_reader: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    const CHUNK_SIZE: usize = 1024;
    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    let mut buf = [0; CHUNK_SIZE];
    loop {
        match buf_reader.read(&mut buf) {
            Ok(size) if size == 0 => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Connection closed",
                ));
            }
            Ok(size) => {
                buffer.extend_from_slice(&buf[..size]);
                if size < CHUNK_SIZE {
                    break;
                }
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }
    Ok(buffer)
}

mod tests {
    use std::error::Error;

    use redis::{Commands, RedisError};

    use crate::dictionary::Dictionary;

    use super::*;

    struct TestCase {
        command: Box<dyn FnOnce(&mut redis::Connection) -> redis::RedisResult<redis::Value>>,
        want: redis::Value,
    }

    #[test]
    fn set_value() -> Result<(), Box<dyn Error>> {
        let tests = HashMap::from([
            (
                "Happypath",
                vec![
                    TestCase {
                        command: Box::new(|connection: &mut redis::Connection| {
                            connection.set("test", "value")
                        }),
                        want: redis::Value::Okay,
                    },
                    TestCase {
                        command: Box::new(|connection: &mut redis::Connection| {
                            connection.get("test")
                        }),
                        want: redis::Value::SimpleString(b"value".into()),
                    },
                ],
            ),
            (
                "unkown key",
                vec![TestCase {
                    command: Box::new(|connection: &mut redis::Connection| connection.get("test")),
                    want: redis::Value::Nil,
                }],
            ),
        ]);
        for (name, commands) in tests {
            println!("Test: {name}");
            let address = "127.0.0.1:6379";
            let mut server =
                ServerThread::new(Server::new(address, Worker::new(Dictionary::new()))?);
            server.start();
            let address = "redis://127.0.0.1:6379";
            let client = redis::Client::open(address)?;
            let mut connection = client.get_connection()?;

            for command in commands {
                let resp = (command.command)(&mut connection)?;
                assert_eq!(resp, command.want, "assertion failed for test: {name}");
            }
        }
        Ok(())
    }
}
