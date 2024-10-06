use std::{
    collections::HashMap,
    io::{self, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream}

    ,
};
use crate::server::listener::TcpListenerNonBlocking;
use crate::server::socket::SocketNonBlocking;
use crate::parser::{command, resp};

mod listener;
mod socket;

pub struct Server {
    listener: TcpListenerNonBlocking,
    pub connections: HashMap<SocketAddr, command::Decoder<SocketNonBlocking>>,
}

impl Server {
    pub fn new(address: &str) -> io::Result<Self> {
        let listener = TcpListenerNonBlocking::bind(address)?;
        Ok(Server {
            listener,
            connections: HashMap::new(),
        })
    }

    pub fn accept_connections(&mut self) -> io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok(None) => return Ok(()),
                Ok(Some((stream, address))) => {
                    println!("new connection: {address}");
                    let resp_decoder = resp::Decoder::new(stream);
                    let command_decoder = command::Decoder::new(resp_decoder);
                    self.connections.insert(address, command_decoder);
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn disconnect(&mut self, client_address: SocketAddr) {
        self.connections.remove(&client_address);
    }
}