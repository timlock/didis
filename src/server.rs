use std::{
    collections::HashMap,
    io::{self, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream}

    ,
};
use std::io::BufRead;
use crate::server::listener::TcpListenerNonBlocking;
use crate::server::socket::SocketNonBlocking;

mod listener;
mod socket;

pub struct Server {
    listener: TcpListenerNonBlocking,
    pub connections: HashMap<SocketAddr, BufReader<SocketNonBlocking>>,
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
                    self.connections.insert(address, BufReader::new(stream));
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn disconnect(&mut self, client_address: SocketAddr) {
        self.connections.remove(&client_address);
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