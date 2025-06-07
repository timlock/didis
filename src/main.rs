use didis::async_io::IO;
use didis::server::Server;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;

fn main() -> Result<(), Box<dyn Error>> {
    let io = IO::new(256)?;
    let mut server = Server::new(io);

    let address = SocketAddr::from_str("127.0.0.1:6379")?;

    server.run(address, Vec::new()).map_err(|err| err.into())
}