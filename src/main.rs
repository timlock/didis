use didis::async_io::IO;
use didis::logger;
use didis::server::Server;
use log::Level;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;

fn main() -> Result<(), Box<dyn Error>> {
    logger::init(Level::Info)?;

    let address = SocketAddr::from_str("127.0.0.1:6379")?;

    let mut server = Server::new(address);

    let mut io = IO::new(256)?;
    server.run(&mut io)?;

    Ok(())
}
