use didis::async_io::IO;
use didis::server::Server;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;

fn main() -> Result<(), Box<dyn Error>> {
    let address = SocketAddr::from_str("127.0.0.1:6379")?;

    let mut server = Server::new(address);

    let io = IO::new(256)?;
    server.run(io)?;
    
    Ok(())
}