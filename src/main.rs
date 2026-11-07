use didis::async_io::driver::IO;
use didis::async_io::runtime::Runtime;
use didis::logger;
use didis::server::Server;
use log::Level;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;

fn main() -> Result<(), Box<dyn Error>> {
    let io = IO::new(256)?;
    let runtime = Runtime::new(io);
    runtime.block_on(async {
        logger::init(Level::Info)?;
        let address = SocketAddr::from_str("127.0.0.1:6379")?;

        let mut server = Server::new(address);

        server.run().await?;
        Ok(())
    })
}
