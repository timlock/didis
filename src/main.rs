use didis::async_io::runtime::Runtime;
use didis::logger;
use didis::server::Server;
use log::Level;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use std::thread;

fn main() -> Result<(), Box<dyn Error>> {
    logger::init(Level::Info)?;

    // TODO multithreaded is slower than single threaded
    let threads = thread::available_parallelism()?.get() - 1;
    // let threads = 0;

    let runtime = Runtime::new(256, threads)?;
    runtime.block_on(async {
        let address = SocketAddr::from_str("127.0.0.1:6379")?;

        let mut server = Server::new(address);

        server.run().await?;
        Ok(())
    })
    // TODO handle signals
}
