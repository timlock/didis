use didis::async_io::IO;
use didis::server::Server;
use std::error::Error;

const QUEUE_DEPTH: usize = 256;
const BUFFER_SIZE: usize = 4096;

fn main() -> Result<(), Box<dyn Error>> {
    let address = "127.0.0.1:6379";

    println!("Starting server on {}", address);

    let io = IO::new(QUEUE_DEPTH)?;
    let mut server = Server::new(io);
    let result = server.run(address);
    println!("Server stopped");
    result.map_err(|e| e.into())
}