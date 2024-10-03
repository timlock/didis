use std::sync::mpsc;

use didis::dictionary::Dictionary;
use didis::server::Server;
use didis::worker::Worker;

fn main() -> Result<(), std::io::Error> {
    let address = "127.0.0.1:6379";
    let mut server = Server::new(address, Worker::new(Dictionary::new()))?;
    let (sender, receiver) = mpsc::channel();
    server.start(receiver);
    Ok(())
}
