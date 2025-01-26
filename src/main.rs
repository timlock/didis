use didis::controller::Controller;
use didis::dictionary::Dictionary;
use didis::parser;
use didis::server::Server;
use std::io::Write;

fn main() -> Result<(), std::io::Error> {
    let address = "127.0.0.1:6379";
    let server = Server::new(address)?;
    let worker = Controller::new(Dictionary::new());
    run(server, worker)
}

fn run(mut server: Server, mut controller: Controller) -> Result<(), std::io::Error> {
    loop {
        server.accept_connections()?;

        let mut disconnected = Vec::new();
        for (address, connection) in server.connections.iter_mut() {
            loop {
                match connection.commands.next() {
                    Some(Ok(command)) => {
                        println!("Received command: {:?}", command);
                        let response = controller.handle_command(command);
                        println!("Sending response {response}");
                        let serialized = Vec::from(response);
                        if let Err(err) = connection.outgoing.write_all(&serialized) {
                            disconnected.push(address.clone());
                            println!("{err}");
                            break;
                        }
                    }
                    Some(Err(parser::Error::Io(err))) => {
                        println!("Closed connection {address} due to IO error: {err}");
                        disconnected.push(address.clone());
                        break;
                    }
                    Some(Err(err)) => {
                        println!("Closed connection {address}: {err}");
                        if let Err(err) = connection.outgoing.write_all(err.to_string().as_bytes()) {
                            disconnected.push(address.clone());
                            println!("{err}");
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        for address in disconnected {
            server.connections.remove(&address);
        }
    }
}
