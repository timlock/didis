use didis::controller::Controller;
use didis::dictionary::Dictionary;
use didis::parser;
use didis::server::Server;
use std::io::{self, Write};

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
        for (client_address, socket) in server.connections.iter_mut() {
            loop {
                match socket.next() {
                    Some(Ok(command)) => {
                        let response = controller.handle_command(command);
                        // println!("Sending response {response}");
                        let serialized = Vec::from(response);
                        if let Err(err) = socket.get_mut().write_all(&serialized) {
                            disconnected.push(client_address.clone());
                            println!("{err}");
                            break;
                        }
                    }
                    Some(Err(parser::Error::Io(err))) => {
                        disconnected.push(client_address.clone());
                        println!("Closed");
                        break;
                    }
                    Some(Err(err)) => {
                        if let Err(err) = socket.get_mut().write_all(err.to_string().as_bytes()) {
                            disconnected.push(client_address.clone());
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
