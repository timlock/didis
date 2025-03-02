
use didis::controller::Controller;
use didis::dictionary::Dictionary;
use didis::parser::command;
use didis::parser::resp::Resp;
use didis::server::Server;
use std::io::Write;
use didis::async_io;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    async_io::IO::new()?;
    // return async_io::test_uring();
    let address = "127.0.0.1:6379";
    let server = Server::new(address)?;
    let worker = Controller::new(Dictionary::new());

    println!("Starting server on {}", address);
    let result = run(server, worker);
    println!("Server stopped");
    result.map_err(|e| e.into())
}

fn run(mut server: Server, mut controller: Controller) -> Result<(), std::io::Error> {
    loop {
        server.accept_connections()?;

        let mut disconnected = Vec::new();
        for (address, connection) in server.connections.iter_mut() {
            loop {
                match connection.incoming.next() {
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
                    Some(Err(err)) => {
                        if let command::Error::Io(err) = err {
                            println!("Closed connection {address} due to IO error: {err}");
                            disconnected.push(address.clone());
                            break;
                        }

                        println!("Closed connection {address}: {err}");
                        let resp_error = Resp::SimpleError(err.to_string());
                        if let Err(err) = connection.outgoing.write_all(&Vec::from(resp_error)) {
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
