use std::io::Write;
use didis::dictionary::Dictionary;
use didis::parser;
use didis::server::Server;
use didis::controller::Controller;

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
            match parser::try_read(socket) {
                Ok(data) => {
                    match parser::parse(&data) {
                        Ok(commands) => {
                            for command in commands {
                                // println!("Received command {command:?}");
                                let response = controller.handle_command(command);
                                // println!("Sending response {response}");
                                let serialized = Vec::from(response);
                                if let Err(err) = socket.get_mut().write_all(&serialized) {
                                    disconnected.push(client_address.clone());
                                    println!("{err}");
                                }
                            }
                        }
                        Err(err_message) => {
                            let serialized = Vec::from(err_message);
                            if let Err(err) = socket.get_mut().write_all(&serialized) {
                                disconnected.push(client_address.clone());
                                println!("{err}");
                            }
                        }
                    }
                }
                Err(err) => {
                    disconnected.push(client_address.clone());
                    println!("{err}");
                }
            }
        }

        for address in disconnected{
            server.connections.remove(&address);
        }
    }
}
