use crate::parser::resp::Resp;

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Command {
    Ping,
    Echo(String),
    Get(String),
    Set { key: String, value: String },
    ConfigGet,
    Client,
}

impl TryFrom<Resp> for Command {
    type Error = Resp;

    fn try_from(value: Resp) -> Result<Self, Self::Error> {
        match value {
            Resp::Array(arr) => Command::try_from(arr),
            _ => Err(Resp::unknown_command(value.to_string().as_str())),
        }
    }
}

impl TryFrom<Vec<Resp>> for Command {
    type Error = Resp;

    fn try_from(value: Vec<Resp>) -> Result<Self, Self::Error> {
        create_command(value)
    }
}

fn create_command(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    let name = command_name(&mut arr)?;
    match name.to_uppercase().as_str() {
        "PING" => Ok(Command::Ping),
        "ECHO" => create_echo(arr),
        "GET" => create_get(arr),
        "SET" => create_set(arr),
        "CONFIG" => Ok(Command::ConfigGet),
        "CLIENT" => Ok(Command::Client),
        _ => Err(Resp::unknown_command(&name)),
    }
}

fn create_set(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    if arr.len() != 2 {
        return Err(Resp::wrong_number_of_arguments());
    }
    let key = match arr.remove(0) {
        Resp::BulkString(s) => s,
        _ => return Err(Resp::invalid_arguments()),
    };
    let value = match arr.remove(0) {
        Resp::BulkString(s) => s,
        _ => return Err(Resp::invalid_arguments()),
    };
    Ok(Command::Set { key, value })
}
fn create_get(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    if arr.len() != 1 {
        return Err(Resp::wrong_number_of_arguments());
    }
    match arr.remove(0) {
        Resp::BulkString(s) => Ok(Command::Get(s)),
        _ => Err(Resp::wrong_number_of_arguments()),
    }
}

fn command_name(arr: &mut Vec<Resp>) -> Result<String, Resp> {
    match arr.remove(0) {
        Resp::BulkString(s) => Ok(s),
        _ => return Err(Resp::wrong_number_of_arguments()),
    }
}

fn create_echo(mut arr: Vec<Resp>) -> Result<Command, Resp> {
    if arr.len() != 1 {
        return Err(Resp::wrong_number_of_arguments());
    }
    match arr.remove(0) {
        Resp::BulkString(s) => Ok(Command::Echo(s)),
        _ => Err(Resp::wrong_number_of_arguments()),
    }
}
mod tests {
    use super::*;

    #[test]
    fn parse_ping() -> Result<(), String> {
        let name = String::from("PING");
        let resp = vec![Resp::BulkString(name)];
        let command = Command::try_from(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Ping, command);
        Ok(())
    }
    #[test]
    fn parse_echo() -> Result<(), String> {
        let name = String::from("ECHO");
        let arg = String::from("test");
        let resp = vec![Resp::BulkString(name), Resp::BulkString(arg.clone())];
        let command = Command::try_from(resp).map_err(|err| err.to_string())?;
        assert_eq!(Command::Echo(arg), command);
        Ok(())
    }
}
