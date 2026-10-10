use didis::async_io::IO;
use didis::client::Client;
use didis::command::{Command, OverwriteRule};
use didis::logger;
use didis::resp::Value;
use didis::server::{BUFFER_SIZE, Server, ServerHandle};
use log::Level;
use std::borrow::Cow;
use std::net::{SocketAddr, TcpStream};
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::{LazyLock, Mutex, Once};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

static INIT: Once = Once::new();

fn initialize() {
    INIT.call_once(|| {
        let _ = logger::init(Level::Info); // returns err when logger is already initialized, can be ignored
    });
}

static PORT: LazyLock<Mutex<u64>> = LazyLock::new(|| Mutex::new(10000));

fn next_port() -> u64 {
    let mut guard = PORT.lock().unwrap();
    guard.add_assign(1);
    *guard
}

fn launch_server() -> (SocketAddr, ServerHandle, JoinHandle<()>) {
    let address = SocketAddr::from_str(format!("127.0.0.1:{}", next_port()).as_str()).unwrap();
    let mut server = Server::new(address);
    let mut server_handle = server.handle();

    let thread_handle = thread::spawn(move || {
        println!("Server thread launched");

        let mut io = IO::new(256).unwrap();

        server.run(&mut io).unwrap();

        println!("Server thread closed");
    });

    server_handle.wait_until_listening().unwrap();

    (address, server_handle, thread_handle)
}

#[test]
fn set_value() -> Result<(), Box<dyn std::error::Error>> {
    initialize();

    let (address, server_handle, thread_handle) = launch_server();

    println!("Connecting to server on {}", address);
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    let mut client = Client::new(stream);

    let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
    let response = client.send(get_cmd)?;

    assert_eq!(Value::Null, response);

    let set_cmd = Command::Set {
        key: Cow::Owned("Key".to_owned()),
        value: Cow::Owned("Value".to_owned()),
        overwrite_rule: None,
        get: false,
        expire_rule: None,
    };
    let response = client.send(set_cmd)?;
    assert_eq!(Value::ok(), response);

    let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
    let response = client.send(get_cmd)?;
    assert_eq!(Value::BulkString(String::from("Value")), response);

    let get_cmd = Command::Exists(vec![Cow::Owned(String::from("Key"))]);
    let response = client.send(get_cmd)?;
    assert_eq!(Value::Integer(1), response);

    let get_cmd = Command::Delete(vec![Cow::Owned(String::from("Key"))]);
    let response = client.send(get_cmd)?;
    assert_eq!(Value::Integer(1), response);

    server_handle.stop();
    thread_handle.join().unwrap();

    Ok(())
}
#[test]
fn set_value_batch() -> Result<(), Box<dyn std::error::Error>> {
    initialize();

    let (address, server_handle, thread_handle) = launch_server();

    println!("Connecting to server on {}", address);
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    let mut client = Client::new(stream);

    let cmd_batch = vec![
        Command::Set {
            key: Cow::Borrowed("Key1"),
            value: Cow::Borrowed("Value1"),
            overwrite_rule: None,
            get: false,
            expire_rule: None,
        },
        Command::Set {
            key: Cow::Borrowed("Key2"),
            value: Cow::Borrowed("Value2"),
            overwrite_rule: None,
            get: false,
            expire_rule: None,
        },
        Command::Get(Cow::Borrowed("Key1")),
        Command::Get(Cow::Borrowed("Key2")),
        Command::Set {
            key: Cow::Borrowed("Key1"),
            value: Cow::Borrowed("should not be applied"),
            overwrite_rule: Some(OverwriteRule::NotExists),
            get: true,
            expire_rule: None,
        },
        Command::Set {
            key: Cow::Borrowed("Key2"),
            value: Cow::Borrowed("Value2 updated"),
            overwrite_rule: None,
            get: true,
            expire_rule: None,
        },
        Command::Get(Cow::Borrowed("Key2")),
    ];
    let response = client.send_batch(cmd_batch)?;
    assert_eq!(7, response.len());
    assert_eq!(Value::ok(), response[0]);
    assert_eq!(Value::ok(), response[1]);
    assert_eq!(Value::BulkString(String::from("Value1")), response[2]);
    assert_eq!(Value::BulkString(String::from("Value2")), response[3]);
    assert_eq!(Value::Null, response[4]);
    assert_eq!(Value::BulkString(String::from("Value2")), response[5]);
    assert_eq!(
        Value::BulkString(String::from("Value2 updated")),
        response[6]
    );

    server_handle.stop();
    thread_handle.join().unwrap();

    Ok(())
}

#[test]
fn set_large_value() -> Result<(), Box<dyn std::error::Error>> {
    initialize();

    let (address, server_handle, thread_handle) = launch_server();

    println!("Connecting to server on {}", address);
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    let mut client = Client::new(stream);

    let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
    let response = client.send(get_cmd)?;

    assert_eq!(Value::Null, response);

    let mut large_value = String::new();
    for i in 0..BUFFER_SIZE * 1000 {
        large_value.push(char::from_digit((i % 10) as u32, 10).unwrap())
    }

    let set_cmd = Command::Set {
        key: Cow::Owned("Key".to_owned()),
        value: Cow::Owned(large_value.clone()),
        overwrite_rule: None,
        get: false,
        expire_rule: None,
    };
    let response = client.send(set_cmd)?;
    assert_eq!(Value::ok(), response);

    let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
    let response = client.send(get_cmd)?;
    assert_eq!(Value::BulkString(String::from(large_value)), response);

    server_handle.stop();
    thread_handle.join().unwrap();

    Ok(())
}

#[test]
fn publish_message() -> Result<(), Box<dyn std::error::Error>> {
    initialize();

    let (address, server_handle, thread_handle) = launch_server();

    println!("Connecting to server on {}", address);
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    let mut client = Client::new(stream);

    let subscribe_cmd = Command::Subscribe(vec![
        Cow::Owned(String::from("first")),
        Cow::Owned(String::from("second")),
    ]);
    let response = client.send(subscribe_cmd)?;
    assert_eq!(Value::Null, response);

    let published = client.published();
    assert_eq!(2, published.len());
    assert_eq!(
        Value::Push(vec![
            Value::BulkString(String::from("subscribe")),
            Value::BulkString(String::from("first")),
            Value::Integer(1),
        ]),
        published[0]
    );
    assert_eq!(
        Value::Push(vec![
            Value::BulkString(String::from("subscribe")),
            Value::BulkString(String::from("second")),
            Value::Integer(2),
        ]),
        published[1]
    );

    let publish_cmd = Command::Publish {
        channel: Cow::Owned(String::from("first")),
        message: Cow::Owned(String::from("hello first")),
    };
    let response = client.send(publish_cmd)?;
    assert_eq!(Value::Integer(1), response);

    let published = client.published();
    assert_eq!(1, published.len());
    assert_eq!(
        Value::Push(vec![
            Value::BulkString(String::from("message")),
            Value::BulkString(String::from("first")),
            Value::BulkString(String::from("hello first")),
        ]),
        published[0]
    );

    let unsubscribe_cmd = Command::Unsubscribe(vec![
        Cow::Owned(String::from("first")),
        Cow::Owned(String::from("second")),
    ]);
    let response = client.send(unsubscribe_cmd)?;
    assert_eq!(Value::Null, response);

    let published = client.published();
    assert_eq!(2, published.len());
    assert_eq!(
        Value::Push(vec![
            Value::BulkString(String::from("unsubscribe")),
            Value::BulkString(String::from("first")),
            Value::Integer(1),
        ]),
        published[0]
    );
    assert_eq!(
        Value::Push(vec![
            Value::BulkString(String::from("unsubscribe")),
            Value::BulkString(String::from("second")),
            Value::Integer(0),
        ]),
        published[1]
    );

    server_handle.stop();
    thread_handle.join().unwrap();

    Ok(())
}

#[test]
fn increment_value() -> Result<(), Box<dyn std::error::Error>> {
    initialize();

    let (address, server_handle, thread_handle) = launch_server();

    println!("Connecting to server on {}", address);
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    let mut client = Client::new(stream);

    let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
    let response = client.send(get_cmd)?;

    assert_eq!(Value::Null, response);

    let incr_cmd = Command::Increment(Cow::Borrowed("Key"));
    let response = client.send(incr_cmd)?;
    assert_eq!(Value::Integer(1), response);

    let decr_cmd = Command::Decrement(Cow::Borrowed("Key"));
    let response = client.send(decr_cmd)?;
    assert_eq!(Value::Integer(0), response);

    let incr_cmd = Command::IncrementBy(Cow::Borrowed("Key"), 5);
    let response = client.send(incr_cmd)?;
    assert_eq!(Value::Integer(5), response);

    let decr_cmd = Command::DecrementBy(Cow::Borrowed("Key"), 5);
    let response = client.send(decr_cmd)?;
    assert_eq!(Value::Integer(0), response);

    let set_cmd = Command::Set {
        key: Cow::Owned("faulty".to_owned()),
        value: Cow::Owned("Value".to_owned()),
        overwrite_rule: None,
        get: false,
        expire_rule: None,
    };
    let response = client.send(set_cmd)?;
    assert_eq!(Value::ok(), response);

    let incr_faulty_key_cmd = Command::Increment(Cow::Borrowed("faulty"));
    let response = client.send(incr_faulty_key_cmd)?;
    assert_eq!(
        Value::SimpleError(String::from("value is not an integer or out of range")),
        response
    );

    server_handle.stop();
    thread_handle.join().unwrap();

    Ok(())
}

#[test]
fn list() -> Result<(), Box<dyn std::error::Error>> {
    initialize();

    let (address, server_handle, thread_handle) = launch_server();

    println!("Connecting to server on {}", address);
    let stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))?;
    let mut client = Client::new(stream);

    let get_cmd = Command::Get(Cow::Owned(String::from("Key")));
    let response = client.send(get_cmd)?;

    assert_eq!(Value::Null, response);

    let lpush_cmd = Command::LeftPush(
        Cow::Borrowed("Key"),
        vec![Cow::Borrowed("first"), Cow::Borrowed("second")],
    );
    let response = client.send(lpush_cmd)?;
    assert_eq!(Value::Integer(2), response);

    let lrange_cmd = Command::ListRange(Cow::Borrowed("Key"), 0, -1);
    let response = client.send(lrange_cmd)?;
    assert_eq!(
        Value::Array(vec![
            Value::BulkString(String::from("second")),
            Value::BulkString(String::from("first")),
        ]),
        response
    );

    let rpush_cmd = Command::RightPush(
        Cow::Borrowed("Key"),
        vec![Cow::Borrowed("third"), Cow::Borrowed("fourth")],
    );
    let response = client.send(rpush_cmd)?;
    assert_eq!(Value::Integer(4), response);

    let lrange_cmd = Command::ListRange(Cow::Borrowed("Key"), 0, -1);
    let response = client.send(lrange_cmd)?;
    assert_eq!(
        Value::Array(vec![
            Value::BulkString(String::from("second")),
            Value::BulkString(String::from("first")),
            Value::BulkString(String::from("third")),
            Value::BulkString(String::from("fourth")),
        ]),
        response
    );

    let lrange_cmd = Command::ListRange(Cow::Borrowed("Key"), 1, 0);
    let response = client.send(lrange_cmd)?;
    assert_eq!(Value::Array(vec![]), response);

    let lrange_cmd = Command::ListRange(Cow::Borrowed("Key"), -2, -1);
    let response = client.send(lrange_cmd)?;
    assert_eq!(
        Value::Array(vec![
            Value::BulkString(String::from("third")),
            Value::BulkString(String::from("fourth")),
        ]),
        response
    );

    let lrange_cmd = Command::ListRange(Cow::Borrowed("Key"), 0, 0);
    let response = client.send(lrange_cmd)?;
    assert_eq!(
        Value::Array(vec![Value::BulkString(String::from("second")),]),
        response
    );

    let lrange_cmd = Command::ListRange(Cow::Borrowed("Key"), -4, 3);
    let response = client.send(lrange_cmd)?;
    assert_eq!(
        Value::Array(vec![
            Value::BulkString(String::from("second")),
            Value::BulkString(String::from("first")),
            Value::BulkString(String::from("third")),
            Value::BulkString(String::from("fourth")),
        ]),
        response
    );

    let lrange_cmd = Command::ListRange(Cow::Borrowed("Key"), -100, 100);
    let response = client.send(lrange_cmd)?;
    assert_eq!(
        Value::Array(vec![
            Value::BulkString(String::from("second")),
            Value::BulkString(String::from("first")),
            Value::BulkString(String::from("third")),
            Value::BulkString(String::from("fourth")),
        ]),
        response
    );

    server_handle.stop();
    thread_handle.join().unwrap();

    Ok(())
}
