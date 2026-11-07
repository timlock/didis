use crate::async_io::slice::{AsSlice, AsSliceMut};
use crate::async_io::{AsyncRead, AsyncWrite};
use crate::command::Command;
use crate::resp;
use crate::resp::Value;
use std::net::TcpStream;
use std::time::Instant;

//todo make client async
pub struct Client {
    tcp_stream: TcpStream,
    resp_parser: resp::Parser,
    published: Vec<Value>,
}

impl Client {
    pub fn new(tcp_stream: TcpStream) -> Self {
        Self {
            tcp_stream,
            resp_parser: resp::Parser::default(),
            published: Vec::new(),
        }
    }

    pub async fn send(&mut self, command: Command<'_>) -> Result<Value, resp::Error> {
        println!("Sending command to server {}", command);
        let bytes = Vec::from(command);

        let start = Instant::now();

        let (result, _) = self.tcp_stream.async_write_all(bytes.slice(..)).await;
        result?;

        let mut values = self.receive().await?;

        let response = values.pop().unwrap_or(Value::Null); // subscribe and unsubscribe get no response

        println!(
            "Received response from server  bytes duration={:?} {}",
            start.elapsed(),
            response
        );
        Ok(response)
    }

    pub async fn receive(&mut self) -> Result<Vec<Value>, resp::Error> {
        let mut bytes = Vec::new();
        let mut buf = Box::new([0u8; 1024]).slice_mut(..);
        loop {
            let (n, b) = self.tcp_stream.async_read(buf).await;
            let n = n?;
            buf = b;
            bytes.extend_from_slice(&buf[..n]);
            if n < buf.len() {
                break;
            }
        }

        let values = self.resp_parser.parse_all(bytes.as_slice())?;
        let values = self.remove_pub_sub_messages(values);

        Ok(values)
    }

    pub async fn send_batch(
        &mut self,
        commands: Vec<Command<'_>>,
    ) -> Result<Vec<Value>, resp::Error> {
        println!("Sending command batch to server {:?}", commands);
        let bytes: Vec<u8> = commands.into_iter().flat_map(Vec::from).collect();
        let bytes = bytes.slice(..);

        let start = Instant::now();

        let (result, _) = self.tcp_stream.async_write_all(bytes.slice(..)).await;
        result?;

        let values = self.receive().await?;

        println!(
            "Received response batch of size {} from server duration={:?} {:?}",
            values.len(),
            start.elapsed(),
            values
        );

        Ok(values)
    }

    fn remove_pub_sub_messages(&mut self, values: Vec<Value>) -> Vec<Value> {
        values
            .into_iter()
            .filter_map(|value| match &value {
                Value::Push(_) => {
                    self.published.push(value);
                    None
                }
                _ => Some(value),
            })
            .collect()
    }

    pub fn published(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.published)
    }
}
