use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::ops::AddAssign;
use std::time::Duration;

enum ReplicaStatus {
    Normal,
    ViewChange,
    Recovering,
}

struct Client<T> {
    request_number: u64,
    response: Option<T>,
}

impl<T> Client<T> {
    fn new(request_number: u64) -> Self {
        Self {
            request_number,
            response: None,
        }
    }
}

pub trait StateMachine<T> {
    fn apply(&mut self, operation: &T) -> T;
}

enum ReplicaRole {
    Primary,
    Backup,
}

pub struct Replica<T>
where
    T: Clone,
{
    configuration: Vec<SocketAddr>,
    replica_number: usize,
    view_number: usize,
    status: ReplicaStatus,
    op_number: u64,
    log: Vec<Request<T>>,
    commit_number: u64,
    client_table: HashMap<SocketAddr, Client<T>>,

    state_machine: Box<dyn StateMachine<T>>,
    prepared: HashMap<u64, HashSet<usize>>,
    in_wait: Vec<Prepare<T>>,
    timeout: Duration,
    time_since_timeout: Duration,
}

impl<T> Replica<T>
where
    T: Clone,
{
    pub fn new(
        state_machine: Box<dyn StateMachine<T>>,
        replica_number: usize,
        configuration: Vec<SocketAddr>,
        timeout: Duration,
    ) -> Self {
        Self {
            configuration,
            replica_number,
            view_number: 0,
            status: ReplicaStatus::Normal,
            op_number: 0,
            log: vec![],
            commit_number: 0,
            client_table: Default::default(),
            state_machine,
            prepared: Default::default(),
            in_wait: Default::default(),
            timeout,
            time_since_timeout: Default::default(),
        }
    }

    pub fn tick(&mut self, dx: Duration) -> Option<(Vec<SocketAddr>, Commit)> {
        self.time_since_timeout.add_assign(dx);

        if self.time_since_timeout < self.timeout {
            return None;
        }

        self.time_since_timeout = Duration::from_secs(0);

        match self.is_primary() {
            true => {
                let commit = Commit {
                    view_number: self.view_number,
                    commit_number: self.commit_number,
                };
                Some((self.peers(), commit))
            }
            false => None, //TODO view change
        }
    }

    pub fn handle_message(&mut self, message: MessageIn<T>) -> Option<MessageOut<T>> {
        match message {
            MessageIn::Prepare(prepare) => {
                if let Some((addr, prepare_ok)) = self.handle_prepare(prepare) {
                    return Some(MessageOut::PrepareOk(prepare_ok, addr));
                }
            }
            MessageIn::PrepareOk(prepare_ok) => {
                if let Some((addr, reply)) = self.handle_prepare_ok(prepare_ok) {
                    return Some(MessageOut::Reply(reply, addr));
                }
            }
            MessageIn::Commit(commit) => {
                self.handle_commit(commit.commit_number);
            }
        };
        None
    }

    pub fn handle_request(&mut self, request: Request<T>) -> Option<RequestResponse<T>> {
        if self.is_backup() {
            return None;
        }

        let client_id = request.client_id;

        let client = self
            .client_table
            .get(&client_id)
            .expect("Primary should store a client before handling its request");

        if request.request_number < client.request_number {
            return None;
        }
        if request.request_number == client.request_number {
            return Some(RequestResponse::SendResponse(
                client_id,
                client.response.clone(),
            ));
        }

        self.op_number += 1;
        self.log.push(request.clone());

        let prepare = Prepare {
            view_number: self.view_number,
            message: request,
            op_number: self.op_number,
            commit_number: self.commit_number,
        };

        Some(RequestResponse::Prepare(self.peers(), prepare))
    }

    fn handle_prepare_ok(&mut self, prepare_ok: PrepareOk) -> Option<(SocketAddr, Reply<T>)> {
        if self.is_backup() {
            return None;
        }

        if prepare_ok.op_number != self.commit_number + 1 {
            return None;
        }

        let prepared = self
            .prepared
            .get_mut(&prepare_ok.op_number)
            .expect("Primary should create a new count set before preparing a operation");
        prepared.insert(prepare_ok.replica_number);
        if self.prepared.len() < self.quorum() {
            return None;
        }

        self.commit_number += 1;
        let request = self
            .log
            .get(self.commit_number as usize)
            .expect("The log should contain the operation after it has been commited");

        let result = self.state_machine.apply(&request.operation);

        let client = self
            .client_table
            .get_mut(&request.client_id)
            .expect("Primary should have the client stored when applying it's request");

        if client.request_number == request.request_number {
            client.response = Some(result.clone());
        }

        let reply = Reply {
            view_number: self.view_number,
            request_number: request.request_number,
            result,
        };

        Some((request.client_id, reply))
    }

    fn handle_prepare(&mut self, prepare: Prepare<T>) -> Option<(SocketAddr, PrepareOk)> {
        if self.is_primary() {
            return None;
        }

        if self.op_number >= prepare.op_number {
            return None;
        }
        if self.op_number + 1 < prepare.op_number {
            self.in_wait.push(prepare);
            return None;
        }

        let client_id = prepare.message.client_id;

        if self.client_table.get(&client_id).is_none() {
            self.client_table
                .insert(client_id, Client::new(prepare.message.request_number));
        }
        let client = self
            .client_table
            .get_mut(&prepare.message.client_id)
            .unwrap();
        client.request_number = prepare.message.request_number;

        self.op_number += 1;
        self.log.push(prepare.message);

        if !self.in_wait.is_empty() {
            self.add_prepares_in_wait()
        }

        if prepare.commit_number > self.commit_number {
            self.handle_commit(prepare.commit_number);
        }

        let prepare_ok = PrepareOk {
            view_number: self.view_number,
            op_number: self.op_number,
            replica_number: self.replica_number,
        };

        Some((self.primary_addr(), prepare_ok))
    }

    fn handle_commit(&mut self, commit_number: u64) {
        let end = min(commit_number as usize, self.log.len());
        let range = self.commit_number as usize..end;
        for request in self.log[range].iter() {
            let reply = self.state_machine.apply(&request.operation);
            let client = self
                .client_table
                .get_mut(&request.client_id)
                .expect("Backup should have client stored when applying its request");

            client.request_number = request.request_number;
            client.response = Some(reply);
        }

        self.commit_number = end as u64;
    }

    fn add_prepares_in_wait(&mut self) {
        self.in_wait.sort_by(|a, b| a.op_number.cmp(&b.op_number));

        let mut i = 0;
        while let Some(prepare) = self.in_wait.iter().next() {
            if prepare.op_number == self.op_number + 1 {
                i += 1;
            }
        }

        for prepare in self.in_wait.drain(..i) {
            self.log.push(prepare.message);
            self.op_number += 1;
        }
    }
    fn is_primary(&self) -> bool {
        self.view_number % self.configuration.len() == 0
    }

    fn is_backup(&self) -> bool {
        !self.is_primary()
    }
    fn quorum(&self) -> usize {
        self.configuration.len() / 2
    }

    fn peers(&self) -> Vec<SocketAddr> {
        let mut peers = self.configuration.clone();
        peers.remove(self.replica_number);
        peers
    }

    fn primary_addr(&self) -> SocketAddr {
        self.configuration[self.view_number % self.configuration.len()]
    }
}

enum RequestResponse<T: Clone> {
    SendResponse(SocketAddr, Option<T>),
    Prepare(Vec<SocketAddr>, Prepare<T>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Request<T: Clone> {
    operation: T,
    client_id: SocketAddr,
    request_number: u64,
}

#[derive(Debug, PartialEq)]
pub struct Prepare<T: Clone> {
    view_number: usize,
    message: Request<T>,
    op_number: u64,
    commit_number: u64,
}

#[derive(Debug, PartialEq)]
pub struct PrepareOk {
    view_number: usize,
    op_number: u64,
    replica_number: usize,
}

#[derive(Debug, PartialEq)]
pub struct Reply<T> {
    view_number: usize,
    request_number: u64,
    result: T,
}

#[derive(Debug, PartialEq)]
pub struct Commit {
    view_number: usize,
    commit_number: u64,
}

#[derive(Debug, PartialEq)]
pub enum MessageIn<T: Clone> {
    Prepare(Prepare<T>),
    PrepareOk(PrepareOk),
    Commit(Commit),
}

#[derive(Debug)]
pub enum MessageOut<T: Clone> {
    Prepare(Prepare<T>, Vec<SocketAddr>),
    PrepareOk(PrepareOk, SocketAddr),
    Commit(Commit, Vec<SocketAddr>),
    Reply(Reply<T>, SocketAddr),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::net::{IpAddr, Ipv4Addr};

    const CONFIG: &[SocketAddr] = &[
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0000),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0001),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0002),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0003),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0005),
    ];

    fn peers(primary: usize) -> Vec<SocketAddr> {
        let mut peers = CONFIG.to_vec();
        peers.remove(primary);
        peers
    }

    struct MockStateMachine {}

    impl StateMachine<String> for MockStateMachine {
        fn apply(&mut self, operation: &String) -> String {
            todo!()
        }
    }

    #[test]
    fn tick_primary_timeout() -> Result<(), Box<dyn Error>> {
        let state_machine = Box::new(MockStateMachine {});
        let timeout = Duration::from_secs(2);
        let mut replica = Replica::new(state_machine, 0, CONFIG.to_vec(), timeout);

        let got = replica.tick(Duration::from_secs(1));
        assert_eq!(None, got);

        let got = replica.tick(Duration::from_secs(1));
        let wanted = Some((
            peers(0),
            Commit {
                view_number: 0,
                commit_number: 0,
            },
        ));
        assert_eq!(wanted, got);

        Ok(())
    }

    #[test]
    fn tick_handle_request() -> Result<(), Box<dyn Error>> {
        let state_machine = Box::new(MockStateMachine {});
        let timeout = Duration::from_secs(2);
        let mut replica = Replica::new(state_machine, 0, CONFIG.to_vec(), timeout);

        Ok(())
    }
}
