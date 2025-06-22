use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

enum ReplicaStatus<T>
where
    T: Clone + Hash + Eq,
{
    Normal,
    ViewChange(HashSet<usize>, usize, HashSet<DoViewChange<T>>),
    Recovering,
}

impl<T> ReplicaStatus<T>
where
    T: Clone + Hash + Eq,
{
    fn is_normal(&self) -> bool {
        match self {
            ReplicaStatus::Normal => true,
            _ => false,
        }
    }

    fn is_view_change(&self) -> bool {
        match self {
            ReplicaStatus::ViewChange(..) => true,
            _ => false,
        }
    }

    fn is_recovering(&self) -> bool {
        match self {
            ReplicaStatus::Recovering => true,
            _ => false,
        }
    }
}

struct Client<T> {
    request_number: u64,
    response: Option<T>,
}

impl<T> Client<T> {
    fn update(&mut self, request_number: u64, response: T) {
        if self.request_number == request_number {
            self.response = Some(response);
        }
    }
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

pub struct Replica<T>
where
    T: Clone + Hash + Eq,
{
    configuration: Vec<SocketAddr>,
    replica_number: usize,
    view_number: usize,
    status: ReplicaStatus<T>,
    op_number: u64,
    log: Vec<Request<T>>,
    commit_number: u64,
    client_table: HashMap<SocketAddr, Client<T>>,

    state_machine: Box<dyn StateMachine<T>>,
    prepared: HashMap<u64, HashSet<usize>>,
    in_wait: Vec<Prepare<T>>,
    interval: Duration,
    last_timeout: Instant,
}

impl<T> Replica<T>
where
    T: Clone + Hash + Eq,
{
    pub fn new(
        state_machine: Box<dyn StateMachine<T>>,
        replica_number: usize,
        configuration: Vec<SocketAddr>,
        timeout: Duration,
        last_timeout: Instant,
    ) -> Self {
        assert!(replica_number > 0);
        Self {
            configuration,
            replica_number,
            view_number: 0,
            status: ReplicaStatus::Normal,
            op_number: 0,
            log: Default::default(),
            commit_number: 0,
            client_table: Default::default(),
            state_machine,
            prepared: Default::default(),
            in_wait: Default::default(),
            interval: timeout,
            last_timeout,
        }
    }

    pub fn handle_timeout(&mut self, now: Instant) -> Option<(MessageOut<T>)> {
        if now.duration_since(self.last_timeout) < self.interval {
            return None;
        }

        self.last_timeout = now;

        match self.is_primary() {
            true => {
                let commit = Commit {
                    view_number: self.view_number,
                    commit_number: self.commit_number,
                };
                Some(MessageOut::Commit(commit, self.peers()))
            }
            false => Some(self.start_view_change(self.view_number + 1)),
        }
    }

    pub fn poll_timeout(&self) -> Instant {
        self.last_timeout + self.interval
    }

    pub fn handle_message(&mut self, message: MessageIn<T>) -> Option<MessageOut<T>> {
        match message {
            MessageIn::ClientRequest(request) => {
                return self.handle_request(request);
            }
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
            MessageIn::Commit(commit) => self.handle_commit(commit.commit_number),
            MessageIn::StartViewChange(start_view_change) => {
                if let Some((addr, do_view_change)) =
                    self.handle_start_view_change(start_view_change)
                {
                    return Some(MessageOut::DoViewChange(do_view_change, addr));
                }
            }
            MessageIn::DoViewChange(do_view_change) => {
                if let Some((addr, start_view)) = self.handle_do_view_change(do_view_change) {
                    return Some(MessageOut::StartView(start_view, addr));
                }
            }
            MessageIn::StartView(_) => todo!(),
        };
        None
    }

    fn add_client(&mut self, client_id: SocketAddr) -> &mut Client<T> {
        self.client_table.insert(
            client_id,
            Client {
                request_number: 0,
                response: None,
            },
        );
        self.client_table.get_mut(&client_id).unwrap()
    }

    fn handle_request(&mut self, request: Request<T>) -> Option<MessageOut<T>> {
        if self.is_backup() {
            return None;
        }

        let client_id = request.client_id;

        let client = match self.client_table.get_mut(&client_id) {
            Some(client) => client,
            None => self.add_client(client_id),
        };

        if request.request_number < client.request_number {
            return None;
        }
        if request.request_number == client.request_number {
            if let Some(result) = client.response.clone() {
                return Some(MessageOut::ClientResponse(
                    Reply {
                        view_number: self.view_number,
                        request_number: request.request_number,
                        result,
                    },
                    client_id,
                ));
            }
            return None;
        }

        client.request_number = request.request_number;

        self.op_number += 1;
        self.log.push(request.clone());

        let prepare = Prepare {
            view_number: self.view_number,
            message: request,
            op_number: self.op_number,
            commit_number: self.commit_number,
        };

        Some(MessageOut::Prepare(prepare, self.peers()))
    }

    fn add_prepared(&mut self, op_number: u64) -> &mut HashSet<usize> {
        self.prepared.insert(op_number, HashSet::new());
        self.prepared.get_mut(&op_number).unwrap()
    }

    fn handle_prepare_ok(&mut self, prepare_ok: PrepareOk) -> Option<(SocketAddr, Reply<T>)> {
        if self.is_backup() {
            return None;
        }

        if prepare_ok.op_number != self.commit_number + 1 {
            return None;
        }

        let prepared_peers = match self.prepared.get_mut(&prepare_ok.op_number) {
            Some(prepared) => prepared,
            None => self.add_prepared(prepare_ok.op_number),
        };
        prepared_peers.insert(prepare_ok.replica_number);
        if prepared_peers.len() < self.quorum() {
            return None;
        }

        self.commit_number += 1;
        let request = self
            .log
            .get((self.commit_number - 1) as usize)
            .expect("The log should contain the operation after it has been commited");

        let result = self.state_machine.apply(&request.operation);

        if let Some(client) = self.client_table.get_mut(&request.client_id) {
            client.update(request.request_number, result.clone());
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

        self.handle_commit(prepare.commit_number);

        let prepare_ok = PrepareOk {
            view_number: self.view_number,
            op_number: self.op_number,
            replica_number: self.replica_number,
        };

        Some((self.primary_addr(), prepare_ok))
    }

    // TODO state transfer if commit_number is higher than log size?
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
            self.commit_number += 1;
        }
    }

    fn handle_start_view_change(
        &mut self,
        start_view_change: StartViewChange,
    ) -> Option<(SocketAddr, DoViewChange<T>)> {
        assert!(self.status.is_view_change());
        if start_view_change.view_number != self.view_number {
            return None;
        }

        let quorum = self.quorum();
        match &mut self.status {
            ReplicaStatus::ViewChange(peers_in_same_view, last_normal_view_number, _) => {
                peers_in_same_view.insert(start_view_change.replica_number);

                if peers_in_same_view.len() > quorum {
                    let do_view_change = DoViewChange {
                        view_number: self.view_number,
                        log: self.log.clone(),
                        last_normal_view_number: *last_normal_view_number,
                        op_number: self.op_number,
                        commit_number: self.commit_number,
                    };

                    return Some((self.primary_addr(), do_view_change));
                }
            }
            _ => {
                panic!()
            }
        };

        None
    }

    fn start_view_change(&mut self, new_view_number: usize) -> MessageOut<T> {
        assert!(new_view_number > self.view_number);

        let last_normal_view_number = match self.status {
            ReplicaStatus::Normal => self.view_number,
            ReplicaStatus::ViewChange(_, last_normal_view_number, _) => last_normal_view_number,
            ReplicaStatus::Recovering => {
                panic!("Replica should not start a view change when in recovery mode")
            }
        };
        self.status = ReplicaStatus::ViewChange(
            Default::default(),
            last_normal_view_number,
            Default::default(),
        );
        self.view_number = new_view_number;

        let start_view_change = StartViewChange {
            view_number: self.view_number,
            replica_number: self.replica_number,
        };
        MessageOut::StartViewChange(start_view_change, self.peers())
    }

    fn handle_do_view_change(
        &mut self,
        do_view_change: DoViewChange<T>,
    ) -> Option<(Vec<SocketAddr>, StartView<T>)> {
        let quorum = self.quorum();

        let do_view_changes = match &mut self.status {
            ReplicaStatus::Normal if self.view_number < do_view_change.view_number => return None,
            ReplicaStatus::Normal => {
                self.start_view_change(do_view_change.view_number);
                match &mut self.status {
                    ReplicaStatus::ViewChange(_, _, do_view_changes) => {
                        do_view_changes.insert(do_view_change);
                        do_view_changes
                    }
                    _ => panic!("Replica should be in view change mode after receiving a DoViewChange with higher view number than its own"),
                }
            }
            ReplicaStatus::ViewChange(_, _, do_view_changes) => do_view_changes,
            ReplicaStatus::Recovering => {
                panic!("Replica should not start a view change when in recovery mode")
            }
        };

        if do_view_changes.len() < quorum {
            return None;
        }
        let mut best_msg: Option<&DoViewChange<T>> = None;
        for msg in do_view_changes.iter() {
            match best_msg {
                Some(best) => {
                    if best.last_normal_view_number < msg.last_normal_view_number
                        || (best.last_normal_view_number == msg.last_normal_view_number
                            && best.op_number < msg.op_number)
                    {
                        best_msg = Some(msg)
                    }
                }
                None => best_msg = Some(msg),
            }
        }
        let best_msg =
            best_msg.expect("Primary should have f do_view_changes messages stored, but has none");
        

        None
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
        self.view_number % self.configuration.len() + 1 == self.replica_number
    }

    fn is_backup(&self) -> bool {
        !self.is_primary()
    }
    fn quorum(&self) -> usize {
        self.configuration.len() / 2
    }

    fn peers(&self) -> Vec<SocketAddr> {
        let mut peers = self.configuration.clone();
        peers.remove(self.replica_number - 1);
        peers
    }

    fn primary_addr(&self) -> SocketAddr {
        self.configuration[self.view_number % self.configuration.len()]
    }
}

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct Request<T: Clone> {
    operation: T,
    client_id: SocketAddr,
    request_number: u64,
}

#[derive(Debug, PartialEq, Clone)]
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
pub struct StartViewChange {
    view_number: usize,
    replica_number: usize,
}

#[derive(Debug, PartialEq, Hash, Eq)]
pub struct DoViewChange<T>
where
    T: Clone + Hash + Eq,
{
    view_number: usize,
    log: Vec<Request<T>>,
    last_normal_view_number: usize,
    op_number: u64,
    commit_number: u64,
}

#[derive(Debug, PartialEq)]
pub struct StartView<T: Clone> {
    view_number: usize,
    log: Vec<Request<T>>,
    op_number: u64,
    commit_number: u64,
}

#[derive(Debug, PartialEq)]
pub enum MessageIn<T>
where
    T: Clone + Hash + Eq,
{
    ClientRequest(Request<T>),
    Prepare(Prepare<T>),
    PrepareOk(PrepareOk),
    Commit(Commit),
    StartViewChange(StartViewChange),
    DoViewChange(DoViewChange<T>),
    StartView(StartView<T>),
}

#[derive(Debug, PartialEq)]
pub enum MessageOut<T>
where
    T: Clone + Hash + Eq,
{
    Prepare(Prepare<T>, Vec<SocketAddr>),
    PrepareOk(PrepareOk, SocketAddr),
    Commit(Commit, Vec<SocketAddr>),
    Reply(Reply<T>, SocketAddr),
    ClientResponse(Reply<T>, SocketAddr),
    StartViewChange(StartViewChange, Vec<SocketAddr>),
    DoViewChange(DoViewChange<T>, SocketAddr),
    StartView(StartView<T>, Vec<SocketAddr>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::net::{IpAddr, Ipv4Addr};
    use std::str::FromStr;

    const CONFIG: &[SocketAddr] = &[
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0001),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0002),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0003),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0004),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), 0005),
    ];

    fn peers(exclude: usize) -> Vec<SocketAddr> {
        let mut peers = CONFIG.to_vec();
        peers.remove(exclude);
        peers
    }

    struct MockStateMachine {
        callback: Box<dyn FnMut(&String) -> String>,
    }

    impl MockStateMachine {
        fn new<T>(callback: T) -> Self
        where
            T: FnMut(&String) -> String + 'static + Into<Box<T>>,
        {
            Self {
                callback: callback.into(),
            }
        }
    }

    impl StateMachine<String> for MockStateMachine {
        fn apply(&mut self, operation: &String) -> String {
            (self.callback)(operation)
        }
    }

    #[test]
    fn tick_primary_timeout() -> Result<(), Box<dyn Error>> {
        let state_machine = Box::new(MockStateMachine::new(|_| unimplemented!()));
        let interval = Duration::from_secs(2);
        let last_timeout = Instant::now();
        let mut replica = Replica::new(state_machine, 1, CONFIG.to_vec(), interval, last_timeout);

        let next_timeout = replica.poll_timeout();
        assert_eq!(last_timeout + interval, next_timeout);

        let got = replica.handle_timeout(last_timeout + Duration::from_secs(1));
        assert_eq!(None, got);

        let got = replica.handle_timeout(last_timeout + Duration::from_secs(2));
        let wanted = Some(MessageOut::Commit(
            Commit {
                view_number: 0,
                commit_number: 0,
            },
            peers(0),
        ));
        assert_eq!(wanted, got);

        Ok(())
    }

    #[test]
    fn replicate_request() -> Result<(), Box<dyn Error>> {
        let interval = Duration::from_secs(2);
        let last_timeout = Instant::now();

        let primary_state_machine = Box::new(MockStateMachine::new(|_| "success".to_owned()));
        let mut primary = Replica::new(
            primary_state_machine,
            1,
            CONFIG.to_vec(),
            interval,
            last_timeout,
        );

        let replica1_state_machine = Box::new(MockStateMachine::new(|op| "success".to_owned()));
        let mut replica1 = Replica::new(
            replica1_state_machine,
            2,
            CONFIG.to_vec(),
            interval,
            last_timeout,
        );
        let replica2_state_machine = Box::new(MockStateMachine::new(|_| "success".to_owned()));
        let mut replica2 = Replica::new(
            replica2_state_machine,
            3,
            CONFIG.to_vec(),
            interval,
            last_timeout,
        );

        assert!(primary.is_primary());
        assert!(replica1.is_backup());
        assert!(replica2.is_backup());

        let client_id = SocketAddr::from_str("0.0.0.2:1")?;
        let first_request = Request {
            operation: "operation".to_owned(),
            client_id: client_id,
            request_number: 1,
        };

        // client sends request
        let response = primary.handle_message(MessageIn::ClientRequest(first_request.clone()));
        assert!(response.is_some());
        let (prepare, destination) = match response.unwrap() {
            MessageOut::Prepare(prepare, peers) => (prepare, peers),
            _ => return Err("primary response should be of type Prepare".into()),
        };
        assert_eq!(peers(0), destination);
        let want = Prepare {
            view_number: 0,
            message: first_request.clone(),
            op_number: 1,
            commit_number: 0,
        };
        assert_eq!(want, prepare);

        // replicas handle prepare
        let response = replica1.handle_message(MessageIn::Prepare(prepare.clone()));
        assert!(response.is_some());
        let (prepare_ok1, destination) = match response.unwrap() {
            MessageOut::PrepareOk(prepare_ok, destination) => (prepare_ok, destination),
            _ => return Err("replica1 response should be of type PrepareOk".into()),
        };
        assert_eq!(CONFIG[0], destination);
        let want = PrepareOk {
            view_number: 0,
            op_number: 1,
            replica_number: 2,
        };
        assert_eq!(want, prepare_ok1);

        let response = replica2.handle_message(MessageIn::Prepare(prepare));
        assert!(response.is_some());
        let (prepare_ok2, destination) = match response.unwrap() {
            MessageOut::PrepareOk(prepare_ok, destination) => (prepare_ok, destination),
            _ => return Err("replica1 response should be of type PrepareOk".into()),
        };
        assert_eq!(CONFIG[0], destination);
        let want = PrepareOk {
            view_number: 0,
            op_number: 1,
            replica_number: 3,
        };
        assert_eq!(want, prepare_ok2);

        // primary handles prepare_oks
        let response = primary.handle_message(MessageIn::PrepareOk(prepare_ok1));
        assert!(response.is_none());

        let response = primary.handle_message(MessageIn::PrepareOk(prepare_ok2));
        assert!(response.is_some());
        let (reply, destination) = match response.unwrap() {
            MessageOut::Reply(reply, destination) => (reply, destination),
            _ => return Err("Primary response should be of type Reply".into()),
        };
        assert_eq!(client_id, destination);
        let want = Reply {
            view_number: 0,
            request_number: 1,
            result: "success".to_owned(),
        };
        assert_eq!(want, reply);

        // client resends request
        let response = primary.handle_message(MessageIn::ClientRequest(first_request.clone()));
        let (reply, destination) = match response.unwrap() {
            MessageOut::ClientResponse(reply, destination) => (reply, destination),
            _ => return Err("Primary response should be of type Reply".into()),
        };
        assert_eq!(client_id, destination);
        assert_eq!(want, reply);

        // client sends new request
        let second_request = Request {
            operation: "operation".to_owned(),
            client_id: client_id,
            request_number: 2,
        };

        let response = primary.handle_message(MessageIn::ClientRequest(second_request.clone()));
        assert!(response.is_some());
        let (prepare, destination) = match response.unwrap() {
            MessageOut::Prepare(prepare, peers) => (prepare, peers),
            _ => return Err("primary response should be of type Prepare".into()),
        };
        assert_eq!(peers(0), destination);
        let want = Prepare {
            view_number: 0,
            message: second_request.clone(),
            op_number: 2,
            commit_number: 1,
        };
        assert_eq!(want, prepare);

        // replicas handle prepare
        let response = replica1.handle_message(MessageIn::Prepare(prepare.clone()));
        assert!(response.is_some());
        let (prepare_ok1, destination) = match response.unwrap() {
            MessageOut::PrepareOk(prepare_ok, destination) => (prepare_ok, destination),
            _ => return Err("replica1 response should be of type PrepareOk".into()),
        };
        assert_eq!(CONFIG[0], destination);
        let want = PrepareOk {
            view_number: 0,
            op_number: 2,
            replica_number: 2,
        };
        assert_eq!(want, prepare_ok1);

        let response = replica2.handle_message(MessageIn::Prepare(prepare));
        assert!(response.is_some());
        let (prepare_ok2, destination) = match response.unwrap() {
            MessageOut::PrepareOk(prepare_ok, destination) => (prepare_ok, destination),
            _ => return Err("replica1 response should be of type PrepareOk".into()),
        };
        assert_eq!(CONFIG[0], destination);
        let want = PrepareOk {
            view_number: 0,
            op_number: 2,
            replica_number: 3,
        };
        assert_eq!(want, prepare_ok2);

        // primary handles prepare_oks
        let response = primary.handle_message(MessageIn::PrepareOk(prepare_ok1));
        assert!(response.is_none());

        let response = primary.handle_message(MessageIn::PrepareOk(prepare_ok2));
        assert!(response.is_some());
        let (reply, destination) = match response.unwrap() {
            MessageOut::Reply(reply, destination) => (reply, destination),
            _ => return Err("Primary response should be of type Reply".into()),
        };
        assert_eq!(client_id, destination);
        let want = Reply {
            view_number: 0,
            request_number: 2,
            result: "success".to_owned(),
        };
        assert_eq!(want, reply);
        Ok(())
    }
}
