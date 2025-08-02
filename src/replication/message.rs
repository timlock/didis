use crate::replication::Operation;
use std::cmp::Ordering;
use std::hash::Hash;
use std::net::SocketAddr;

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct Request<T: Clone> {
    pub operation: T,
    pub client_id: SocketAddr,
    pub request_number: u64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Prepare<T: Clone> {
    pub view_number: usize,
    pub message: Request<T>,
    pub op_number: u64,
    pub commit_number: u64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PrepareOk {
    pub view_number: usize,
    pub op_number: u64,
    pub replica_number: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Reply<T> {
    pub view_number: usize,
    pub request_number: u64,
    pub result: T,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Commit {
    pub view_number: usize,
    pub commit_number: u64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct StartViewChange {
    pub view_number: usize,
    pub replica_number: usize,
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
pub struct DoViewChange<T>
where
    T: Clone + Hash + Eq,
{
    pub view_number: usize,
    pub log: Vec<Request<T>>,
    pub last_normal_view_number: usize,
    pub op_number: u64,
    pub commit_number: u64,
    pub replica_number: usize,
}

impl<T> PartialOrd<Self> for DoViewChange<T>
where
    T: Clone + Eq + Hash,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for DoViewChange<T>
where
    T: Clone + Hash + Eq,
{
    fn cmp(&self, other: &Self) -> Ordering {
        if self.last_normal_view_number > other.last_normal_view_number {
            return Ordering::Greater;
        }
        if self.last_normal_view_number < other.last_normal_view_number {
            return Ordering::Less;
        }
        if self.op_number > other.op_number {
            return Ordering::Greater;
        }
        if self.op_number < other.op_number {
            return Ordering::Less;
        }
        Ordering::Equal
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct StartView<T: Clone> {
    pub view_number: usize,
    pub log: Vec<Request<T>>,
    pub op_number: u64,
    pub commit_number: u64,
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
pub enum MessageOut<T>
where
    T: Operation,
{
    Prepare(Prepare<T>, Vec<SocketAddr>),
    PrepareOk(PrepareOk, SocketAddr),
    Commit(Commit, Vec<SocketAddr>),
    Reply(Reply<T>, SocketAddr),
    ClientResponse(Reply<T>, SocketAddr),
    StartViewChange(StartViewChange, Vec<SocketAddr>),
    DoViewChange(DoViewChange<T>, SocketAddr),
    StartView(StartView<T>, Vec<SocketAddr>, Vec<(Reply<T>, SocketAddr)>),
}

impl<T> TryFrom<MessageOut<T>> for MessageIn<T>
where
    T: Operation,
{
    type Error = ();

    fn try_from(value: MessageOut<T>) -> Result<Self, Self::Error> {
        let message_in = match value {
            MessageOut::Prepare(prepare, _) => MessageIn::Prepare(prepare),
            MessageOut::PrepareOk(prepare_ok, _) => MessageIn::PrepareOk(prepare_ok),
            MessageOut::Commit(commit, _) => MessageIn::Commit(commit),
            MessageOut::Reply(reply, _) => return Err(()),
            MessageOut::ClientResponse(client_response, _) => return Err(()),
            MessageOut::StartViewChange(start_view_change, _) => {
                MessageIn::StartViewChange(start_view_change)
            }
            MessageOut::DoViewChange(do_view_change, _) => MessageIn::DoViewChange(do_view_change),
            MessageOut::StartView(start_view, _, _) => MessageIn::StartView(start_view),
        };

        Ok(message_in)
    }
}
