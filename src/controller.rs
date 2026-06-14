use crate::dictionary::{Dictionary, Error};
use crate::parser::command::Command;
use crate::parser::resp::{Reference, ValOrRef, Value};
use crate::pubsub::{ChannelStore, Message};
use crate::storage::rdb::{ValueType, RDB};
use libc::{c_int, pid_t};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::time::SystemTime;
use std::{fs, io};

pub struct Controller {
    dictionary: Dictionary,
    channel_store: ChannelStore,
    messages: VecDeque<Message>,
    background_jobs: JobQueue,
    last_save: SystemTime,
}

impl Controller {
    pub fn new(last_save: SystemTime) -> Controller {
        Controller {
            dictionary: Dictionary::default(),
            channel_store: ChannelStore::default(),
            messages: VecDeque::default(),
            background_jobs: JobQueue::default(),
            last_save,
        }
    }
    pub fn handle_command(&mut self, client_id: u64, command: Command) -> Option<ValOrRef> {
        let result: ValOrRef = match command {
            Command::Ping(None) => Reference::SimpleString("PONG").into(),
            Command::Ping(Some(text)) => Value::BulkString(text.into_owned()).into(),
            Command::Echo(s) => Value::BulkString(s.into_owned()).into(),
            Command::Get(key) => match self.dictionary.get(&key) {
                Ok(Some(value)) => Reference::BulkString(value).into(),
                Ok(None) => Value::Null.into(),
                Err(err) => Value::SimpleError(err.to_string()).into(),
            },
            Command::Set {
                key,
                value,
                overwrite_rule,
                get,
                expire_rule,
            } => {
                match self.dictionary.set(
                    key.as_ref(),
                    value.into_owned(),
                    overwrite_rule,
                    get,
                    expire_rule,
                ) {
                    Ok(Some(old_value)) => Value::BulkString(old_value).into(),
                    Ok(None) if !get => Value::ok().into(),
                    Ok(None) => Value::Null.into(),
                    Err(Error::OverrideConflict) => Value::Null.into(),
                    Err(err) => Value::SimpleError(err.to_string()).into(),
                }
            }
            Command::ConfigGet(key) => {
                // minimal implementation to allow benchmarking with redis-cli
                if key[0] == "appendonly" {
                    Reference::Array(vec![
                        Reference::BulkString("appendonly"),
                        Reference::BulkString("no"),
                    ])
                    .into()
                } else {
                    Reference::Array(vec![
                        Reference::BulkString("save"),
                        Reference::BulkString(""),
                    ])
                    .into()
                }
            }
            Command::Client => Value::ok().into(), // minimal implementation to allow benchmarking with redis-cli
            Command::Exists(keys) => {
                let mut count = 0;
                for key in keys {
                    if self.dictionary.exists(key.as_ref()) {
                        count += 1;
                    }
                }

                Value::Integer(count).into()
            }
            Command::Delete(keys) => {
                let mut count = 0;
                for key in keys {
                    if self.dictionary.delete(&key) {
                        count += 1;
                    }
                }

                Value::Integer(count).into()
            }
            Command::Increment(key) => match self.dictionary.increment(key.as_ref(), 1) {
                Ok(value) => ValOrRef::Val(Value::Integer(value)),
                Err(err) => ValOrRef::Val(Value::SimpleError(err.to_string())),
            },
            Command::IncrementBy(key, by) => match self.dictionary.increment(key.as_ref(), by) {
                Ok(value) => ValOrRef::Val(Value::Integer(value)),
                Err(err) => ValOrRef::Val(Value::SimpleError(err.to_string())),
            },
            Command::Decrement(key) => match self.dictionary.decrement(key.as_ref(), 1) {
                Ok(value) => ValOrRef::Val(Value::Integer(value)),
                Err(err) => ValOrRef::Val(Value::SimpleError(err.to_string())),
            },
            Command::DecrementBy(key, by) => match self.dictionary.decrement(key.as_ref(), by) {
                Ok(value) => ValOrRef::Val(Value::Integer(value)),
                Err(err) => ValOrRef::Val(Value::SimpleError(err.to_string())),
            },
            Command::ListRange(key, start, end) => {
                match self.dictionary.list_range(key.as_ref(), start, end) {
                    Ok(items) => {
                        let references = items
                            .into_iter()
                            .map(|item| Reference::BulkString(item))
                            .collect();

                        ValOrRef::Ref(Reference::Array(references))
                    }
                    Err(err) => ValOrRef::Val(Value::SimpleError(err.to_string())),
                }
            }
            Command::LeftPush(key, items) => match self.dictionary.left_push(key.as_ref(), items) {
                Ok(len) => ValOrRef::Val(Value::Integer(len)),
                Err(err) => ValOrRef::Val(Value::SimpleError(err.to_string())),
            },
            Command::RightPush(key, items) => match self.dictionary.right_push(key.as_ref(), items)
            {
                Ok(len) => ValOrRef::Val(Value::Integer(len)),
                Err(err) => ValOrRef::Val(Value::SimpleError(err.to_string())),
            },
            Command::Subscribe(channels) => {
                for channel in channels {
                    let channel = channel.into_owned();
                    let subscribed_channels =
                        self.channel_store.subscribe(client_id, channel.clone());

                    let subscribers: Vec<u64> =
                        self.channel_store.subscribers(channel.as_ref()).collect();

                    let message =
                        Message::subscribe(subscribers, channel, subscribed_channels as i64);
                    self.messages.push_back(message);
                }
                return None;
            }
            Command::Unsubscribe(channels) => {
                for channel in channels {
                    let subscribers: Vec<u64> =
                        self.channel_store.subscribers(channel.as_ref()).collect();

                    let subscribed_channels =
                        self.channel_store.unsubscribe(client_id, channel.as_ref());

                    let message = Message::unsubscribe(
                        subscribers,
                        channel.into_owned(),
                        subscribed_channels as i64,
                    );
                    self.messages.push_back(message);
                }
                return None;
            }
            Command::Publish { channel, message } => {
                let subscribers: Vec<u64> =
                    self.channel_store.subscribers(channel.as_ref()).collect();
                let subscribers_len = subscribers.len() as i64;

                let message =
                    Message::publish(subscribers, channel.into_owned(), message.into_owned());
                self.messages.push_back(message);

                Value::Integer(subscribers_len).into()
            }
            Command::Save => match self.save() {
                Ok(()) => ValOrRef::Ref(Reference::SimpleString("ok")),
                Err(simple_error) => ValOrRef::Val(simple_error),
            },
            Command::BackgroundSave(scheduled) => match self.background_save(scheduled) {
                Ok(true) => ValOrRef::Ref(Reference::SimpleString("Background saving started")),
                Ok(false) => ValOrRef::Ref(Reference::SimpleString("Background saving scheduled")),
                Err(simple_error) => ValOrRef::Val(simple_error),
            },
            Command::LastSave => match self.last_save.duration_since(SystemTime::UNIX_EPOCH) {
                Ok(duration) => ValOrRef::Val(Value::Integer(duration.as_secs() as i64)),
                Err(err) => ValOrRef::Ref(Reference::SimpleError("System clock error")),
            },
            Command::Expire {
                key,
                seconds,
                expire_rule,
            } => match self.dictionary.expire(key.as_ref(), seconds, expire_rule) {
                true => ValOrRef::Val(Value::Integer(1)),
                false => ValOrRef::Val(Value::Integer(0)),
            },
        };
        Some(result)
    }

    pub fn remove_client(&mut self, client_id: &u64) {
        self.channel_store.remove_client(*client_id);
    }

    pub fn has_messages(&self) -> bool {
        !self.messages.is_empty()
    }
    pub fn messages(&mut self) -> Vec<Message> {
        self.messages.drain(..).collect()
    }

    pub fn restore_from_snapshot(&mut self, snapshot_path: &str) -> Result<(), Error> {
        let bytes = fs::read(snapshot_path)?;
        let rdb = RDB::try_from(bytes)?;
        for (_db, db_hash_map) in rdb.db_hash_maps {
            for (key, value) in db_hash_map {
                match value.value_type {
                    ValueType::String(string) => {
                        self.dictionary
                            .set(key.as_str(), string, None, false, None)?;
                    }
                    ValueType::List(list) => {
                        self.dictionary
                            .left_push(key.as_str(), list.into_iter().map(Cow::Owned).collect())?;
                    }
                    ValueType::Set(set) => {
                        todo!()
                    }
                };
                if let Some(timestamp) = value.expires_at {
                    self.dictionary
                        .expire(key.as_str(), timestamp.seconds() as u64, None);
                }
            }
        }

        self.last_save = SystemTime::now();
        Ok(())
    }

    pub fn save(&mut self) -> Result<(), Value> {
        let rdb = self.dictionary.snapshot();
        let bytes = Vec::<u8>::try_from(&rdb)?;

        println!("Creating new save file");
        fs::write("dump.rdb", bytes.as_slice())?;

        println!("Replacing old save file");
        fs::rename("dump.rdb", "save.rdb")?;

        self.last_save = SystemTime::now();

        Ok(())
    }

    pub fn background_save(&mut self, scheduled: bool) -> Result<bool, Value> {
        while let Some(result) = self.background_jobs.pop_if_done() {
            result?;
        }

        match self.background_jobs.pop_if_scheduled() {
            Some(_) => {
                return if scheduled {
                    self.background_jobs.push_scheduled();
                    Ok(false)
                } else {
                    Err(Value::simple_error(
                        "There is already a save process running",
                    ))
                };
            }
            None => {}
        }

        let process_id = self.do_background_save()?;
        self.background_jobs.push_active(process_id);
        Ok(true)
    }

    pub fn do_jobs(&mut self) -> io::Result<()> {
        while let Some(result) = self.background_jobs.pop_if_done() {
            result?;
        }

        if let None = self.background_jobs.pop_if_scheduled() {
            return Ok(());
        }

        let child_process_id = self.do_background_save()?;
        self.background_jobs.push_active(child_process_id);

        Ok(())
    }

    fn do_background_save(&mut self) -> io::Result<pid_t> {
        println!("Begin background save");
        unsafe {
            let process_id = libc::fork();
            match process_id {
                ..0 => {
                    return Err(io::Error::last_os_error());
                }
                0 => {}
                1.. => {
                    println!("forked child process {}", process_id);
                    return Ok(process_id);
                }
            }
        }

        match self.save() {
            Ok(()) => {
                println!("Created save file")
            }
            Err(err) => {
                println!("Failed to create save file {}", err)
            }
        }

        println!("exit child_process");
        unsafe { libc::_exit(0) }
    }
}

enum JobStatus {
    Scheduled,
    Active,
    Done,
}

#[derive(Default)]
struct JobQueue {
    inner: VecDeque<BackgroundJob>,
}

impl JobQueue {
    pub fn push_scheduled(&mut self) {
        self.inner.push_back(BackgroundJob::scheduled())
    }

    pub fn push_active(&mut self, process_id: pid_t) {
        self.inner.push_front(BackgroundJob::active(process_id))
    }

    fn pop_if_done(&mut self) -> Option<io::Result<BackgroundJob>> {
        if let Some(job) = self.inner.front() {
            return match job.status() {
                Ok(JobStatus::Done) => self.inner.pop_front().map(|job| Ok(job)),
                Err(err) => Some(Err(err)),
                _ => None,
            };
        }

        None
    }

    fn pop_if_scheduled(&mut self) -> Option<BackgroundJob> {
        if let Some(job) = self.inner.front() {
            return if job.process_id.is_some() {
                None
            } else {
                self.inner.pop_front()
            };
        }

        None
    }
}
struct BackgroundJob {
    process_id: Option<pid_t>,
}
impl BackgroundJob {
    fn scheduled() -> BackgroundJob {
        BackgroundJob { process_id: None }
    }
    fn active(process_id: pid_t) -> BackgroundJob {
        BackgroundJob {
            process_id: Some(process_id),
        }
    }

    fn status(&self) -> io::Result<JobStatus> {
        let process_id = match self.process_id {
            Some(process_id) => process_id,
            None => return Ok(JobStatus::Scheduled),
        };

        let result = unsafe { libc::kill(process_id, 0 as c_int) };
        if result < 0 {
            let err = io::Error::last_os_error();
            return match err.kind() {
                io::ErrorKind::NotFound => Ok(JobStatus::Done),
                _ => Err(err),
            };
        }
        if result == 0 {
            return Ok(JobStatus::Active);
        }

        panic!("kill(2) should return 0 or -1, but returned {}", result)
    }
}
