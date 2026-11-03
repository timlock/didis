use std::future::poll_fn;
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::{RecvError, SendError};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (inner_sender, inner_receiver) = std::sync::mpsc::channel();
    let waker = Arc::new(Mutex::new(None));
    
    let sender = Sender {
        inner: inner_sender,
        receiver_waker: waker.clone(),
    };
    let receiver = Receiver {
        inner: inner_receiver,
        waker: waker.clone(),
    };
    
    (sender, receiver)
}

pub struct Receiver<T> {
    inner: std::sync::mpsc::Receiver<T>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Receiver<T> {
    pub async fn recv(&self) -> Result<T, RecvError> {
        poll_fn(|cx| match self.inner.try_recv() {
            Ok(value) => Poll::Ready(Ok(value)),
            Err(TryRecvError::Disconnected) => Poll::Ready(Err(RecvError {})),
            Err(TryRecvError::Empty) => {
                let new_waker = cx.waker();
                if self
                    .waker
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_none_or(|current_waker| !current_waker.will_wake(new_waker))
                {
                    self.waker.lock().unwrap().replace(new_waker.clone());
                }
                Poll::Pending
            }
        })
        .await
    }
}

#[derive(Clone)]
pub struct Sender<T> {
    inner: std::sync::mpsc::Sender<T>,
    receiver_waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.inner.send(value)?;
        self.receiver_waker
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .wake_by_ref();
        Ok(())
    }
}
