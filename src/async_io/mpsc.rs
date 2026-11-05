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
        inner: Mutex::new(inner_receiver),
        waker: waker.clone(),
    };

    (sender, receiver)
}

pub struct Receiver<T> {
    inner:  Mutex<std::sync::mpsc::Receiver<T>>, // TODO check if mutex is necessary
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Receiver<T> {
    pub async fn recv(&self) -> Result<T, RecvError> {
        poll_fn(|cx| match self.inner.lock().unwrap().try_recv() {
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

#[cfg(test)]
mod tests {
    use crate::async_io::driver::IO;
    use crate::async_io::mpsc::channel;
    use crate::async_io::runtime::{select, select_pair, spawn, Runtime, SelectResult};
    use std::pin::{Pin, pin};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn wait_for_receive() -> Result<(), Box<dyn std::error::Error>> {
        let (sender, receiver) = channel();
        let runtime = Runtime::new(IO::new(256)?);

        let thread_handle = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            sender.send(1).unwrap();
        });

        let received = runtime.block_on(async move { receiver.recv().await });

        assert_eq!(Ok(1), received);
        thread_handle.join().unwrap();
        Ok(())
    }

    #[test]
    fn select_receive() -> Result<(), Box<dyn std::error::Error>> {
        let (sender1, receiver1) = channel();
        let (sender2, receiver2) = channel();
        let (sender3, receiver3) = channel();
        let runtime = Runtime::new(IO::new(256)?);

        let thread_handle = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            sender3.send(3).unwrap();
        });

        let received = runtime.block_on(async move {
            select(&mut [
                pin!(receiver1.recv()),
                pin!(receiver2.recv()),
                pin!(receiver3.recv()),
            ])
            .await
        });

        assert_eq!(Ok(3), received);
        thread_handle.join().unwrap();
        Ok(())
    }

    #[test]
    fn select_pair_receive() -> Result<(), Box<dyn std::error::Error>> {
        let (sender1, receiver1) = channel::<i32>();
        let (sender2, receiver2) = channel();
        let runtime = Runtime::new(IO::new(256)?);

        let thread_handle = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            sender2.send(2).unwrap();
        });

        let received =
            runtime.block_on(async move { select_pair(receiver1.recv(), receiver2.recv()).await });

        assert_eq!(SelectResult::Second(Ok(2)), received);
        thread_handle.join().unwrap();
        Ok(())
    }
}
