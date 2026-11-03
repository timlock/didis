use crate::async_io::driver::{Driver, IO};
use log::warn;
use std::cell::RefCell;
use std::pin::{Pin, pin};
use std::sync::mpsc::TryRecvError;
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::thread::Thread;
use std::time::Duration;

thread_local! {
    pub static CURRENT: RefCell<Option<Executor> > =  RefCell::new(None) ;
}

struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    scheduler: Scheduler,
}
impl Task {
    fn poll(self: Arc<Self>) -> Poll<()> {
        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);

        let mut task_future = self.future.try_lock().unwrap();

        task_future.as_mut().poll(&mut cx)
    }

    fn spawn<F>(future: F, scheduler: Scheduler) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (handle_sender, handle_receiver) = mpsc::channel();
        let handle_waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
        let handle_waker_clone = handle_waker.clone();

        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(async move {
                let result = future.await;

                // error can be ignored because the handle might have been dropped
                let _ = handle_sender.send(result);

                let waker = handle_waker_clone.lock().unwrap();
                if let Some(waker) = waker.as_ref() {
                    waker.wake_by_ref();
                }
            })),
            scheduler: scheduler.clone(),
        });

        let _ = scheduler.sender.send(task);

        TaskHandle {
            receiver: handle_receiver,
            waker: handle_waker,
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.scheduler.schedule(self.clone())
    }
}

struct ThreadWaker(Thread);
impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

pub struct TaskHandle<T> {
    receiver: mpsc::Receiver<T>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Future for TaskHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let new_waker = cx.waker();
        let mut current_waker = self.waker.lock().unwrap();

        if current_waker
            .as_ref()
            .is_some_and(|current_waker| current_waker.will_wake(new_waker))
            || current_waker.is_none()
        {
            *current_waker = Some(new_waker.clone());
        }

        match self.receiver.try_recv() {
            Ok(result) => Poll::Ready(result),
            Err(TryRecvError::Empty) => Poll::Pending,
            Err(TryRecvError::Disconnected) => todo!(),
        }
    }
}

pub struct Executor {
    scheduler: Scheduler,
    scheduled: mpsc::Receiver<Arc<Task>>,
    pub driver: RefCell<Driver>,
}
impl Executor {
    pub fn new(driver: RefCell<Driver>) -> Executor {
        let (sender, scheduled) = mpsc::channel();
        Executor {
            scheduled,
            scheduler: Scheduler {
                executor_thread: thread::current(),
                sender,
            },
            driver,
        }
    }
}

pub struct Runtime;

impl Runtime {
    pub fn new(io: IO) -> Runtime {
        let executor = Executor::new(RefCell::new(Driver::new(io)));
        CURRENT.set(Some(executor));
        Runtime
    }
    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        CURRENT.with(|executor| {
            let thread_waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
            let mut context = Context::from_waker(&thread_waker);

            let mut future = pin!(future);

            loop {
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(result) => return result,
                    Poll::Pending => {}
                }

                let tasks = executor
                    .borrow_mut()
                    .as_ref()
                    .unwrap()
                    .scheduled
                    .try_iter()
                    .collect::<Vec<_>>();
                for task in tasks {
                    let _ = task.poll();
                }

                let result = executor
                    .borrow_mut()
                    .as_ref()
                    .unwrap()
                    .driver
                    .borrow_mut()
                    .poll_timeout(Duration::from_secs(1));
                
                if let Err(err) = result {
                    warn!("Poll error {err}")
                }
            }
        })
    }
}

pub fn spawn<F>(future: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    CURRENT
        .with_borrow(|executor| Task::spawn(future, executor.as_ref().unwrap().scheduler.clone()))
}

#[derive(Clone)]
struct Scheduler {
    executor_thread: Thread,
    sender: mpsc::Sender<Arc<Task>>,
}

impl Scheduler {
    fn schedule(&self, task: Arc<Task>) {
        self.sender.send(task).unwrap();
        self.executor_thread.unpark();
    }
}

#[cfg(test)]
mod tests {
    use crate::async_io::driver::IO;
    use crate::async_io::runtime::{Runtime, spawn};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::thread;
    use std::time::{Duration, Instant};

    struct Delay {
        when: Instant,
    }

    impl Future for Delay {
        type Output = &'static str;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<&'static str> {
            if Instant::now() >= self.when {
                Poll::Ready("done")
            } else {
                // Get a handle to the waker for the current task
                let waker = cx.waker().clone();
                let when = self.when;

                // Spawn a timer thread.
                thread::spawn(move || {
                    let now = Instant::now();

                    if now < when {
                        thread::sleep(when - now);
                    }

                    waker.wake();
                });

                Poll::Pending
            }
        }
    }

    #[test]
    fn test() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Runtime::new(IO::new(256)?);

        runtime.block_on(async {
            let when = Instant::now() + Duration::from_millis(10);
            let future = Delay { when };

            let out = future.await;
            assert_eq!(out, "done");

            println!("Done 1");

            let when = Instant::now() + Duration::from_millis(10);
            let future = Delay { when };

            let out = future.await;
            assert_eq!(out, "done");
        });

        Ok(())
    }

    #[test]
    fn block_on_continues_other_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Runtime::new(IO::new(256)?);

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = counter.clone();

        spawn(async move {
            *counter_clone.lock().unwrap() = 1;
        });

        runtime.block_on(async {
            let when = Instant::now() + Duration::from_millis(10);
            let future = Delay { when };

            let out = future.await;
            assert_eq!(out, "done");
        });

        assert_eq!(1i32, *counter.lock().unwrap());

        Ok(())
    }

    #[test]
    fn join_task() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Runtime::new(IO::new(256)?);

        let task_handle = spawn(async move { 1 });

        runtime.block_on(async {
            let result = task_handle.await;
            assert_eq!(1, result);
        });

        Ok(())
    }

    #[test]
    fn nested_spawn() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Runtime::new(IO::new(256)?);

        runtime.block_on(async {
            let task_handle =
                spawn(async { 1 + spawn(async { 2 + spawn(async { 3 }).await }).await });
            let result = task_handle.await;
            assert_eq!(6, result);
        });

        Ok(())
    }
}
