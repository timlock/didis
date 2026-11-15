use crate::async_io::driver::Driver;
use log::{error, info, warn};
use std::cell::RefCell;
use std::pin::{Pin, pin};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io, thread};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);

thread_local! {
    pub static CURRENT: RefCell<Option<Ctx> > =  RefCell::new(None) ;
}

pub struct Ctx {
    pub scheduler: Arc<Mutex<Scheduler>>,
    executor: Executor,
    pub driver: RefCell<Driver>,
}

struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    executor_channel: Sender<Arc<Task>>,
}
impl Task {
    fn poll(self: Arc<Self>) -> Poll<()> {
        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);

        let mut task_future = self.future.try_lock().unwrap();

        task_future.as_mut().poll(&mut cx)
    }

    fn spawn<F>(future: F, executor_channel: Sender<Arc<Task>>) -> TaskHandle<F::Output>
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
            executor_channel: executor_channel.clone(),
        });

        match executor_channel.send(task) {
            Ok(()) => {}
            Err(err) => {
                error!("Wake failed, Executor closed channel {err:?}")
            }
        }

        TaskHandle {
            receiver: handle_receiver,
            waker: handle_waker,
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        match self.executor_channel.send(self.clone()) {
            Ok(()) => {}
            Err(err) => {
                error!("Wake failed, Executor closed channel {err:?}")
            }
        }
    }
}

pub struct TaskHandle<T> {
    receiver: Receiver<T>,
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

struct Executor {
    task_queue: Receiver<Arc<Task>>,
}
impl Executor {
    pub fn new() -> (Executor, Sender<Arc<Task>>) {
        let (sender, receiver) = channel();

        (
            Executor {
                task_queue: receiver,
            },
            sender,
        )
    }

    // TODO limit tasks to poll to prevent io starvation?
    pub fn try_poll(&self) {
        loop {
            match self.task_queue.try_recv() {
                Ok(task) => {
                     let _ = task.poll();
                }
                Err(_) => break,
            }
        }
    }
}

#[derive(Default)]
pub struct Scheduler {
    executors: Vec<Sender<Arc<Task>>>,
    next: usize,
}

impl Scheduler {
    fn register(&mut self, executor_handle: Sender<Arc<Task>>) {
        self.executors.push(executor_handle);
    }

    fn next(&mut self) -> Sender<Arc<Task>> {
        let handle = &self.executors[self.next];
        self.next = (self.next + 1) % self.executors.len();
        handle.clone()
    }
}

pub struct Runtime {
    join_handles: Vec<(Sender<()>, JoinHandle<()>)>,
}

impl Runtime {
    pub fn new(io_queue_depth: usize, threads: usize) -> io::Result<Runtime> {
        let scheduler = Arc::new(Mutex::new(Scheduler::default()));
        let driver = RefCell::new(Driver::new(io_queue_depth)?);
        let (executor, handle) = Executor::new();
        scheduler.clone().lock().unwrap().register(handle);
        CURRENT.set(Some(Ctx {
            scheduler: scheduler.clone(),
            executor,
            driver,
        }));

        let (sender, receiver) = mpsc::channel();

        let mut join_handles = Vec::with_capacity(threads);
        for i in 0..threads {
            let sender = sender.clone();
            let scheduler = scheduler.clone();

            let join_handle = thread::spawn(move || {
                let driver = match Driver::new(io_queue_depth) {
                    Ok(driver) => RefCell::new(driver),
                    Err(err) => {
                        error!("Could not create IO driver {err}");
                        return;
                    }
                };
                let (executor, handle) = Executor::new();
                scheduler.lock().unwrap().register(handle);
                CURRENT.set(Some(Ctx {
                    scheduler,
                    executor,
                    driver,
                }));

                let (stop_sender, stop_receiver) = channel();

                info!("Launched executor {i}");
                sender.send(stop_sender).unwrap();
                loop {
                    match stop_receiver.try_recv() {
                        Ok(()) | Err(TryRecvError::Disconnected) => break,
                        Err(TryRecvError::Empty) => {}
                    };
                    let result = CURRENT.with_borrow(|ctx| {
                        let ctx = ctx.as_ref().unwrap();
                        ctx.executor.try_poll();
                        ctx.driver.borrow_mut().poll_timeout(POLL_TIMEOUT)
                    });

                    if let Err(err) = result {
                        warn!("Executor error {err}")
                    }
                }

                info!("Stopping executor {i}");
            });

            let stop_sender = match receiver.recv_timeout(Duration::from_secs(5)) {
                Ok(s) => s,
                Err(_) => {
                    if join_handle.is_finished() {
                        match join_handle.join() {
                            Ok(()) => panic!("Executor {i} finished early without panic"),
                            Err(err) => panic!("Executor {i} panic'ed {err:?}"),
                        }
                    } else {
                        panic!("Executor {i} closed stop channel but is still running");
                    }
                }
            };
            join_handles.push((stop_sender, join_handle));
        }

        Ok(Runtime { join_handles })
    }

    pub fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        CURRENT.with_borrow(|ctx| {
            let ctx = ctx.as_ref().unwrap();
            let mut context = Context::from_waker(&Waker::noop());

            let mut future = pin!(future);

            loop {
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(result) => return result,
                    Poll::Pending => {}
                }

                ctx.executor.try_poll();

                let result = ctx.driver.borrow_mut().poll_timeout(POLL_TIMEOUT);
                if let Err(err) = result {
                    warn!("Executor error {err}")
                }
            }
        })
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        for (i, (stop_sender, join_handle)) in self.join_handles.drain(..).enumerate() {
            match stop_sender.send(()) {
                Ok(()) => {}
                Err(err) => {
                    warn!("Can't send stop message to executor {i} {err}");
                }
            }
            match join_handle.join() {
                Ok(()) => {}
                Err(err) => {
                    warn!("Executor has panicked {err:?}");
                }
            }
        }
    }
}

pub fn spawn<F>(future: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    CURRENT.with_borrow(|ctx| {
        Task::spawn(
            future,
            ctx.as_ref().unwrap().scheduler.lock().unwrap().next(),
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::async_io::runtime::{Runtime, spawn};
    use crate::logger;
    use log::Level;
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
    fn wait_for_timer() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Runtime::new(256, 0)?;

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
        let runtime = Runtime::new(256, 0)?;

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
        let runtime = Runtime::new(256, 0)?;

        let task_handle = spawn(async move { 1 });

        runtime.block_on(async {
            let result = task_handle.await;
            assert_eq!(1, result);
        });

        Ok(())
    }

    #[test]
    fn nested_spawn() -> Result<(), Box<dyn std::error::Error>> {
        let _ = logger::init(Level::Info);

        let runtime = Runtime::new(256, 0)?;

        runtime.block_on(async {
            let result =
                spawn(async { 1 + spawn(async { 2 + spawn(async { 3 }).await }).await }).await;
            assert_eq!(6, result);
        });

        Ok(())
    }
}
