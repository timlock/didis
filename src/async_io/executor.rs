use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::thread::Thread;

struct Task {
    task_future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    executor: mpsc::Sender<Arc<Task>>,
    thread_waker: ThreadWaker,
}
impl Task {
    fn poll(self: Arc<Self>) -> Poll<()> {
        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);

        let mut task_future = self.task_future.try_lock().unwrap();

        task_future.as_mut().poll(&mut cx)
    }

    fn spawn<F>(future: F, executor_sender: &mpsc::Sender<Arc<Task>>) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (handle_sender, handle_receiver) = mpsc::channel();

        let handle_waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));

        let handle_waker_clone = handle_waker.clone();

        let task = Arc::new(Task {
            task_future: Mutex::new(Box::pin(async move {
                let result = future.await;

                // error can be ignored because the handle might have been dropped
                let _ = handle_sender.send(result);

                let waker = handle_waker_clone.lock().unwrap();
                if let Some(waker) = waker.as_ref() {
                    waker.wake_by_ref();
                }
            })),
            executor: executor_sender.clone(),
            thread_waker: ThreadWaker(thread::current()),
        });

        let _ = executor_sender.send(task);

        TaskHandle {
            receiver: handle_receiver,
            waker: handle_waker,
        }
    }

    fn schedule(self: &Arc<Self>) {
        self.executor.send(self.clone()).unwrap();
        self.thread_waker.0.unpark();
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.schedule();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule();
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
    type Output = Result<T, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let new_waker = cx.waker();
        let mut current_waker = self.waker.lock().unwrap();

        if current_waker.is_some() && current_waker.as_ref().unwrap().will_wake(new_waker)
            || current_waker.is_none()
        {
            *current_waker = Some(new_waker.clone());
        }

        match self.receiver.try_recv() {
            Ok(result) => Poll::Ready(Ok(result)),
            Err(_) => Poll::Pending,
        }
    }
}

pub struct Executor {
    scheduled: mpsc::Receiver<Arc<Task>>,
    sender: mpsc::Sender<Arc<Task>>,
}
impl Executor {
    pub fn new() -> Executor {
        let (sender, scheduled) = mpsc::channel();
        Executor { sender, scheduled }
    }
    pub fn spawn<F>(&mut self, future: F) -> TaskHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Task::spawn(future, &self.sender)
    }

    pub fn run(&mut self) {
        while let Ok(task) = self.scheduled.recv() {
            let _ = task.poll();
        }
    }

    pub fn block_on<F>(&mut self, future: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let thread_waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
        let mut context = Context::from_waker(&thread_waker);

        let mut future = pin!(future);

        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(result) => return result,
                Poll::Pending => {}
            }

            for task in self.scheduled.try_iter() {
                let _ = task.poll();
            }

            thread::park();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::async_io::executor::Executor;
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
                println!("Hello world");
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

                    println!("waking");
                    waker.wake();
                });

                Poll::Pending
            }
        }
    }

    #[test]
    fn test() -> Result<(), Box<dyn std::error::Error>> {
        let mut executor = Executor::new();

        executor.block_on(async {
            let when = Instant::now() + Duration::from_millis(10);
            let future = Delay { when };

            let out = future.await;
            assert_eq!(out, "done");

            println!("Done 1");

            let when = Instant::now() + Duration::from_millis(10);
            let future = Delay { when };

            let out = future.await;
            assert_eq!(out, "done");

            println!("Done 2");
        });

        Ok(())
    }

    #[test]
    fn block_on_continues_other_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let mut executor = Executor::new();

        let counter = Arc::new(Mutex::new(0));
        let counter_clone = counter.clone();

        executor.spawn(async move {
            *counter_clone.lock().unwrap() = 1;
        });

        executor.block_on(async {
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
        let mut executor = Executor::new();

        let task_handle = executor.spawn(async move {
            println!("sub task");
            1
        });

        executor.block_on(async {
            let result = task_handle.await;
            assert_eq!(1, result.unwrap());
        });

        Ok(())
    }
}
