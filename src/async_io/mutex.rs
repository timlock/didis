use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};

pub struct Mutex<T: ?Sized> {
    semaphore: Semaphore,
    value: UnsafeCell<T>,
}

impl<T> Mutex<T> {
    pub fn new(value: T) -> Mutex<T> {
        Mutex {
            value: UnsafeCell::new(value),
            semaphore: Semaphore::new(1),
        }
    }

    pub async fn lock(&self) -> MutexGuard<'_, T> {
        let permit = self.semaphore.acquire().await;

        MutexGuard {
            permit,
            mutex: self,
        }
    }

    pub fn try_lock(&self) -> Result<MutexGuard<'_, T>, TryLockError> {
        let permit = self.semaphore.try_acquire().map_err(|_| TryLockError)?;

        Ok(MutexGuard {
            permit,
            mutex: self,
        })
    }
}

unsafe impl<T> Send for Mutex<T> where T: ?Sized + Send {}

unsafe impl<T> Sync for Mutex<T> where T: ?Sized + Send {}

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub struct TryLockError;

pub struct MutexGuard<'a, T> {
    mutex: &'a Mutex<T>,
    permit: Permit<'a>,
}

impl<'a, T> Deref for MutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.mutex.value.get().as_ref_unchecked() }
    }
}

impl<'a, T> DerefMut for MutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.mutex.value.get().as_mut_unchecked() }
    }
}

pub struct Semaphore {
    remaining_permits: AtomicUsize,
    queue: std::sync::Mutex<VecDeque<(usize, Waker)>>,
}

impl Semaphore {
    pub fn new(n: usize) -> Semaphore {
        Semaphore {
            remaining_permits: AtomicUsize::new(n),
            queue: std::sync::Mutex::new(VecDeque::new()),
        }
    }
    pub async fn acquire(&self) -> Permit<'_> {
        self.remove_permits(1).await;

        Permit { semaphore: self }
    }

    pub fn try_acquire(&self) -> Result<Permit<'_>, TryAcquireError> {
        match self
            .remaining_permits
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            }) {
            Ok(_) => Ok(Permit { semaphore: self }),

            Err(_) => Err(TryAcquireError),
        }
    }

    pub fn add(&self, n: usize) {
        let available =
            self.remaining_permits
                .update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining + n
                })
                + n;

        let mut queue = self.queue.lock().unwrap();

        if let Some((needs, _)) = queue.front()
            && *needs <= available
        {
            let (_, waker) = queue
                .pop_front()
                .expect("expect VecDequeue to have an entry when front() return Some(..)");
            drop(queue);
            waker.wake_by_ref();
        }
    }

    fn remove_permits(&self, n: usize) -> impl Future<Output = ()> {
        poll_fn(move |cx| {
            match self.remaining_permits.try_update(
                Ordering::SeqCst,
                Ordering::SeqCst,
                |remaining| remaining.checked_sub(n),
            ) {
                Ok(_) => Poll::Ready(()),
                Err(_) => {
                    self.queue
                        .lock()
                        .unwrap()
                        .push_back((n, cx.waker().clone()));
                    Poll::Pending
                }
            }
        })
    }
}

#[derive(Debug, PartialOrd, PartialEq, Copy, Clone)]
pub struct TryAcquireError;

pub struct Permit<'a> {
    semaphore: &'a Semaphore,
}

impl<'a> Drop for Permit<'a> {
    fn drop(&mut self) {
        self.semaphore.add(1)
    }
}

#[cfg(test)]
mod tests {
    use crate::async_io::mpsc::channel;
    use crate::async_io::mutex::{Mutex, TryLockError};
    use crate::async_io::runtime::{Runtime, spawn};
    use std::sync::Arc;

    #[test]
    fn concurrent_addition() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Runtime::new(256, 8)?;
        runtime.block_on(async {
            let (sender, receiver) = channel();

            let counter = Arc::new(Mutex::new(0));
            let tasks = 100;

            for _ in 0..tasks {
                let sender = sender.clone();
                let counter = counter.clone();
                spawn(async move {
                    for _ in 0..100 {
                        let mut c = counter.lock().await;
                        *c += 1;
                    }

                    sender.send(()).unwrap();
                });
            }

            for _ in 0..tasks {
                receiver.recv().await.unwrap();
            }

            assert_eq!(10000, *counter.try_lock().unwrap());
        });
        Ok(())
    }

    #[test]
    fn locked_mutex_blocks_caller() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = Runtime::new(256, 8)?;
        runtime.block_on(async {
            let (sender, receiver) = channel();

            let flag = Arc::new(Mutex::new(false));
            let guard = flag.try_lock().unwrap();

            let flag_clone = flag.clone();
            spawn(async move {
                assert_eq!(Some(TryLockError), flag_clone.try_lock().err());
                sender.send(()).unwrap();

                let mut guard = flag_clone.lock().await;
                *guard = true;
                sender.send(()).unwrap();
            });

            receiver.recv().await.unwrap();
            drop(guard);
            receiver.recv().await.unwrap();
            assert_eq!(Some(true), flag.try_lock().ok().map(|b| *b))
        });
        Ok(())
    }
}
