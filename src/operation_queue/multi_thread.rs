/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//! `Send`-safe variant of the queue's operation traits, used when the `send`
//! feature is enabled.

use std::{fmt::Debug, future::Future, pin::Pin};

/// An operation that can be added to an [`OperationQueue`](crate::OperationQueue).
/// `Send`-safe counterpart to the non-`send`-feature `QueuedOperation`, for
/// use with e.g. `tokio::spawn` on a multi-threaded runtime.
pub trait QueuedOperation: Debug + Send {
    fn perform(&self) -> impl Future<Output = ()> + Send;
}

/// A dyn-compatible version of [`QueuedOperation`], implemented for all types
/// that implement it. See `local_thread::ErasedQueuedOperation` for why this
/// is necessary.
pub trait ErasedQueuedOperation: Debug + Send {
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + Send + 'op>>;
}

impl<T> ErasedQueuedOperation for T
where
    T: QueuedOperation,
{
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + Send + 'op>> {
        Box::pin(QueuedOperation::perform(self))
    }
}

/// The type used to spawn a runner's loop, e.g. `tokio::spawn`. Must not be
/// blocking.
pub(crate) type SpawnTaskFn = fn(Pin<Box<dyn Future<Output = ()> + Send>>);

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{Arc, Condvar, Mutex},
        thread::{self, ThreadId},
        time::Duration,
    };

    use async_channel::Sender;

    use super::QueuedOperation;
    use crate::OperationQueue;

    fn assert_send<T: Send>() {}

    #[test]
    fn operation_queue_is_send() {
        assert_send::<crate::OperationQueue>();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn operations_run_in_parallel_across_threads() {
        const RUNNERS: usize = 4;
        const TIMEOUT: Duration = Duration::from_secs(5);

        #[derive(Debug)]
        struct Operation {
            arrivals: Arc<(Mutex<usize>, Condvar)>,
            sender: Sender<(ThreadId, bool)>,
        }
        impl QueuedOperation for Operation {
            async fn perform(&self) {
                // Block this thread until every operation is running, so each
                // one must be on its own thread. `block_in_place` lets the
                // other runners carry on meanwhile.
                let result = tokio::task::block_in_place(|| {
                    let (count, all_arrived) = &*self.arrivals;
                    let mut count = count.lock().unwrap();
                    *count += 1;
                    all_arrived.notify_all();
                    let (count, _) = all_arrived
                        .wait_timeout_while(count, TIMEOUT, |count| *count < RUNNERS)
                        .unwrap();
                    (thread::current().id(), *count == RUNNERS)
                });

                // This only fails if the test has already failed.
                _ = self.sender.send(result).await;
            }
        }

        let queue = OperationQueue::new(|fut| {
            _ = tokio::spawn(fut);
        });
        queue.start(RUNNERS as u32).unwrap();

        let arrivals = Arc::new((Mutex::new(0), Condvar::new()));
        let (sender, receiver) = async_channel::unbounded();

        for _ in 0..RUNNERS {
            queue
                .enqueue(Box::new(Operation {
                    arrivals: arrivals.clone(),
                    sender: sender.clone(),
                }))
                .await
                .unwrap();
        }

        let mut threads = HashSet::new();
        for _ in 0..RUNNERS {
            let (thread_id, all_arrived) = tokio::time::timeout(TIMEOUT * 2, receiver.recv())
                .await
                .expect("timed out waiting for an operation to be performed")
                .unwrap();
            assert!(all_arrived, "operations did not all run at the same time");
            threads.insert(thread_id);
        }

        assert_eq!(threads.len(), RUNNERS, "operations shared a thread");
        assert!(
            !threads.contains(&thread::current().id()),
            "an operation ran on the test's thread"
        );

        queue.stop().await;
    }
}
