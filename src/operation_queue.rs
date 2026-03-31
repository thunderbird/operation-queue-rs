/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

//! This module defines the types and data structures for the operation queue.
//! See the crate's top-level documentation.

use std::{
    cell::{Cell, RefCell},
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::Arc,
};

use async_channel::{Receiver, Sender};

use crate::error::Error;

/// An operation that can be added to an [`OperationQueue`].
#[allow(async_fn_in_trait)]
pub trait QueuedOperation: Debug {
    /// Performs the operation asynchronously.
    async fn perform(&self);
}

/// A dyn-compatible version of [`QueuedOperation`]. It is implemented for all
/// types that implement [`QueuedOperation`].
///
/// [`ErasedQueuedOperation`] makes [`QueuedOperation`] dyn-compatible by
/// wrapping the opaque [`Future`] returned by `perform` into a [`Box`], which
/// is essentially an owned pointer and which size is known at compile time.
/// This makes `perform` dispatchable from a trait object.
///
/// This return value is further wrapped into a [`Pin`] so that the `Future` can
/// be `await`ed (since the receiver for [`Future::poll`] is `Pin<&mut Self>`).
///
/// In this context, "erased" refers to how this trait "erases" the
/// opaque/generic return type of [`QueuedOperation::perform`] by turning it
/// into a trait object.
pub trait ErasedQueuedOperation: Debug {
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + 'op>>;
}

impl<T> ErasedQueuedOperation for T
where
    T: QueuedOperation,
{
    fn perform<'op>(&'op self) -> Pin<Box<dyn Future<Output = ()> + 'op>> {
        Box::pin(self.perform())
    }
}

/// A queue that performs asynchronous operations in order.
pub struct OperationQueue {
    channel_sender: Sender<Box<dyn ErasedQueuedOperation>>,
    channel_receiver: Receiver<Box<dyn ErasedQueuedOperation>>,
    runners: RefCell<Vec<Arc<Runner>>>,
    spawn_task: fn(fut: Box<dyn Future<Output = ()>>),
}

impl OperationQueue {
    /// Creates a new operation queue.
    ///
    /// The function provided as argument is used when spawning new runners,
    /// e.g. `tokio::task::spawn`. It must not be blocking.
    ///
    /// Since most methods require the queue to be wrapped inside an [`Arc`],
    /// this is how this method returns the new queue.
    // See the design consideration section from the top of this file regarding
    // the use of `Arc`.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(spawn_task: fn(fut: Box<dyn Future<Output = ()>>)) -> Arc<OperationQueue> {
        let (snd, rcv) = async_channel::unbounded();

        let queue = OperationQueue {
            channel_sender: snd,
            channel_receiver: rcv,
            runners: RefCell::new(Vec::new()),
            spawn_task,
        };

        Arc::new(queue)
    }

    /// Starts the given number of runners that consume new items pushed to the
    /// queue.
    ///
    /// A runner loops infinitely, performing operations as they get queued.
    ///
    /// An error can be returned if the queue has previously been stopped.
    pub fn start(&self, runners: u32) -> Result<(), Error> {
        if self.channel_sender.is_closed() {
            return Err(Error::Stopped);
        }

        for i in 0..runners {
            let runner = Runner::new(i, self.channel_receiver.clone());
            (self.spawn_task)(Box::new(runner.clone().run()));
            self.runners.borrow_mut().push(runner);
        }

        Ok(())
    }

    /// Pushes an operation to the back of the queue.
    ///
    /// An error can be returned if the queue has been stopped.
    pub async fn enqueue(&self, op: Box<dyn ErasedQueuedOperation>) -> Result<(), Error> {
        self.channel_sender.send(op).await?;
        Ok(())
    }

    /// Stops the queue.
    ///
    /// Operations that have already been queued up will still be performed, but
    /// any call to [`enqueue`] following a call to `stop` will fail.
    ///
    /// [`enqueue`]: OperationQueue::enqueue
    pub async fn stop(&self) {
        if !self.channel_sender.close() {
            log::warn!("request queue: attempted to close channel that's already closed");
        }

        // Clear the references we have on the runners, so they can be dropped
        // when they finish running.
        self.runners.borrow_mut().clear();
    }

    /// Checks whether one or more runner(s) is currently active.
    ///
    /// If a runner has been created but isn't running yet, it is still included
    /// in this count. Thus a runner being active means it's in any state other
    /// than fully stopped.
    ///
    /// This method also returns `false` if there aren't any runners (e.g. if
    /// the queue hasn't been started yet, or it has been stopped).
    pub fn running(&self) -> bool {
        // Count every runner that's not permanently stopped. This should be
        // fine, since the only places we mutably borrow `self.runners` are
        // `start` and `stop` and:
        //  * both `start`, `stop` and `running` are expected to be run in the
        //    same thread/routine, and
        //  * both are synchronous functions so there should be no risk of one
        //    happening while the other waits.
        let active_runners =
            self.count_matching_runners(|runner| !matches!(runner.state(), RunnerState::Stopped));

        log::debug!("{active_runners} runner(s) currently active");

        // Check if there's at least one runner currently active.
        active_runners > 0
    }

    /// Checks whether all runners are currently waiting for a new operation to
    /// perform.
    pub fn idle(&self) -> bool {
        // Count every runner that's waiting for a new operation to perform.
        // This should be fine, since the only places we mutably borrow
        // `self.runners` are `start` and `stop` and:
        //  * both `start`, `stop` and `idle` are expected to be run in the
        //    thread/routine, and
        //  * both are synchronous functions so there should be no risk of one
        //    happening while the other waits.
        let idle_runners =
            self.count_matching_runners(|runner| matches!(runner.state(), RunnerState::Waiting));

        log::debug!("{idle_runners} runner(s) currently idle");

        // If `self.runner` was being mutably borrowed here, we would have
        // already panicked when calling `self.count_matching_runners()`.
        idle_runners == self.runners.borrow().len()
    }

    /// Counts the number of runners matching the given closure. The type of the
    /// closure is the same that would be used by [`Iterator::filter`].
    ///
    /// # Panics
    ///
    /// This method will panic if it's called while `self.runners` is being
    /// mutably borrowed.
    fn count_matching_runners<PredicateT>(&self, predicate: PredicateT) -> usize
    where
        PredicateT: FnMut(&&Arc<Runner>) -> bool,
    {
        self.runners.borrow().iter().filter(predicate).count()
    }
}

/// The status of a runner.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RunnerState {
    /// The runner has been created but isn't running yet.
    Pending,

    /// The runner is currently waiting for an operation to perform.
    Waiting,

    /// The runner is currently performing an operation.
    Running,

    /// The runner has finished performing its last operation and has exited its
    /// main loop.
    Stopped,
}

/// A runner created and run by the [`OperationQueue`].
///
/// Each runner works by entering an infinite loop upon calling [`Runner::run`],
/// which is only exited when the queue's channel is closed and has been
/// emptied.
///
/// The current state of the runner can be checked at any time with
/// [`Runner::state`].
struct Runner {
    receiver: Receiver<Box<dyn ErasedQueuedOperation>>,
    state: Cell<RunnerState>,

    // A numerical identifier attached to the current runner, used for
    // debugging.
    id: u32,
}

impl Runner {
    /// Creates a new [`Runner`], wrapped into an [`Arc`].
    ///
    /// `id` is a numerical identifier used for debugging.
    // See the design consideration section from the top of this file regarding
    // the use of `Arc`.
    #[allow(clippy::arc_with_non_send_sync)]
    fn new(id: u32, receiver: Receiver<Box<dyn ErasedQueuedOperation>>) -> Arc<Runner> {
        Arc::new(Runner {
            id,
            receiver,
            state: Cell::new(RunnerState::Pending),
        })
    }

    /// Starts a loop that waits for new operations to come down the inner
    /// channel and performs them.
    ///
    /// This method does not explicitly take care of sharing the operation's
    /// response to the consumer; this is expected to be done by
    /// [`QueuedOperation::perform`].
    async fn run(self: Arc<Runner>) {
        loop {
            self.state.replace(RunnerState::Waiting);

            let op = match self.receiver.recv().await {
                Ok(op) => op,
                Err(_) => {
                    log::info!(
                        "request queue: channel has closed (likely due to client shutdown), exiting the loop"
                    );
                    self.state.replace(RunnerState::Stopped);
                    return;
                }
            };

            self.state.replace(RunnerState::Running);

            log::info!(
                "operation_queue::Runner: runner {} performing op: {op:?}",
                self.id
            );

            op.perform().await;
        }
    }

    /// Gets the runner's current state.
    fn state(&self) -> RunnerState {
        self.state.get()
    }
}
