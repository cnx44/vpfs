//! Task queue in front of `State`.
//!
//! Receivers (client gateway, peer handler, human resolver) never touch the
//! state: they submit a task, a closure over `&mut State`, and await its
//! result. This is the only place that decides *how* tasks run. Today: one
//! thread, tasks strictly one after the other. A different policy (per-path
//! queues, a pool with read/write locking, ...) only has to change this file.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::thread;

use tokio::sync::oneshot;

use super::state::{Effects, State};

type Task = Box<dyn FnOnce(&mut State) + Send>;

/// Handle to the task queue; cheap to clone. The state itself is owned by the worker thread.
#[derive(Clone)]
pub struct Executor {
    queue: mpsc::Sender<Task>,
}

impl Executor {
    /// Move `state` into a dedicated OS thread that runs tasks in submission order.
    /// Called once in `main`.
    pub fn spawn(mut state: State) -> Executor {
        let (queue, tasks) = mpsc::channel::<Task>();
        thread::spawn(move || {
            for task in tasks {
                // A panicking task drops its reply (the caller sees an error) but must not stop the node.
                let _ = catch_unwind(AssertUnwindSafe(|| task(&mut state)));
            }
        });
        Executor { queue }
    }

    /// Run `f` on the state; returns its result and the effects it produced.
    /// Effects are taken right after `f`, so they belong to this task only.
    /// If `f` panics, the node survives but this call panics in the caller.
    pub async fn run<R: Send + 'static>(&self, f: impl FnOnce(&mut State) -> R + Send + 'static) -> (R, Effects) {
        let (reply, result) = oneshot::channel();
        let task: Task = Box::new(move |state| {
            let r = f(state);
            let _ = reply.send((r, state.take_effects()));
        });
        self.queue.send(task).expect("executor stopped");
        result.await.expect("executor task panicked")
    }
}
