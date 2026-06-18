//! Main idea:
//!
//! "How does this arbitrary concurrent primitive scale?"
//! -> Counter, HashTable, Queue, RingBuffer, etc.

pub mod computable;

use crate::computable::Computable;
use std::{
    sync::{Arc, Barrier},
    thread::{self, JoinHandle},
};

// TODO: consider synchronization alternatives:
// - crossbeam WaitGroup
// - pure atomics with epoch/reset bookkeeping and parking (note: Idle CPU burn)
pub struct ThreadPool<C: Computable> {
    state: Arc<C>,

    workers: Vec<JoinHandle<()>>,

    start: Arc<Barrier>,
    done: Arc<Barrier>,
}

impl<C> ThreadPool<C>
where
    C: Computable + Send + Sync + 'static,
{
    pub fn new(state: C, threads_count: usize) -> Self {
        let mut workers = vec![];

        let start = Arc::new(Barrier::new(threads_count + 1));
        let done = Arc::new(Barrier::new(threads_count + 1));

        let state = Arc::new(state);

        for _ in 0..threads_count {
            let task = thread::spawn({
                let state = state.clone();

                let start = start.clone();
                let done = done.clone();

                move || {
                    loop {
                        // Will be unblocked when all threads is ready (signal commited from the run_batch).
                        start.wait();

                        'batch: loop {
                            let completed = state.compute_step();

                            if completed {
                                // Mark current thread as done
                                done.wait();
                                break 'batch;
                            }
                        }
                    }
                }
            });

            workers.push(task);
        }

        ThreadPool {
            state,
            workers,
            start,
            done,
        }
    }

    // reset, start, wait for done
    pub fn run_batch(&self) {
        self.state.reset();

        // Gives start signal for a workers.
        self.start.wait();

        // Establishes a happens-before edge.
        self.done.wait();
    }
}
