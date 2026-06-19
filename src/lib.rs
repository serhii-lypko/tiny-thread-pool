//! Main idea:
//!
//! "How does this arbitrary concurrent primitive scale?"
//! -> Counter, HashTable, Queue, RingBuffer, etc.

#![allow(dead_code)]

pub mod computable;

use crate::computable::Computable;
use std::{
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

// TODO: consider synchronization alternatives:
// - crossbeam WaitGroup
// - pure atomics with epoch/reset bookkeeping and parking (note: Idle CPU burn)
pub struct ThreadPool<C: Computable> {
    state: Arc<C>,

    workers: Vec<JoinHandle<()>>,

    ready: Arc<Barrier>,
    done: Arc<Barrier>,
    shutdown: Arc<AtomicBool>,
}

impl<C> ThreadPool<C>
where
    C: Computable + Send + Sync + 'static,
{
    pub fn new(state: C, threads_count: usize) -> Self {
        let mut workers = vec![];

        let ready = Arc::new(Barrier::new(threads_count + 1));
        let done = Arc::new(Barrier::new(threads_count + 1));

        let shutdown = Arc::new(AtomicBool::new(false));

        let state = Arc::new(state);

        for id in 0..threads_count {
            let task = thread::spawn({
                let state = state.clone();

                let ready = ready.clone();
                let done = done.clone();
                let shutdown = shutdown.clone();

                move || {
                    loop {
                        // Will be unblocked when all threads is ready (signal commited from the run_batch).
                        ready.wait();

                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }

                        'batch: loop {
                            let completed = state.compute_step(id);

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
            ready,
            done,
            shutdown,
        }
    }

    // reset, start, wait for done
    pub fn run_batch(&self) {
        self.shutdown.store(false, Ordering::Relaxed);

        self.state.reset();

        // Gives start signal for a workers.
        self.ready.wait();

        // Establishes a happens-before edge.
        self.done.wait();
    }

    pub fn shutdown(self) {
        self.shutdown.store(true, Ordering::Relaxed);

        // wake all workers - they'll hit the check and break
        self.ready.wait();

        for w in self.workers {
            w.join().unwrap();
        }

        self.state.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::{Computable, ThreadPool};
    use std::sync::Mutex;

    #[test]
    fn test_naive_counter() {
        struct NaiveCounter {
            counter: Mutex<u64>,
            treshold: u64,
        }

        impl Computable for NaiveCounter {
            type Inner = u64;

            fn compute_step(&self, _worker_id: usize) -> bool {
                let mut ctr = self.counter.lock().unwrap();
                *ctr += 1;

                if *ctr >= self.treshold {
                    return true;
                } else {
                    return false;
                }
            }

            fn reset(&self) {
                let mut ctr = self.counter.lock().unwrap();
                *ctr = 0;
            }

            fn curr(&self) -> Self::Inner {
                let ctr = self.counter.lock().unwrap();
                *ctr
            }
        }

        let naive_counter = NaiveCounter {
            counter: Mutex::new(0),
            treshold: 1e6 as u64,
        };

        const WORKERS: usize = 9;
        let thread_pool = ThreadPool::new(naive_counter, WORKERS);

        thread_pool.run_batch();

        // Testing the range
        assert!(thread_pool.state.treshold <= thread_pool.state.curr());
        assert!(thread_pool.state.curr() < thread_pool.state.treshold + WORKERS as u64);

        thread_pool.shutdown();
    }
}
