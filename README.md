# tiny-thread-pool

A minimal barrier-synchronized worker pool for benchmarking how a concurrent
primitive scales across threads. You implement [`Computable`] for your data
structure (counter, hash table, queue, ring buffer, ...), and each call to
`run_batch()` resets it, fires all workers at once, and waits for them to finish.

## Usage example

```rust
use criterion::Criterion;
use tiny_thread_pool::ThreadPool;

fn bench_lock_free_counter_4(c: &mut Criterion) {
    let threads = 4;
    let ds = LockFreeCounter::new();
    let pool = ThreadPool::new(ds, threads);

    c.bench_function(
        &format!("lock-free counter with threads: {}", threads),
        |b| {
            b.iter(|| pool.run_batch());
        },
    );
}
```

## Contract

`run_batch()` blocks until every worker calls `compute_step()` and gets `true`
**exactly once** per batch. If a worker never returns `true`, or the workers
disagree on how many times they finish, the pool deadlocks on its internal
barrier.
