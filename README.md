# threadpool

A simple, lightweight Rust thread pool implementation (`threadpool_self`) for executing tasks concurrently across a pool of worker threads.

## Features

- **Fixed-size Thread Pool**: Easily create a thread pool with a specified number of worker threads.
- **Customizable Builder**: Configure custom thread names, stack sizes, and pool capacity using `Builder`.
- **Task Execution**: Dispatch closures (`FnOnce() + Send + 'static`) to be executed asynchronously by worker threads.
- **Join / Synchronization**: Block until all queued and running tasks complete using `pool.join()`.

## Installation & Requirements

- **Language**: Rust (2024 edition)
- Add `threadpool_self` to your `Cargo.toml`:

```toml
[dependencies]
threadpool_self = "0.1.0"
```

## Usage

### Basic Example

```rust
use threadpool_self::ThreadPool;
use std::sync::mpsc::channel;

fn main() {
    let n_workers = 4;
    let n_jobs = 8;
    let pool = ThreadPool::new(n_workers);

    let (tx, rx) = channel();
    for _ in 0..n_jobs {
        let tx = tx.clone();
        pool.execute(move || {
            tx.send(1).expect("channel send failed");
        });
    }

    pool.join();
    assert_eq!(rx.iter().take(n_jobs).sum::<i32>(), 8);
}
```

### Using the Builder

```rust
use threadpool_self::Builder;

fn main() {
    let pool = Builder::new()
        .thread_name("worker-thread".into())
        .num_threads(8)
        .thread_stack_size(8_000_000)
        .build();

    pool.execute(|| {
        println!("Hello from builder-configured thread pool!");
    });

    pool.join();
}
```

## API Reference

### `ThreadPool`
- `ThreadPool::new(num_threads: usize) -> ThreadPool`: Creates a new thread pool with `num_threads` workers.
- `pool.execute<F>(&self, f: F)`: Queues a job `f` (`FnOnce() + Send + 'static`) for execution.
- `pool.join(&self)`: Waits for all running and pending jobs in the pool to finish.

### `Builder`
- `Builder::new() -> Builder`: Initializes a new builder instance (default thread count uses available CPUs).
- `.num_threads(num_threads: usize) -> Builder`: Sets worker thread count.
- `.thread_name(name: String) -> Builder`: Sets the name prefix for spawned worker threads.
- `.thread_stack_size(size: usize) -> Builder`: Sets custom stack size in bytes for worker threads.
- `.build() -> ThreadPool`: Builds and starts the configured `ThreadPool`.

## Building & Testing

```bash
# Build the library and binary
cargo build

# Run tests
cargo test

# Run example binary
cargo run
```
