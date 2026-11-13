use std::fmt;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct ThreadPoolSharedData{
    name: Option<String>,
    job_receiver: Mutex<Receiver<Job>>,
    empty_trigger: Mutex<()>,
    empty_condvar: Condvar,
    join_generation: AtomicUsize,
    queued_count: AtomicUsize,
    active_count: AtomicUsize,
    max_thread_count: AtomicUsize,
    panic_count: AtomicUsize,
    stack_size: Option<usize>,
}

impl ThreadPoolSharedData{
    fn has_work(&self) -> bool {
        self.queued_count.load(Ordering::SeqCst) > 0 || self.active_count.load(Ordering::SeqCst) > 0
    }


    //It will be called when there is no work to do or thread is panicking
    fn no_work_notify_all(&self) {
        if !self.has_work() {
            let _guard = self
                .empty_trigger
                .lock()//if the join method doesn't acquire lock then only we can take it
                .expect("Unable to notify all joining threads");
            self.empty_condvar.notify_all();
        }
    }
}

pub struct ThreadPool{
    shared_data: Arc<ThreadPoolSharedData>,
    channel: Option<mpsc::Sender<Job>>,
}

impl ThreadPool{
    pub fn new(num_threads: usize) -> ThreadPool{
        Builder::new().num_threads(num_threads).build()
    }

    pub fn with_name(name: String, num_threads: usize) -> Self {
        Builder::new()
            .num_threads(num_threads)
            .thread_name(name)
            .build()
    }

    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.shared_data.queued_count.fetch_add(1, Ordering::Relaxed);
        let job = Box::new(job);
        self.channel.as_ref().unwrap().send(job).unwrap();
    }

    pub fn queue_count(&self) -> usize {
        self.shared_data.queued_count.load(Ordering::Relaxed)
    }

    pub fn active_count(&self) -> usize {
        self.shared_data.active_count.load(Ordering::SeqCst)
    }

    pub fn running_count(&self) -> usize {
        self.shared_data.active_count.load(Ordering::Relaxed) - self.shared_data.queued_count.load(Ordering::Relaxed)
    }

    pub fn max_count(&self) -> usize {
        self.shared_data.max_thread_count.load(Ordering::Relaxed)
    }

    pub fn set_num_threads(&self, num_threads: usize) {

        let prev_count = self.shared_data.max_thread_count.swap(num_threads, Ordering::Release);
        if let Some(count) = num_threads.checked_sub(prev_count) {
            for _ in 0..count {
                spawn_in_pool(self.shared_data.clone());
            }
        }
    }

    ///What Happens Without the Lock Before the Loop (Lost Wakeup Bug)
    // Suppose join() did not lock empty_trigger before checking has_work():
    //
    // Thread A (Joiner): Checks has_work() → sees true (1 job left).
    //
    // Context Switch! Thread B (Worker): Finishes that last job (active_count becomes 0), acquires empty_trigger, and calls empty_condvar.notify_all(). Nobody is sleeping yet, so the notification vanishes into thin air.
    //
    // Thread A (Joiner): Resumes, locks empty_trigger, and calls empty_condvar.wait(lock).
    //
    // Because Thread B already sent notify_all() before Thread A entered wait(), Thread A sleeps forever—a classic deadlock known as a lost wakeup.
    //
    // How Locking Before the Loop Prevents It
    // Because both join() and no_work_notify_all() acquire empty_trigger, only two execution orders are possible:
    //
    // Case 1: Worker wins the lock first
    //
    // Worker sets active_count = 0, locks empty_trigger, calls notify_all(), and unlocks.
    //
    // Joiner acquires empty_trigger, enters the while loop, checks has_work() → sees false (thanks to SeqCst ordering), and skips wait() completely.
    //
    // Case 2: Joiner wins the lock first
    //
    // Joiner locks empty_trigger and checks has_work() → sees true.
    //
    // Meanwhile, Worker sets active_count = 0, then tries to lock empty_trigger inside no_work_notify_all()—it blocks, because Joiner still holds the lock.
    //
    // Joiner calls empty_condvar.wait(lock), which atomically puts Joiner into the condvar's wait queue and releases empty_trigger.
    //
    // Now Worker acquires empty_trigger and calls notify_all(), which is guaranteed to find Joiner already registered in the wait queue and wake it up.
    ///


    ///
    /// The Setup
    // You have a ThreadPool shared across three threads:
    //
    // Thread A1 and Thread A2: Two threads waiting for the current batch of jobs to finish (pool.join()).
    //
    // Thread B: A producer thread that periodically submits new jobs (pool.execute(...)).
    //
    // Worker: A background worker thread executing jobs.
    //
    // Currently, Batch 0 has 1 job running (active_count = 1), and join_generation = 0.
    //
    // Scenario 1: Without join_generation (The Starvation Bug)
    // Suppose join() only checked while self.shared_data.has_work().
    //
    // T=1: Both Thread A1 and Thread A2 call pool.join(). Both see has_work() == true and go to sleep on empty_condvar.wait(lock).
    //
    // T=2: The Worker finishes that 1 job. active_count drops to 0. has_work() is now false!
    //
    // T=3: The Worker calls empty_condvar.notify_all(). Both Thread A1 and Thread A2 are moved from the "sleeping" queue to the OS "ready to run" queue.
    //
    // T=4 (The Race): Before the OS schedules Thread A2 to run, Thread B calls pool.execute(new_job)! queued_count becomes 1. has_work() is now true again.
    //
    // T=5: Thread A2 finally wakes up from wait(), reacquires empty_trigger, and re-checks its while condition:
    //
    // Rust
    // while self.shared_data.has_work() // Evaluates to TRUE because of Thread B's new job!
    // Result: Thread A2 misses the fact that the pool did hit zero at T=2! It thinks Batch 0 never finished and goes back to sleep, forced to wait for Thread B's unrelated work. If Thread B keeps adding jobs, Thread A2 could hang forever.
    ///
    pub fn join(&self) {
        if !self.shared_data.has_work() {
            return;
        }

        let generation = self.shared_data.join_generation.load(Ordering::Acquire);
        //before going to sleep acquire the lock so that no notify is lost when has_work become false again
        //Critical Sectionx`
        let mut lock = self.shared_data.empty_trigger.lock().unwrap();

        while generation == self.shared_data.join_generation.load(Ordering::Relaxed) && self.shared_data.has_work() {
            lock = self.shared_data.empty_condvar.wait(lock).unwrap();
        }

        self.shared_data.join_generation.compare_exchange(generation, generation + 1, Ordering::Release, Ordering::Relaxed).unwrap();
    }
}

fn spawn_in_pool(shared_data: Arc<ThreadPoolSharedData>) {
    let mut builder = thread::Builder::new();
    if let Some(ref name) = shared_data.name {
        builder = builder.name(name.clone());
    }
    if let Some(stack_size) = shared_data.stack_size {
        builder = builder.stack_size(stack_size);
    }

    builder
        .spawn(|| {
            let mut worker = Worker::new(shared_data);

            loop {
                let thread_counter_val = worker.shared_data.active_count.load(Ordering::Acquire);
                let max_thread_count_val = worker.shared_data.max_thread_count.load(Ordering::Relaxed);
                if thread_counter_val >= max_thread_count_val {
                    break;
                }

                let message = {
                    let lock = worker
                        .shared_data
                        .job_receiver
                        .lock()//it can panic becuase of posion mutex
                        .expect("Worker thread unable to lock job_receiver");
                    lock.recv()
                };

                let job = match message {
                    Ok(job) => job,
                    // The ThreadPool was dropped (all Senders closed).
                    Err(..) => break,
                };

                worker.start_job();
                job(); //job can panic, in this case when in drop function both active and is_running_job will be true
                worker.finish_job();
            }
            worker.cancel();
        })
        .unwrap();
}

impl Clone for ThreadPool {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            shared_data: self.shared_data.clone(),
        }
    }
}

impl Default for ThreadPool {
    fn default() -> Self {
        let num_cpus = thread::available_parallelism().unwrap().get();
        Self::new(num_cpus)
    }
}

impl Debug for ThreadPool{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadPool")
            .field("max_threads", &self.max_count())
            .field("active_threads", &self.running_count())
            .field("queued_jobs", &self.queue_count())
            .finish()
    }
}

impl PartialEq for ThreadPool {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared_data, &other.shared_data)
    }
}

impl Eq for ThreadPool {}

struct Worker{
    shared_data: Arc<ThreadPoolSharedData>,
    active: bool,
    is_running_job: bool,
}

impl Worker{
    fn new(shared_data: Arc<ThreadPoolSharedData>) -> Worker {
        Worker {
            shared_data,
            active: true,
            is_running_job: false,
        }
    }

    fn start_job(&mut self) {
        self.shared_data.active_count.fetch_add(1, Ordering::SeqCst);
        self.shared_data.queued_count.fetch_sub(1, Ordering::SeqCst);
        self.is_running_job = true;
    }

    fn finish_job(&mut self) {
        self.shared_data.active_count.fetch_sub(1, Ordering::SeqCst);
        self.shared_data.no_work_notify_all();
        self.is_running_job = false;
    }

    fn cancel(&mut self) {
        self.active = false;
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.active {
            self.is_running_job.then(|| self.shared_data.active_count.fetch_sub(1, Ordering::Relaxed));
            thread::panicking().then(|| self.shared_data.panic_count.fetch_add(1, Ordering::Relaxed));
            self.shared_data.no_work_notify_all();
            spawn_in_pool(self.shared_data.clone());
        }
    }
}

#[derive(Clone, Default)]
pub struct Builder {
    num_threads: Option<usize>,
    thread_name: Option<String>,
    thread_stack_size: Option<usize>,
}

impl Builder{
    pub fn new() -> Builder {
        Builder {
            num_threads: None,
            thread_name: None,
            thread_stack_size: None,
        }
    }
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }
    pub fn thread_name(mut self, thread_name: String) -> Self {
        self.thread_name = Some(thread_name);
        self
    }
    pub fn thread_stack_size(mut self, thread_stack_size: usize) -> Self {
        self.thread_stack_size = Some(thread_stack_size);
        self
    }
    pub fn build(self) -> ThreadPool {
        let (tx, rx) = mpsc::channel();

        let num_thread = self.num_threads.unwrap_or_else(|| {
            thread::available_parallelism().unwrap().get()
        });

        let shared_data = Arc::new(ThreadPoolSharedData{
            name: self.thread_name,
            job_receiver: Mutex::new(rx),
            empty_trigger: Mutex::new(()),
            empty_condvar: Condvar::new(),
            join_generation: AtomicUsize::new(0),
            queued_count: AtomicUsize::new(0),
            active_count: AtomicUsize::new(0),
            max_thread_count: AtomicUsize::new(0),
            panic_count: AtomicUsize::new(0),
            stack_size: self.thread_stack_size,
        });

        for _ in 0..num_thread {
            spawn_in_pool(shared_data.clone());
        }

        ThreadPool{
            shared_data,
            channel: Some(tx),
        }
    }
}
