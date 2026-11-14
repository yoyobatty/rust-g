//! Job system
use flume::{Receiver, RecvTimeoutError, Sender};
use std::{
    cell::RefCell,
    collections::hash_map::{Entry, HashMap},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

struct Job {
    rx: Receiver<Output>,
}

type Output = String;
type JobId = String;
type Task = Box<dyn FnOnce() + Send>;

const NO_RESULTS_YET: &str = "NO RESULTS YET";
const NO_SUCH_JOB: &str = "NO SUCH JOB";
const JOB_PANICKED: &str = "JOB PANICKED";

/// How long a pool thread waits for another job before it exits.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// The threads jobs run on. A job goes to a thread that's waiting for one if there is one, and
/// gets a new thread if not, so a slow job (an HTTP request that hangs until it times out) never
/// holds up the others. Threads are reused instead of being started for every job.
struct Pool {
    tx: Sender<Task>,
    rx: Receiver<Task>,
    /// Threads waiting for a task that no queued task has been promised to yet.
    idle: AtomicUsize,
}

static POOL: LazyLock<Pool> = LazyLock::new(|| {
    let (tx, rx) = flume::unbounded();
    Pool {
        tx,
        rx,
        idle: AtomicUsize::new(0),
    }
});

impl Pool {
    fn run(&'static self, task: Task) {
        if self.claim_idle() {
            // The thread this claimed is waiting on the channel, so the send can't fail.
            let _ = self.tx.send(task);
        } else {
            thread::spawn(move || {
                Self::run_task(task);
                self.work();
            });
        }
    }

    /// Takes one waiting thread off the idle count. False if none are waiting.
    fn claim_idle(&self) -> bool {
        self.idle
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
            .is_ok()
    }

    fn work(&self) {
        loop {
            self.idle.fetch_add(1, Ordering::AcqRel);
            loop {
                match self.rx.recv_timeout(IDLE_TIMEOUT) {
                    Ok(task) => {
                        Self::run_task(task);
                        break;
                    }
                    // Only exit if no task has been promised to a waiting thread: if one has,
                    // it's in the channel or about to be, and this thread picks it up.
                    Err(RecvTimeoutError::Timeout) if self.claim_idle() => return,
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
        }
    }

    /// A panicking job only loses its own result (its sender is dropped, so `check` reports
    /// JOB PANICKED). The thread carries on with the next job.
    fn run_task(task: Task) {
        let _ = catch_unwind(AssertUnwindSafe(task));
    }
}

#[derive(Default)]
struct Jobs {
    map: HashMap<JobId, Job>,
    next_job: usize,
}

impl Jobs {
    fn start<F: FnOnce() -> Output + Send + 'static>(&mut self, f: F) -> JobId {
        let (tx, rx) = flume::unbounded();
        POOL.run(Box::new(move || {
            let _ = tx.send(f());
        }));
        let id = self.next_job.to_string();
        self.next_job += 1;
        self.map.insert(id.clone(), Job { rx });
        id
    }

    fn check(&mut self, id: &str) -> Output {
        let entry = match self.map.entry(id.to_owned()) {
            Entry::Occupied(occupied) => occupied,
            Entry::Vacant(_) => return NO_SUCH_JOB.to_owned(),
        };
        let result = match entry.get().rx.try_recv() {
            Ok(result) => result,
            Err(flume::TryRecvError::Disconnected) => JOB_PANICKED.to_owned(),
            Err(flume::TryRecvError::Empty) => return NO_RESULTS_YET.to_owned(),
        };
        entry.remove();
        result
    }
}

thread_local! {
    static JOBS: RefCell<Jobs> = RefCell::default();
}

pub fn start<F: FnOnce() -> Output + Send + 'static>(f: F) -> JobId {
    JOBS.with(|jobs| jobs.borrow_mut().start(f))
}

/// Runs `f` on the job threads without keeping a result for it.
#[allow(dead_code)] // Used depending on feature set
pub fn run_detached<F: FnOnce() + Send + 'static>(f: F) {
    POOL.run(Box::new(f));
}

pub fn check(id: &str) -> String {
    JOBS.with(|jobs| jobs.borrow_mut().check(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn wait(id: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let out = check(id);
            if out != NO_RESULTS_YET || Instant::now() > deadline {
                return out;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn jobs_return_results_and_panics_dont_kill_the_pool() {
        let ids: Vec<_> = (0..50).map(|n| start(move || n.to_string())).collect();
        for (n, id) in ids.iter().enumerate() {
            assert_eq!(wait(id), n.to_string());
        }
        assert_eq!(check(&ids[0]), NO_SUCH_JOB);

        let bad = start(|| panic!("job panic"));
        assert_eq!(wait(&bad), JOB_PANICKED);
        let after = start(|| "still working".to_owned());
        assert_eq!(wait(&after), "still working");
    }

    #[test]
    fn a_slow_job_does_not_hold_up_others() {
        let slow = start(|| {
            thread::sleep(Duration::from_secs(2));
            "slow".to_owned()
        });
        let started = Instant::now();
        let fast = start(|| "fast".to_owned());
        assert_eq!(wait(&fast), "fast");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(wait(&slow), "slow");
    }
}
