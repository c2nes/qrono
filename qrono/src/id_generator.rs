use crate::data::ID;
use crate::path;
use crate::result::IgnoreErr;
use crate::scheduler::{BoxFn, Scheduler, TaskHandle};
use crossbeam::utils::Backoff;
use std::fs::File;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::{fs, io};

#[derive(Clone)]
pub struct IdGenerator {
    inner: Arc<Inner>,
    raise_ceiling: Arc<TaskHandle<BoxFn>>,
}

impl IdGenerator {
    pub fn new<P: AsRef<Path>>(path: P, scheduler: Scheduler) -> io::Result<IdGenerator> {
        let inner = Arc::new(Inner::new(path)?);
        let raise_ceiling = {
            let inner = Arc::clone(&inner);
            let (handle, _) = scheduler.register(BoxFn::new(move || inner.raise_ceiling()));
            Arc::new(handle)
        };
        if inner.raise_ceiling() {
            raise_ceiling.schedule().ignore_err();
        }
        Ok(IdGenerator {
            inner,
            raise_ceiling,
        })
    }

    pub fn generate_id(&self) -> ID {
        self.inner.generate_ids(1, &self.raise_ceiling).start
    }

    pub fn generate_ids(&self, count: usize) -> Range<ID> {
        self.inner.generate_ids(count, &self.raise_ceiling)
    }
}

struct Inner {
    next: AtomicU64,
    ceiling: AtomicU64,

    reservation_size: u64,
    raise_ceiling_threshold: u64,

    path: PathBuf,
}

const DEFAULT_RESERVATION_SIZE: u64 = 1_000_000;
const DEFAULT_RAISE_CEILING_THRESHOLD: u64 = 500_000;

impl Inner {
    fn new<P: AsRef<Path>>(path: P) -> io::Result<Inner> {
        let n = match fs::read_to_string(&path) {
            Ok(s) => match s.parse() {
                Ok(n) => n,
                Err(err) => return Err(io::Error::new(io::ErrorKind::InvalidData, err)),
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => 1,
            Err(err) => return Err(err),
        };

        let next = AtomicU64::new(n);
        let ceiling = AtomicU64::new(n);
        let reservation_size = DEFAULT_RESERVATION_SIZE;
        let raise_ceiling_threshold = DEFAULT_RAISE_CEILING_THRESHOLD;
        let path = path.as_ref().into();

        Ok(Inner {
            next,
            ceiling,
            reservation_size,
            raise_ceiling_threshold,
            path,
        })
    }

    fn generate_ids(&self, n: usize, raise_ceiling: &TaskHandle<BoxFn>) -> Range<ID> {
        if n == 0 {
            return 0..0;
        }

        let n = n as ID;
        let id = self.next.fetch_add(n, Ordering::Relaxed);
        let ceiling = self.ceiling.load(Ordering::Acquire);
        if ceiling < id + n + self.raise_ceiling_threshold {
            raise_ceiling.schedule().ignore_err();
        }
        if ceiling < id + n {
            self.wait_for_clearance(id + n)
        }
        id..id + n
    }

    fn wait_for_clearance(&self, limit: ID) {
        let backoff = Backoff::new();
        while self.ceiling.load(Ordering::Acquire) < limit {
            backoff.snooze();
        }
    }

    fn raise_ceiling(&self) -> bool {
        let new_ceiling = self.next.load(Acquire) + self.reservation_size;
        let temp_path = path::with_temp_suffix(&self.path);

        /// Like io::write, but syncs the file before returning.
        fn write(path: &Path, contents: &[u8]) -> io::Result<()> {
            let mut file = File::create(path)?;
            file.write_all(contents)?;
            file.sync_all()
        }

        if let Err(err) = write(&temp_path, new_ceiling.to_string().as_bytes()) {
            eprintln!("Error raising IdGenerator ceiling (write): {}", err);
            // Try again
            return true;
        }

        if let Err(err) = fs::rename(&temp_path, &self.path) {
            eprintln!("Error raising IdGenerator ceiling (rename): {}", err);
            // Try again
            return true;
        }

        self.ceiling.store(new_ceiling, Ordering::Release);

        false
    }
}

#[cfg(test)]
mod tests {
    use crate::id_generator::IdGenerator;
    use crate::scheduler::{Scheduler, Unpooled};
    use tempfile::tempdir;

    #[test]
    fn monotonic() {
        let dir = tempdir().unwrap();
        let mut last = 0;
        for _ in 0..10 {
            let gen = IdGenerator::new(dir.path().join("id"), Scheduler::new(Unpooled)).unwrap();
            for _ in 0..1000 {
                let id = gen.generate_id();
                assert!(id > last);
                last = id;
            }
        }
    }

    #[test]
    fn monotonic_batches() {
        let dir = tempdir().unwrap();
        let mut last = 1;
        for i in 0..10 {
            let gen = IdGenerator::new(dir.path().join("id"), Scheduler::new(Unpooled)).unwrap();
            for _ in 0..1000 {
                let range = gen.generate_ids(i * 10 + 1);
                assert!(range.start >= last);
                last = range.end;
            }
        }
    }

    #[test]
    fn empty_batch() {
        let dir = tempdir().unwrap();
        let gen = IdGenerator::new(dir.path().join("id"), Scheduler::new(Unpooled)).unwrap();
        assert!(gen.generate_ids(0).is_empty());
    }

    #[test]
    fn wait_for_clearance() {
        let dir = tempdir().unwrap();
        let gen = IdGenerator::new(dir.path().join("id"), Scheduler::new(Unpooled)).unwrap();
        let mut last = 1;
        for _ in 0..100 {
            // By requesting more IDs than the reservation size, we're guaranteed that the ceiling
            // will need to be raised to complete our request.
            let count = (10 * super::DEFAULT_RESERVATION_SIZE) as usize;
            let range = gen.generate_ids(count);
            assert_eq!(count, (range.end - range.start) as usize);
            assert!(range.start >= last);
            last = range.end;
        }
    }
}
