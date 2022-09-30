use parking_lot::{Condvar, Mutex};

#[derive(Debug, Default)]
pub struct WaitGroup {
    count: Mutex<usize>,
    done: Condvar,
}

impl WaitGroup {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn add(&self, count: usize) {
        *self.count.lock() += count;
    }

    pub fn done(&self) -> usize {
        let mut locked = self.count.lock();
        *locked -= 1;
        if *locked == 0 {
            self.done.notify_all();
        }
        *locked
    }

    pub fn wait(&self) {
        let mut locked = self.count.lock();
        while *locked != 0 {
            self.done.wait(&mut locked);
        }
    }
}
