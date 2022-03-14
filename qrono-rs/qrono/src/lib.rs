extern crate core;

pub mod data;
mod encoding;
pub mod hash;
mod ops;
pub mod segment;
use qrono_channel as channel;
pub mod bytes;
mod error;
pub mod http;
pub mod id_generator;
mod io;
mod path;
mod promise;
mod queue;
pub mod redis;
mod result;
pub mod scheduler;
pub mod service;
pub mod timer;
pub mod wal;
pub mod working_set;

#[cfg(test)]
pub(crate) mod test_alloc {
    use std::alloc::{GlobalAlloc, Layout};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;

    struct TrackingAlloc {
        a: jemallocator::Jemalloc,
        n: AtomicUsize,
    }

    impl TrackingAlloc {
        const fn new() -> Self {
            Self {
                a: jemallocator::Jemalloc,
                n: AtomicUsize::new(0),
            }
        }
    }

    unsafe impl GlobalAlloc for TrackingAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let size = layout.size();
            let res = self.a.alloc(layout);
            if !res.is_null() {
                self.n.fetch_add(size, Relaxed);
            }
            res
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            self.n.fetch_sub(layout.size(), Relaxed);
            self.a.dealloc(ptr, layout);
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let size = layout.size();
            let res = self.a.alloc_zeroed(layout);
            if !res.is_null() {
                self.n.fetch_add(size, Relaxed);
            }
            res
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let old_size = layout.size();
            let res = self.a.realloc(ptr, layout, new_size);
            if !res.is_null() {
                self.n.fetch_add(new_size, Relaxed);
                self.n.fetch_sub(old_size, Relaxed);
            }
            res
        }
    }

    #[global_allocator]
    static GLOBAL: TrackingAlloc = TrackingAlloc::new();

    pub fn allocated() -> usize {
        GLOBAL.n.load(Relaxed)
    }
}
