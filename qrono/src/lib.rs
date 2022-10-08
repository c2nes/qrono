extern crate core;

use qrono_channel as channel;

pub mod bytes;
pub mod data;
mod encoding;
pub mod error;
pub mod grpc;
pub mod hash;
pub mod http;
pub mod id_generator;
mod io;
pub mod ops;
mod path;
pub mod promise;
mod queue;
pub mod redis;
pub mod result;
pub mod scheduler;
pub mod segment;
pub mod service;
pub mod timer;
pub mod wait_group;
pub mod wal;
pub mod working_set;

#[cfg(test)]
pub(crate) mod test_alloc {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;

    thread_local! {
        static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
    }

    pub fn allocated() -> usize {
        ALLOCATED.with(|n| n.load(Relaxed))
    }

    #[cfg(not(miri))]
    mod alloc {
        use std::alloc::{GlobalAlloc, Layout};
        use std::sync::atomic::Ordering::Relaxed;

        #[global_allocator]
        static GLOBAL: TrackingAlloc = TrackingAlloc::new();

        struct TrackingAlloc {
            a: jemallocator::Jemalloc,
        }

        impl TrackingAlloc {
            const fn new() -> Self {
                Self {
                    a: jemallocator::Jemalloc,
                }
            }

            fn add(&self, size: usize) {
                super::ALLOCATED.with(|n| n.fetch_add(size, Relaxed));
            }

            fn sub(&self, size: usize) {
                super::ALLOCATED.with(|n| n.fetch_sub(size, Relaxed));
            }
        }

        unsafe impl GlobalAlloc for TrackingAlloc {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                let size = layout.size();
                let res = self.a.alloc(layout);
                if !res.is_null() {
                    self.add(size);
                }
                res
            }

            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                self.sub(layout.size());
                self.a.dealloc(ptr, layout);
            }

            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                let size = layout.size();
                let res = self.a.alloc_zeroed(layout);
                if !res.is_null() {
                    self.add(size);
                }
                res
            }

            unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                let old_size = layout.size();
                let res = self.a.realloc(ptr, layout, new_size);
                if !res.is_null() {
                    self.add(new_size);
                    self.sub(old_size);
                }
                res
            }
        }
    }
}
