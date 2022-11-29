use std::{
    alloc::{GlobalAlloc, Layout},
    cell::Cell,
    ffi::c_int,
};

use jemallocator::usable_size;

thread_local! {
    static TRACKER: Cell<Option<isize>> = Cell::new(None);
}

unsafe fn record(alloc: *const u8, dealloc: *const u8) {
    unsafe fn size(ptr: *const u8) -> isize {
        if ptr.is_null() {
            0
        } else {
            usable_size(ptr) as isize
        }
    }

    TRACKER.with(|val| {
        if let Some(current) = val.get() {
            val.set(Some(current + size(alloc) - size(dealloc)));
        }
    });
}

pub struct QronoAllocator(jemallocator::Jemalloc);

impl QronoAllocator {
    pub const fn new() -> QronoAllocator {
        QronoAllocator(jemallocator::Jemalloc)
    }
}

unsafe impl GlobalAlloc for QronoAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let res = self.0.alloc(layout);
        record(res, std::ptr::null());
        res
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(std::ptr::null(), ptr);
        self.0.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let res = self.0.alloc_zeroed(layout);
        record(res, std::ptr::null());
        res
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let res = self.0.realloc(ptr, layout, new_size);
        // If the realloc was unsuccessful then null will be
        // returned and the original allocation remains valid.
        if !res.is_null() {
            record(res, ptr);
        }
        res
    }
}

pub fn size_of(layout: Layout) -> usize {
    if layout.size() == 0 {
        return 0;
    }

    let flags = layout_to_flags(layout.align(), layout.size());
    unsafe {
        // SAFETY: layout.size() == 0 is handled above
        jemalloc_sys::nallocx(layout.size(), flags)
    }
}

pub fn track() -> Tracker {
    let previous = TRACKER.with(|val| {
        let previous = val.get();
        val.set(Some(previous.unwrap_or(0)));
        previous
    });

    Tracker { previous }
}

pub struct Tracker {
    previous: Option<isize>,
}

impl Tracker {
    pub fn allocation_change(&self) -> isize {
        TRACKER
            .with(|v| v.get())
            .expect("memory usage should be available")
            - self.previous.unwrap_or(0)
    }
}

impl Drop for Tracker {
    fn drop(&mut self) {
        if self.previous.is_none() {
            TRACKER.with(|val| val.set(None))
        }
    }
}

// --------------------------------------------------------------------------------
// Copied from jemallocator
// Copyright (c) 2014 Alex Crichton
// MIT license

// This constant equals _Alignof(max_align_t) and is platform-specific. It
// contains the _maximum_ alignment that the memory allocations returned by the
// C standard library memory allocation APIs (e.g. `malloc`) are guaranteed to
// have.
//
// The memory allocation APIs are required to return memory that can fit any
// object whose fundamental aligment is <= _Alignof(max_align_t).
//
// In C, there are no ZSTs, and the size of all types is a multiple of their
// alignment (size >= align). So for allocations with size <=
// _Alignof(max_align_t), the malloc-APIs return memory whose alignment is
// either the requested size if its a power-of-two, or the next smaller
// power-of-two.
#[cfg(all(any(
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mipsel",
    target_arch = "powerpc"
)))]
const ALIGNOF_MAX_ALIGN_T: usize = 8;
#[cfg(all(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "powerpc64",
    target_arch = "powerpc64le",
    target_arch = "mips64",
    target_arch = "riscv64",
    target_arch = "s390x",
    target_arch = "sparc64"
)))]
const ALIGNOF_MAX_ALIGN_T: usize = 16;

/// If `align` is less than `_Alignof(max_align_t)`, and if the requested
/// allocation `size` is larger than the alignment, we are guaranteed to get a
/// suitably aligned allocation by default, without passing extra flags, and
/// this function returns `0`.
///
/// Otherwise, it returns the alignment flag to pass to the jemalloc APIs.
fn layout_to_flags(align: usize, size: usize) -> c_int {
    if align <= ALIGNOF_MAX_ALIGN_T && align <= size {
        0
    } else {
        jemalloc_sys::MALLOCX_ALIGN(align)
    }
}
