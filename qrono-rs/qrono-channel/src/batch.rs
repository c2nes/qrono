use crossbeam_utils::Backoff;
use std::cell::UnsafeCell;

use std::mem::MaybeUninit;

use std::ptr::NonNull;
use std::{mem, ptr};

use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize};

const LAP: usize = 1 << 5;
const SLOTS: usize = LAP - 1;

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    ready: AtomicBool,
}

impl<T> Slot<T> {
    /// Writes a value to the slot.
    ///
    /// ## Safety
    ///
    /// If there is an existing value in the slot it is replaced without dropping. To drop
    /// an existing value, `take()` should be called first.
    unsafe fn write(&mut self, value: T) {
        self.value.get().write(MaybeUninit::new(value));
        self.ready.store(true, Release);
    }

    fn take(&mut self) -> T {
        let backoff = Backoff::new();
        while self
            .ready
            .compare_exchange_weak(true, false, Acquire, Relaxed)
            .is_err()
        {
            backoff.spin();
        }

        // SAFETY: `write` sets `ready` to true only after writing an initialized value.
        unsafe { ptr::read(self.value.get() as *const T) }
    }

    fn borrow(&self) -> &T {
        let backoff = Backoff::new();
        while !self.ready.load(Acquire) {
            backoff.spin();
        }

        // SAFETY:
        // - `write` sets `ready` to true only after writing an initialized value.
        // - The reference is valid until `take` or `write` are called, both of which
        //   require a mutable reference.
        unsafe { &*(self.value.get() as *const T) }
    }
}

struct Block<T> {
    slots: [Slot<T>; SLOTS],
    next: AtomicPtr<Block<T>>,
}

impl<T> Block<T> {
    fn new() -> Self {
        // SAFETY: All fields can be safely zeroed.
        // - slots can be safely zeroed because Slot<T> can be safely zeroed.
        //   - Slot.value can be safely zeroed because it is uninitialized.
        //   - Slot.ready can be safely zeroed because its underlying representation is `false`.
        // - next can be safely zeroed because it is a null pointer.
        unsafe { MaybeUninit::zeroed().assume_init() }
    }
}

struct Tail<T> {
    block: AtomicPtr<Block<T>>,
    index: AtomicUsize,
}

struct Shared<T> {
    tail: Tail<T>,
    dropped: AtomicBool,
}

impl<T> Shared<T> {
    /// Given the current `tail` and `index`, allocate and install a new tail block.
    unsafe fn push_new_tail(
        &self,
        block: Box<Block<T>>,
        tail: *mut Block<T>,
        index: usize,
    ) -> *mut Block<T> {
        let new_tail = Box::into_raw(block);
        (*tail).next.store(new_tail, Release);
        self.tail.block.store(new_tail, Release);
        self.tail.index.store(index + 1, Release);
        new_tail
    }

    fn push(&self, value: T) {
        let backoff = Backoff::new();
        let mut index = self.tail.index.load(Acquire);
        let mut tail = self.tail.block.load(Acquire);
        let mut new_block = None;

        loop {
            if tail.is_null() {
                panic!("Receiver dropped")
            }

            let pos = index & SLOTS;
            if pos == SLOTS {
                // Spin while new block is allocated.
                backoff.snooze();
                index = self.tail.index.load(Acquire);
                tail = self.tail.block.load(Acquire);
                continue;
            }

            let next_index = index + 1;
            let next_pos = next_index & SLOTS;

            // Optimistically allocate the new block before performing the compare_exchange.
            // This keeps the allocation out of the critical section during which other publishers
            // and consumers are spinning.
            if next_pos == SLOTS && new_block.is_none() {
                new_block = Some(Box::new(Block::new()));
            }

            match self
                .tail
                .index
                .compare_exchange_weak(index, next_index, SeqCst, Acquire)
            {
                Ok(_) => unsafe {
                    if next_pos == SLOTS {
                        let new_block = new_block.take().unwrap_unchecked();
                        self.push_new_tail(new_block, tail, next_index);
                    }

                    (*tail).slots.get_unchecked_mut(pos).write(value);
                    return;
                },
                Err(idx) => {
                    index = idx;
                    tail = self.tail.block.load(Acquire);
                    backoff.spin();
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Sender<T> {
    shared: NonNull<Shared<T>>,
}

impl<T> Sender<T> {
    pub fn send(&self, val: T) {
        unsafe { self.shared.as_ref().push(val) }
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        unsafe {
            let shared = self.shared.as_ref();
            let dropped = shared.dropped.swap(true, Relaxed);
            if dropped {
                drop(Box::from(self.shared.as_ptr()))
            }
        }
    }
}

pub struct Receiver<T> {
    shared: NonNull<Shared<T>>,
    head: NonNull<Block<T>>,
    free: Option<Box<Block<T>>>,
}

unsafe impl<T: Send> Send for Receiver<T> {}

impl<T> Receiver<T> {
    pub fn recv(&mut self, limit: usize) -> Batch<T> {
        let backoff = Backoff::new();
        let mut batch_head = ptr::null_mut::<Block<T>>();
        let mut batch_tail = batch_head;
        let mut batch_len = 0;

        // We pop the block at `head` and then set `head` to `head.next`. We do not want
        // `head` to ever be null however, so if `head == tail` we first push a new block
        // at tail. We do so by jumping the index to the end of the block, causing senders
        // to wait while we allocate and install the new block.
        unsafe {
            let shared = self.shared.as_ref();

            loop {
                let head = self.head.as_ptr();
                let mut next = (*head).next.load(Acquire);
                let mut len = SLOTS;

                if next.is_null() {
                    let index = shared.tail.index.load(Acquire);
                    let tail = shared.tail.block.load(Acquire);

                    // If `next` is null then `head` and `tail` should be the same. If they are not
                    // then we may have raced with a sender allocating a new tail.
                    if head != tail {
                        backoff.spin();
                        continue;
                    }

                    // The block is empty, so we can just return an empty batch.
                    let pos = index & SLOTS;
                    if pos == 0 {
                        break;
                    }

                    // A new block is being allocated as we speak. Spin until it completes.
                    if pos == SLOTS {
                        backoff.snooze();
                        continue;
                    }

                    // Ensure there is a free block available
                    if self.free.is_none() {
                        self.free = Some(Box::new(Block::new()));
                    }

                    let next_index = index | SLOTS;
                    if shared
                        .tail
                        .index
                        .compare_exchange_weak(index, next_index, Release, Relaxed)
                        .is_err()
                    {
                        backoff.spin();
                        continue;
                    }

                    // Install new tail so we can safely take head
                    let new_block = self.free.take().unwrap_unchecked();
                    next = shared.push_new_tail(new_block, tail, next_index);
                    len = pos
                }

                // Update head
                self.head = NonNull::new_unchecked(next);

                if batch_head.is_null() {
                    batch_head = head;
                    batch_tail = head;
                } else {
                    (*batch_tail).next = AtomicPtr::new(head);
                    batch_tail = head;
                }

                batch_len += len;

                // Break once we get a partial batch, or are at risk of exceeding the limit.
                if len < SLOTS || batch_len + SLOTS > limit {
                    break;
                }
            }
        }

        if batch_head.is_null() {
            Batch {
                block: None,
                len: 0,
                pos: 0,
                block_pos: 0,
                free: &mut self.free,
            }
        } else {
            let block = unsafe {
                // Terminate the linked list
                (*batch_tail).next = AtomicPtr::default();
                Box::from_raw(batch_head)
            };

            Batch {
                block: Some(block),
                len: batch_len,
                pos: 0,
                block_pos: 0,
                free: &mut self.free,
            }
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        unsafe {
            let shared = self.shared.as_ref();
            let dropped = shared.dropped.swap(true, Relaxed);
            let backoff = Backoff::new();
            let mut index = shared.tail.index.load(Acquire);

            // If the receiver has not dropped then we need to do some work to ensure
            // no new values can be sent since we will not be around to drop them later.
            if !dropped {
                while index & SLOTS == SLOTS {
                    backoff.spin();
                    index = shared.tail.index.load(Acquire);
                }

                while let Err(value) =
                    shared
                        .tail
                        .index
                        .compare_exchange_weak(index, index | SLOTS, Release, Relaxed)
                {
                    index = value;
                    backoff.spin();
                }

                shared.tail.block.store(ptr::null_mut(), Release);
            } else {
                drop(Box::from(self.shared.as_ptr()))
            }

            let mut block = self.head.as_ptr();
            while !block.is_null() {
                let next = (*block).next.load(Acquire);
                let len = if next.is_null() { index & SLOTS } else { SLOTS };

                for i in 0..len {
                    drop((*block).slots[i].take())
                }

                drop(Box::from_raw(block));
                block = next;
            }
        }
    }
}

pub struct Batch<'a, T> {
    block: Option<Box<Block<T>>>,
    len: usize,
    pos: usize,
    block_pos: usize,
    free: &'a mut Option<Box<Block<T>>>,
}

impl<'a, T> Batch<'a, T> {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.len - self.pos
    }

    pub fn iter(&self) -> Iter<T> {
        Iter {
            block: self.block.as_ref().map(|b| b.as_ref()),
            len: self.len,
            pos: self.pos,
            block_pos: self.block_pos,
        }
    }
}

impl<'a, T> Iterator for Batch<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_pos == SLOTS && self.pos < self.len {
            unsafe {
                let mut old = self.block.take().unwrap_unchecked();
                let ptr = mem::take(&mut old.next);
                *self.free = Some(old);

                self.block = Some(Box::from_raw(ptr.into_inner()));
                self.block_pos = 0;
            }
        }

        match &mut self.block {
            None => None,
            Some(block) => {
                if self.pos < self.len {
                    let slot = &mut block.slots[self.block_pos];
                    let val = slot.take();
                    self.pos += 1;
                    self.block_pos += 1;
                    Some(val)
                } else {
                    None
                }
            }
        }
    }
}

impl<'a, T> Drop for Batch<'a, T> {
    fn drop(&mut self) {
        // Consuming the rest of the iterator to ensure all values
        // and intermediate blocks are dropped.
        for _ in self.by_ref() {}

        // Recycling the final block, if any.
        if let Some(block) = self.block.take() {
            assert!(block.next.load(Relaxed).is_null());
            *self.free = Some(block);
        }
    }
}

pub struct Iter<'a, T> {
    block: Option<&'a Block<T>>,
    len: usize,
    pos: usize,
    block_pos: usize,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_pos == SLOTS && self.pos < self.len {
            unsafe {
                let next = self.block.unwrap().next.load(Relaxed);
                self.block = Some(&*next);
                self.block_pos = 0;
            }
        }

        match &mut self.block {
            None => None,
            Some(block) => {
                if self.pos < self.len {
                    let slot = &block.slots[self.block_pos];
                    let val = slot.borrow();
                    self.pos += 1;
                    self.block_pos += 1;
                    Some(val)
                } else {
                    None
                }
            }
        }
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let block = Box::into_raw(Box::new(Block::new()));
    let shared = Box::new(Shared {
        tail: Tail {
            block: AtomicPtr::new(block),
            index: AtomicUsize::new(0),
        },
        dropped: AtomicBool::new(false),
    });

    // SAFETY: `into_raw` returns a properly aligned and non-null pointer.
    let shared = unsafe { NonNull::new_unchecked(Box::into_raw(shared)) };
    let head = unsafe { NonNull::new_unchecked(block) };

    let sender = Sender { shared };
    let receiver = Receiver {
        shared,
        head,
        free: None,
    };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::{channel, SLOTS};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::Barrier;

    #[test]
    fn recv_empty() {
        let (_, mut rx) = channel::<usize>();
        let batch = rx.recv(100);
        assert_eq!(0, batch.len());
        assert!(batch.is_empty());
    }

    #[test]
    fn recv_partial() {
        let (tx, mut rx) = channel::<usize>();
        tx.send(1);
        tx.send(2);
        tx.send(3);
        assert_eq!(vec![1, 2, 3], rx.recv(100).collect::<Vec<_>>());
        assert_eq!(0, rx.recv(100).len());
    }

    #[test]
    fn recv_one_full() {
        let (tx, mut rx) = channel::<usize>();
        for i in 0..SLOTS {
            tx.send(i);
        }
        assert_eq!(SLOTS, rx.recv(100).len());
        assert_eq!(0, rx.recv(100).len());
    }

    #[test]
    fn recv_multiple() {
        let (tx, mut rx) = channel::<usize>();
        for i in 0..(5 * SLOTS) {
            tx.send(i);
        }

        let mut batch = rx.recv(2 * SLOTS);
        assert_eq!(2 * SLOTS, batch.len());
        for expected in 0..(2 * SLOTS) {
            assert_eq!(Some(expected), batch.next());
        }
        drop(batch);

        assert_eq!(3 * SLOTS, rx.recv(10 * SLOTS).len());
    }

    #[test]
    fn recv_multiple_iter() {
        let (tx, mut rx) = channel::<usize>();
        for i in 0..(5 * SLOTS) {
            tx.send(i);
        }

        let mut batch = rx.recv(2 * SLOTS);
        let mut batch_iter = batch.iter();
        for expected in 0..(2 * SLOTS) {
            assert_eq!(Some(&expected), batch_iter.next());
        }
        drop(batch_iter);
        for expected in 0..(2 * SLOTS) {
            assert_eq!(Some(expected), batch.next());
        }
        drop(batch);

        assert_eq!(3 * SLOTS, rx.recv(10 * SLOTS).len());
    }

    #[test]
    fn recv_cycle_mixed() {
        let (tx, mut rx) = channel::<usize>();

        tx.send(0);
        for _ in 0..10 {
            for i in 1..SLOTS {
                tx.send(i);
            }
            tx.send(0);

            let mut batch = rx.recv(SLOTS);
            for i in 0..SLOTS {
                assert_eq!(Some(i), batch.next());
            }
            assert_eq!(None, batch.next());
        }
        assert_eq!(1, rx.recv(100).len);

        for i in 0..SLOTS - 1 {
            tx.send(i);
        }
        for _ in 0..10 {
            tx.send(SLOTS - 1);
            for i in 0..SLOTS - 1 {
                tx.send(i);
            }

            let mut batch = rx.recv(SLOTS);
            for i in 0..SLOTS {
                assert_eq!(Some(i), batch.next());
            }
            assert_eq!(None, batch.next());
        }
        assert_eq!(SLOTS - 1, rx.recv(SLOTS).len);

        assert_eq!(0, rx.recv(SLOTS).len());
    }

    #[test]
    fn drop_receiver() {
        let (tx, rx) = channel();
        tx.send(1);
        tx.send(2);
        tx.send(3);
        drop(rx);

        assert!(catch_unwind(AssertUnwindSafe(|| {
            tx.send(4);
        }))
        .is_err())
    }

    #[test]
    fn spsc() {
        const N: i64 = 1_000_000;
        let (tx, mut rx) = channel::<i64>();
        let barrier = Barrier::new(2);
        crossbeam::scope(|scope| {
            scope.spawn(|_| {
                barrier.wait();
                for i in 0..N {
                    tx.send(i);
                }
            });

            barrier.wait();
            let mut i = 0;
            while i < N {
                for x in rx.recv(100) {
                    assert_eq!(i, x);
                    i += 1;
                }
            }
        })
        .unwrap();
    }

    #[test]
    fn serial() {
        const N: i64 = 1_000_000;
        let (tx, mut rx) = channel::<i64>();
        for i in 0..N {
            tx.send(i);
        }

        let mut i = 0;
        while i < N {
            for x in rx.recv(100) {
                assert_eq!(i, x);
                i += 1;
            }
        }
    }

    #[test]
    fn mpsc() {
        const N: i64 = 100_000;
        const M: i64 = 100;
        let (tx, mut rx) = channel::<i64>();
        let barrier = Barrier::new((M + 1) as usize);
        crossbeam::scope(|scope| {
            for i in 0..M {
                let barrier = &barrier;
                let tx = &tx;
                scope.spawn(move |_| {
                    barrier.wait();
                    for j in 0..N {
                        tx.send(i * N + j);
                    }
                });
            }

            barrier.wait();
            let mut remaining = N * M;
            let mut sum = 0;
            while remaining > 0 {
                for x in rx.recv(100) {
                    remaining -= 1;
                    sum += x;
                }
            }

            assert_eq!((N * M) * (N * M - 1) / 2, sum);
        })
        .unwrap();
    }
}
