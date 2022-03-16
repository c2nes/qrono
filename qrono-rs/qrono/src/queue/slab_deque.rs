use std::mem;

/// An optional pointer to a entry. Roughly equivalent to `Option<usize>`
/// without the discriminant overhead.
#[derive(Copy, Clone, Debug, Default)]
struct Pointer(usize);

impl Pointer {
    #[inline(always)]
    const fn none() -> Pointer {
        Pointer(0)
    }

    fn get(self) -> Option<usize> {
        match self.0 {
            0 => None,
            val => Some(val - 1),
        }
    }

    fn is_none(self) -> bool {
        self.0 == 0
    }
}

impl From<Pointer> for Option<usize> {
    fn from(val: Pointer) -> Self {
        val.get()
    }
}

impl From<Option<usize>> for Pointer {
    fn from(val: Option<usize>) -> Self {
        match val {
            None => Pointer(0),
            Some(val) => Pointer(val + 1),
        }
    }
}

impl From<usize> for Pointer {
    fn from(val: usize) -> Self {
        Some(val).into()
    }
}

struct Occupied<T> {
    val: T,
    next: Pointer,
    prev: Pointer,
}

struct Free {
    next: Pointer,
}

enum Slot<T> {
    Occupied(Occupied<T>),
    Free(Free),
}

impl<T> Slot<T> {
    fn unwrap_occupied(self) -> Occupied<T> {
        match self {
            Slot::Occupied(occupied) => occupied,
            Slot::Free(_) => panic!(),
        }
    }

    fn unwrap_occupied_mut(&mut self) -> &mut Occupied<T> {
        match self {
            Slot::Occupied(occupied) => occupied,
            Slot::Free(_) => panic!(),
        }
    }

    fn unwrap_occupied_ref(&self) -> &Occupied<T> {
        match self {
            Slot::Occupied(occupied) => occupied,
            Slot::Free(_) => panic!(),
        }
    }

    fn unwrap_free_mut(&mut self) -> &mut Free {
        match self {
            Slot::Free(free) => free,
            Slot::Occupied(_) => panic!(),
        }
    }
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Slot::Free(Free {
            next: Pointer::none(),
        })
    }
}

/// A queue combined with a slab. Entries are maintained in FIFO order, but also have
/// indices assigned matching their positions in the underlying slab. These indices
/// allow for O(1) removal of arbitrary entries from the queue.
pub(super) struct SlabQueue<T> {
    slots: Vec<Slot<T>>,
    head: Pointer,
    tail: Pointer,
    free: Pointer,
    len: usize,
}

impl<T> Default for SlabQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SlabQueue<T> {
    pub(super) fn new() -> Self {
        Self {
            slots: vec![],
            head: Pointer::none(),
            tail: Pointer::none(),
            free: Pointer::none(),
            len: 0,
        }
    }

    pub(super) fn push_back(&mut self, val: T) -> usize {
        let idx = if let Some(idx) = self.free.get() {
            let slot = &mut self.slots[idx];
            self.free = slot.unwrap_free_mut().next;
            idx
        } else {
            self.slots.push(Slot::default());
            self.slots.len() - 1
        };

        let pointer = Pointer::from(idx);
        let occupied = Occupied {
            val,
            prev: self.tail,
            next: Pointer::none(),
        };

        if let Some(idx) = self.tail.get() {
            let tail = self.slots[idx].unwrap_occupied_mut();
            tail.next = pointer;
        }

        if occupied.prev.is_none() {
            self.head = pointer;
        }

        self.tail = pointer;
        self.slots[idx] = Slot::Occupied(occupied);
        self.len += 1;
        idx
    }

    pub(super) fn front(&self) -> Option<(usize, &T)> {
        self.head
            .get()
            .map(|idx| (idx, &self.slots[idx].unwrap_occupied_ref().val))
    }

    pub(super) fn pop_front(&mut self) -> Option<(usize, T)> {
        if let Some(idx) = self.head.get() {
            self.removed_unchecked(idx).map(|val| (idx, val))
        } else {
            None
        }
    }

    pub(super) fn remove(&mut self, idx: usize) -> Option<T> {
        if let Some(Slot::Occupied(_)) = self.slots.get(idx) {
            self.removed_unchecked(idx)
        } else {
            None
        }
    }

    fn removed_unchecked(&mut self, idx: usize) -> Option<T> {
        let free = Slot::Free(Free { next: self.free });
        let slot = mem::replace(&mut self.slots[idx], free);
        self.free = Pointer::from(idx);
        self.len -= 1;

        let occupied = slot.unwrap_occupied();

        if self.len == 0 {
            self.clear();
            return Some(occupied.val);
        }

        if let Some(prev) = occupied.prev.get() {
            let next = self.slots[prev].unwrap_occupied_mut();
            next.next = occupied.next;
        } else {
            self.head = occupied.next;
        }

        if let Some(next) = occupied.next.get() {
            let next = self.slots[next].unwrap_occupied_mut();
            next.prev = occupied.prev;
        } else {
            self.tail = occupied.prev;
        }

        Some(occupied.val)
    }

    fn clear(&mut self) {
        self.slots.clear();
        self.head = Pointer::none();
        self.tail = Pointer::none();
        self.free = Pointer::none();
        self.len = 0;
    }
}
