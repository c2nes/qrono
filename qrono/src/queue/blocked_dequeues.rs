use crate::data::Timestamp;
use crate::ops::DequeueResp;
use crate::promise::QronoPromise;
use crate::queue::slab_deque::SlabQueue;

use std::collections::BTreeSet;

/// A collection of blocking dequeue operations, waiting to be fulfilled or to time out.
#[derive(Default)]
pub(super) struct BlockedDequeues {
    by_arrival: SlabQueue<(Timestamp, u64, QronoPromise<DequeueResp>)>,
    by_deadline: BTreeSet<(Timestamp, usize)>,
    total_count: u64,
}

impl BlockedDequeues {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn push_back(
        &mut self,
        timeout: Timestamp,
        count: u64,
        resp: QronoPromise<DequeueResp>,
    ) {
        let key = self.by_arrival.push_back((timeout, count, resp));
        self.by_deadline.insert((timeout, key));
        self.total_count += count;
    }

    pub(super) fn pop_front(&mut self) -> Option<(Timestamp, u64, QronoPromise<DequeueResp>)> {
        match self.by_arrival.pop_front() {
            Some((key, (timeout, count, resp))) => {
                self.by_deadline.remove(&(timeout, key));
                self.total_count -= count;
                Some((timeout, count, resp))
            }
            None => None,
        }
    }

    pub(super) fn front(&mut self) -> Option<(Timestamp, u64, &QronoPromise<DequeueResp>)> {
        self.by_arrival
            .front()
            .map(|(_, (timeout, count, resp))| (*timeout, *count, resp))
    }

    pub(super) fn expire(
        &mut self,
        now: Timestamp,
    ) -> Vec<(Timestamp, u64, QronoPromise<DequeueResp>)> {
        let removed = self
            .by_deadline
            .range(..(now, usize::MAX))
            .cloned()
            .collect::<Vec<_>>();

        let mut expired = vec![];
        for (timeout, key) in removed {
            let (_, count, resp) = self.by_arrival.remove(key).unwrap();
            expired.push((timeout, count, resp));
            self.by_deadline.remove(&(timeout, key));
            self.total_count -= count;
        }
        expired
    }

    pub(super) fn next_timeout(&self) -> Option<Timestamp> {
        self.by_deadline.iter().next().map(|(t, _)| *t)
    }

    pub(super) fn total_count(&self) -> u64 {
        self.total_count
    }
}
