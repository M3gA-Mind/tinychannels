//! Small bounded memories the adapters keep about recent traffic.

use std::collections::{HashMap, HashSet, VecDeque};

/// A first-in-first-out set: remembers the last `capacity` inserted ids.
pub(crate) struct SeenSet {
    capacity: usize,
    order: VecDeque<String>,
    ids: HashSet<String>,
}

impl SeenSet {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            ids: HashSet::new(),
        }
    }

    pub(crate) fn contains(&self, id: &str) -> bool {
        self.ids.contains(id)
    }

    pub(crate) fn insert(&mut self, id: String) {
        if !self.ids.insert(id.clone()) {
            return;
        }
        self.order.push_back(id);
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.ids.remove(&oldest);
            }
        }
    }
}

/// A map that keeps the `capacity` most recently *created* keys. Updating an
/// existing key replaces its value without refreshing its age.
pub(crate) struct BoundedMap {
    capacity: usize,
    order: VecDeque<String>,
    values: HashMap<String, String>,
}

impl BoundedMap {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            values: HashMap::new(),
        }
    }

    pub(crate) fn insert(&mut self, key: &str, value: &str) {
        if self
            .values
            .insert(key.to_owned(), value.to_owned())
            .is_none()
        {
            self.order.push_back(key.to_owned());
        }
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<String> {
        self.values.get(key).cloned()
    }
}

/// Clears a shared slot when dropped, including when the owning task is
/// aborted. A listener that parks keeps its inbound sender in such a slot; if
/// the slot outlived the listener the module's inbound forwarder would never
/// see its channel close and provider shutdown would wait forever.
pub(crate) struct ClearOnDrop<'a, T>(pub(crate) &'a parking_lot::Mutex<Option<T>>);

impl<T> Drop for ClearOnDrop<'_, T> {
    fn drop(&mut self) {
        *self.0.lock() = None;
    }
}

#[cfg(test)]
#[path = "window_tests.rs"]
mod tests;
