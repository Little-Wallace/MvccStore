///
/// Several kinds of mem-store
///

use super::super::util::collection::HashMap as Map;
use super::{Key, Value};
use std::cmp::Ordering;

type V = (u64, Value);

/// Hash table
pub struct MemStore {
    // key -> (ts, value)
    map: Map<Key, V>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            map: Map::default(),
        }
    }

    pub fn insert(&mut self, key: Key, value: Value, ts: u64) -> Option<V> {
        self.map.insert(key, (ts, value))
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        self.map.contains_key(key)
    }

    pub fn remove(&mut self, key: &Key) -> Option<V> {
        self.map.remove(key)
    }

    pub fn get(&self, key: &Key) -> Option<&V> {
        self.map.get(key)
    }
    pub fn range_conflict(&self, start: &Key, end: &Key, ts: u64) -> bool {
        // TODO:  check whether there is one key locked, which is between range [start,end].
        return false;
    }
}

/// Skip list
pub struct SkipList {}
