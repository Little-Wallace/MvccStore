
use std::string::String;
use std::u64;
use std::io::Write;

use super::{Key, Value};

use rocksdb::{DB, ReadOptions, SeekKey, DBOptions, ColumnFamilyOptions};
use std::collections::{HashSet};
use super::super::util::engine::FixedSuffixSliceTransform;
use super::memstore::MemStore;
use super::{MvccStorage, CF_DEFAULT, ERR_KEY_VERSION, ERR_KEY_LOCKED};
use std::sync::{Arc, RwLock};
use rocksdb::rocksdb_options::{bytes_to_u64, u64_to_bytes};
use rocksdb::rocksdb::Writable;

const TIMESTAMP_LEN: usize = 16;

pub struct Storage {
    // Store pre-write result.
    // TODO: add wal for mem_store
    // latch_map: HashSet<Key>,
    mem_store: RwLock<MemStore>,

    // Only committed value can write to DB.

    db: DB,
}

impl Storage {
    pub fn new(db: DB) -> Self {
        Self {
            mem_store: RwLock::new(MemStore::new()),
            db,
        }
    }

    fn get_uncommitted_data(&self, key: &Key, start_ts: u64) -> Result<Option<Value>, String> {
        let mut mem_store = self.mem_store.read().unwrap();
        match mem_store.get(&key) {
            Some((timestamp, value)) => {
                if *timestamp == start_ts {
                    // Pre-write result is ok
                    Ok(Some(value.clone()))
                } else {
                    // Rollback-ed or committed by other txn
                    Err(String::from("This key was prewrite by other transaction"))
                }
            }
            None => Ok(None)
        }
    }
}

impl MvccStorage  for Storage {
    fn prewrite(&self, key: &Key, value: &Value, ts: u64) -> Result<(), String> {
         // TODO: check write conflict
        if self.mem_store.read().unwrap().contains_key(&key) {
            return Err(String::from(ERR_KEY_LOCKED));
        }
        let mut read_opt = ReadOptions::new();
        // read_opt.set_prefix_same_as_start(true);
        let mut iter = self.db.iter_opt(read_opt);
        let key_ts = encode_ts_to_key(u64::MAX, key);
        iter.seek(SeekKey::Key(&key_ts));
        if iter.valid() {
            let k = iter.key();
            let commit_ts = decode_ts_from_key(k);
            let k_len = k.len();
            let write_key = k[..(k_len-8)].to_vec();
            if write_key == *key && commit_ts >= ts {
                return Err(String::from(ERR_KEY_VERSION));
            }
        }
        let mut mem_store = self.mem_store.write().unwrap();
        mem_store.insert(key.clone(), value.clone(), ts);
        Ok(())
    }

    fn commit(&self, key: &Key, start_ts: u64, commit_ts: u64) -> Result<(), String> {
        if let Some(value) = self.get_uncommitted_data(&key, start_ts)? {
            let mut v = encode_ts_to_value(start_ts, &value);
            let key_ts = encode_ts_to_key(commit_ts, key);
            self.db.put(&key_ts, &v);
            {
                let mut mem_store = self.mem_store.write().unwrap();
                mem_store.remove(&key);
            }
            return Ok(());
        }
        // Find to see if it is committed or rollback-ed
        return Err(String::from("rollback-ed by other txn"));
    }

    fn rollback(&self, key: &Key, ts: u64) -> Result<(), String> {
        // todo
        Err(String::from("not support"))
    }

    fn get(&self, key: &Key, ts: u64) -> Result<Option<Value>, String> {
        if let Some((start_ts, _)) = self.mem_store.read().unwrap().get(key) {
            if *start_ts <= ts {
                return Err(String::from(ERR_KEY_LOCKED));
            }
        }
        let mut read_opt = ReadOptions::new();
        let mut iter = self.db.iter_opt(read_opt);
        let key_ts = encode_ts_to_key(ts, key);
        iter.seek(SeekKey::Key(&key_ts));
        if iter.valid() {
            let k = iter.key();
            let l = k.len();
            let commit_ts = bytes_to_u64(&k[l-8..]);
            let write_key = k[..l-8].to_vec();
            if write_key == *key && commit_ts <= ts {
                let value = iter.value();
                return Ok(Some(decode_data_from_value(value)))
            }
        }
        Ok(None)
    }

    fn scan(&self, start: &Key, end: &Key, ts: u64) -> Result<Vec<Value>, String> {
        Err(String::from("not support"))
    }
}

fn encode_ts_to_value(ts: u64, value: &Value) -> Value {
    let mut res = value.clone();
    // res.append(ts.to_be_bytes());
    let mut ts_data = u64_to_bytes(ts);
    // TODO:  expected mutable reference, found array of 8 elements
    res.append(&mut ts_data);
    res
}

fn encode_ts_to_key(ts: u64, key: &Key) -> Key {
    let mut res = key.clone();
    let mut ts_data= u64_to_bytes(ts);
    res.append(&mut ts_data);
    res
}

fn decode_ts_from_value(value: &[u8]) -> u64 {
    // u64::from_be_bytes(&value.as_slice()[..8].as_ref())
    let l = value.len();
    return bytes_to_u64(&value[l-8..]);
}

fn decode_data_from_value(value: &[u8]) -> Vec<u8> {
    let l = value.len();
    return value[..(l-8)].to_vec();
}

fn decode_ts_from_key(key: &[u8]) -> u64 {
    let l = key.len();
    return bytes_to_u64(&key[l-8..]);
}

pub fn create_storage(options: DBOptions, path: &str) -> Result<Arc<dyn MvccStorage>, String> {
    let mut cfoption = ColumnFamilyOptions::new();
    let f = Box::new(FixedSuffixSliceTransform::new(8));
    cfoption.set_prefix_extractor("FixedSuffixSliceTransform", f).unwrap();
    let mut cfds = Vec::new();
    cfds.push((CF_DEFAULT, cfoption));
    let mut db = DB::open_cf(options, path, cfds)?;
    let storage = Storage::new(db);
    return Ok(Arc::new(storage));
}