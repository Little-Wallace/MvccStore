
use std::string::String;
use std::u64;


use super::super::memstore::MemStore;
use super::super::{Key, Value};
use rocksdb::{DB, WriteOptions, ReadOptions, SeekKey, DBOptions, ColumnFamilyOptions};
use rocksdb::rocksdb_options::{bytes_to_u64, u64_to_bytes};
use super::super::MvccStorage;
use std::sync::{Arc, RwLock};
use rocksdb::rocksdb::Writable;
use super::super::{ERR_KEY_LOCKED, ERR_KEY_VERSION};

const TIMESTAMP_LEN: usize = 16;

pub struct Storage {
    // Store pre-write result.
    // TODO: add wal for mem_store
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

    // Return committed value whose start ts equal to `start_ts`, or return None.
    fn get_committed_version(&self, key: &Key, start_ts: u64, commit_ts: u64) -> Option<Value> {
        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(commit_ts);
        match self.db.get_opt(key, &read_opt) {
            Ok(ret) => {
                if let Some(value) = ret {
                    let value = value.to_vec();
                    let txn_start_ts = decode_start_ts_from_value(&value);
                    let txn_commit_ts = decode_commit_ts_from_value(&value);
                    if txn_commit_ts == commit_ts &&  txn_start_ts == start_ts {
                        let mut res = Value::with_capacity(value.len() - TIMESTAMP_LEN);
                        res.copy_from_slice(&value.as_slice()[TIMESTAMP_LEN..]);
                        return Some(res)
                    }
                }
            },
            Err(_) => (),
        }
        return None;
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

    fn unlock_uncommitted_data(&self, key: &Key, start_ts: u64) -> Result<Option<Value>, String> {
        let mut mem_store = self.mem_store.write().unwrap();
        match mem_store.remove(&key) {
            Some((timestamp, value)) => {
                if timestamp == start_ts {
                    // Pre-write result is ok
                    Ok(Some(value))
                } else {
                    // Rollback-ed or committed by other txn
                    mem_store.insert(key.clone(), value, timestamp);
                    Err(String::from("This key was prewrite by other transaction"))
                }
            }
            None => Ok(None)
        }
    }
}

impl MvccStorage for Storage {
    fn prewrite(&self, key: &Key, value: &Value, ts: u64) -> Result<(), String> {
        // TODO: check write conflict
        if self.mem_store.read().unwrap().contains_key(&key) {
            return Err(String::from(ERR_KEY_LOCKED));
        }
        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(u64::MAX);
        let ret = self.db.get_opt(key, &read_opt)?;
        if let Some(value) = ret {
            let value = value.to_vec();
            let commit_ts = decode_commit_ts_from_value(&value);
            if commit_ts >= ts {
                return Err(String::from(ERR_KEY_VERSION));
            }
        }
        let mut mem_store = self.mem_store.write().unwrap();
        mem_store.insert(key.clone(), value.clone(), ts);
        Ok(())
    }

    fn commit(&self, key: &Key, start_ts: u64, commit_ts: u64) -> Result<(), String> {
        // we should keep key in lock until data has been committed into db.
        if let Some(value) = self.get_uncommitted_data(&key, start_ts)? {
            let mut write_opt = WriteOptions::new();
            write_opt.set_timestamp(commit_ts);
            let mut v = value;
            encode_ts_to_value(start_ts, &mut v);
            encode_ts_to_value(commit_ts, &mut v);
            self.db.put_opt(&key, &v, &write_opt);
            self.unlock_uncommitted_data(&key, start_ts).unwrap();
            return Ok(());
        }
        // Find to see if it is committed or rollback-ed
        if let Some(_) = self.get_committed_version(key, start_ts, commit_ts) {
            return Ok(());
        } else {
            return Err(String::from("rollback-ed by other txn"));
        }
    }

    fn rollback(&self, key: &Key, start_ts: u64) -> Result<(), String> {
        // when rollback, we could remove key at once
        if let Some(value) = self.unlock_uncommitted_data(key, start_ts)? {
            // TODO: write rollback into WRITE_CF
            return Ok(());
        }

        // Find to see if it is committed or rollback-ed
        if let Some(_) = self.get_committed_version(key, start_ts, u64::MAX) {
            return Err(String::from("committed by other txn"));
        } else {
            return Err(String::from("rollback-ed by other txn"));
        }
    }

    fn get(&self, key: &Key, ts: u64) -> Result<Option<Value>, String> {
        if let Some((start_ts, _)) = self.mem_store.read().unwrap().get(key) {
            if *start_ts <= ts {
                return Err(String::from(ERR_KEY_LOCKED));
            }
        }
        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(ts);
        match self.db.get_opt(key, &read_opt)? {
            Some(v) => {
                let value = v.to_vec();
                let value_len = value.len() - TIMESTAMP_LEN;
                let res = &value[..value_len].to_owned();
                Ok(Some(res.clone()))
            }
            None => Ok(None),
        }
    }

    fn scan(&self, start: &Key, end: &Key, ts: u64) -> Result<Vec<Value>, String> {
//        if self.mem_store.range_conflict(&start, &end, ts) {
//            return Err(String::from("key is locked"));
//        }
//        let mut read_opt = ReadOptions::new();
//        read_opt.set_timestamp(ts);
//        let snap = self.db.snapshot();
//        let mut iter = snap.iter_opt(read_opt);
        let mut result = Vec::new();
//        iter.seek(SeekKey::Key(start));
//        while iter.valid() {
//            let key = iter.key();
//            if key.gt(end) {
//                break;
//            }
//            result.push(Vec::from(key));
//        }
        return Ok(result)
    }
}

fn encode_ts_to_value(ts: u64, value: &mut Value) {
    let mut v = u64_to_bytes(ts);
    value.append(&mut v);
}

fn decode_start_ts_from_value(value: &Value) -> u64 {
    let l = value.len();
    bytes_to_u64(&value[l-16..l-8])
}

fn decode_commit_ts_from_value(value: &Value) -> u64 {
    let l = value.len();
    bytes_to_u64(&value[l-8..])
}

pub fn create_storage(options: DBOptions, path: &str) -> Result<Arc<dyn MvccStorage>, String> {
    let mut option = options;
    option.set_user_timestamp_comparator(8);
    let db = DB::open_opt(option, path)?;
    let storage = Storage::new(db);
    return Ok(Arc::new(storage));
}

pub fn create_storage_cf(options: DBOptions, path: &str, cfds: Vec<(&str, ColumnFamilyOptions)>) -> Result<Arc<dyn MvccStorage>, String> {
    let mut cfds = cfds;
    for (_, cf) in cfds.iter_mut() {
        cf.set_timestamp_comparator(8);
    }
    let db = DB::open_cf(options, path, cfds)?;
    let storage = Storage::new(db);
    return Ok(Arc::new(storage));
}
