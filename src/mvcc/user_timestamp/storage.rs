
use std::string::String;
use std::u64;
use std::io::Write;

use byteorder::{BigEndian};

use super::super::memstore::HashMemStore;
use super::super::Value;
use rocksdb::{DB, WriteOptions, ReadOptions, SeekKey};

const TIMESTAMP_LEN: usize = 8;

pub struct Storage {
    // Store pre-write result.
    // TODO: add wal for mem_store
    mem_store: HashMemStore,

    // Only committed value can write to DB.
    db: DB,
}

impl Storage {
    pub fn new(db: DB) -> Self {
        Self {
            mem_store: HashMemtable::new(),
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
                    let value = v.to_vec();
                    let ts = decode_ts_from_value(&value);
                    if ts == start_ts {
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
}

impl MvccStorage for Storage {
    fn prewrite(&mut self, key: Key, value: Value, ts: u64) -> Result<(), String> {
        // TODO: check write conflict
        if self.mem_store.contains_key(&key) {
            return Err(String::from("key is locked"));
        }
        if self.mem_store.insert(key, value, ts).is_some() {
            panic!("key is locked");
        }
        Ok(())
    }

    fn commit(&mut self, &key: Key, start_ts: u64, commit_ts: u64) -> Result<(), String> {
        match self.mem_store.remove(key) {
            Some((timestamp, value)) => {
                if timestamp == start_ts {
                    // Pre-write result is ok
                    let mut write_opt = WriteOptions::new();
                    write_opt.set_timestamp(commit_ts);
                    let mut v = value;
                    let encoded_value = encode_ts_to_value(start_ts, &mut v);
                    self.db.put_opt(key, &encoded_value, &write_opt);
                } else {
                    // Rollback-ed or committed by other txn
                    self.mem_store.insert(key, value, timestamp);
                }
                return Ok(());
            }
            None => {}
        }

        // Find to see if it is committed or rollback-ed
        if let Some(_) = self.get_committed_version(key, start_ts, commit_ts) {
            return Err(String::from("committed by other txn"));
        } else {
            return Err(String::from("rollback-ed by other txn"));
        }
    }

    fn rollback(&mut self, key: Key, ts: u64) -> Result<(), String> {
        match self.mem_store.remove(key) {
            Some((timestamp, _)) => {
                if timestamp != ts {
                } else {
                    // Rollback-ed or committed by other txn
                    self.mem_store.insert(key, value, timestamp);
                }
                return Ok(());
            }
            None => {}
        }

        // Find to see if it is committed or rollback-ed
        if let Some(_) = self.get_committed_version(key, start_ts, commit_ts) {
            return Err(String::from("committed by other txn"));
        } else {
            return Err(String::from("rollback-ed by other txn"));
        }
    }

    fn get(&self, key: &Key, ts: u64) -> Result<Option<Value>, String> {
        if let Some((start_ts, _)) = self.mem_store.get(key) {
            if *start_ts < ts {
                return Err(String::from("key is locked"));
            }
        }

        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(ts);
        match self.db.get_opt(key, &read_opt) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.len() - TIMESTAMP_LEN);
                res.append(v.as_slice()[TIMESTAMP_LEN..]);
                Ok(Some(res))
            }
            None => Ok(None),
        }
    }

    fn scan(&self, start: Key, end: Key, ts: u64) -> Result<Vec<Value>, String> {
        if self.mem_store.range_conflict(&start, &end, ts) {
            return Err(String::from("key is locked"));
        }
        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(ts);
        let snap = self.db.snapshot();
        let mut iter = snap.iter_opt(read_opt);
        let mut result = Vec::new();
        iter.seek(SeekKey::Key(start));
        while iter.valid() {
            let key = iter.key();
            if key.gt(end) {
                break;
            }
            result.push(Vec::from(key));
        }
        return Ok(result)
    }
}

fn encode_ts_to_value(ts: u64, value: &mut Value) -> Value {
    let mut res = Value::with_capacity(TIMESTAMP_LEN + value.len());
    res.append(ts.to_be_bytes());
    res.append(value);
    res
}

fn decode_ts_from_value(value: &Value) -> u64 {
    u64::from_be_bytes(&value.as_slice()[..8])
}