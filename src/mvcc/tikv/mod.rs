
use std::string::String;
use std::u64;
use std::io::Write;

use super::{Key, Value};

use rocksdb::{DB, WriteOptions, ReadOptions, SeekKey};
use std::collections::{HashSet};
use super::super::util::engine::{LOCK_CF, WRITE_CF, DATA_CF};
use super::MvccStorage;

const TIMESTAMP_LEN: usize = 8;

pub struct Storage {
    // Store pre-write result.
    // TODO: add wal for mem_store
    // latch_map: HashSet<Key>,

    // Only committed value can write to DB.

    db: DB,
}

impl Storage {
    pub fn new(db: DB) -> Self {
        Self {
            db,
        }
    }
}
impl MvccStorage  for Storage {
    fn prewrite(&self, key: &Key, value: &Value, ts: u64) -> Result<(), String> {
        // TODO: check write conflict
        let mut m_value = value.clone();
        let mut m_key = key.clone();
        let lock_cf = self.db.cf_handle(LOCK_CF).unwrap();
        let write_cf = self.db.cf_handle(WRITE_CF).unwrap();
        let options = WriteOptions::new();
        let read_options = ReadOptions::new();
        let ret = self.db.get_cf_opt(lock_cf, m_key.as_ref(), &read_options)?;
        if let Some(v) = ret {
            return Err(String::from("key has been locked"));
        }
        let key_ts = encode_ts_to_key(ts, &mut m_key);
        let value_ts = encode_ts_to_value(ts, &mut m_value);
        self.db.put_cf_opt(lock_cf, &key_ts, &value_ts, &options)?;
        Ok(())
    }

    fn commit(&self, key: &Key, start_ts: u64, commit_ts: u64) -> Result<(), String> {
        Err(String::from("not support"))
//        match self.mem_store.remove(key) {
//            Some((timestamp, value)) => {
//                if timestamp == start_ts {
//                    // Pre-write result is ok
//                    let mut write_opt = WriteOptions::new();
//                    write_opt.set_timestamp(commit_ts);
//                    let mut v = value;
//                    let encoded_value = encode_ts_to_value(start_ts, &mut v);
//                    self.db.put_opt(key, &encoded_value, &write_opt);
//                } else {
//                    // Rollback-ed or committed by other txn
//                    self.mem_store.insert(key, value, timestamp);
//                }
//                return Ok(());
//            }
//            None => {}
//        }

        // Find to see if it is committed or rollback-ed
//        if let Some(_) = self.get_committed_version(key, start_ts, commit_ts) {
//            return Err(String::from("committed by other txn"));
//        } else {
//            return Err(String::from("rollback-ed by other txn"));
//        }
    }

    fn rollback(&self, key: &Key, ts: u64) -> Result<(), String> {
        // todo
        Err(String::from("not support"))
    }

    fn get(&self, key: &Key, ts: u64) -> Result<Option<Value>, String> {
//        if let Some((start_ts, _)) = self.mem_store.get(key) {
//            if *start_ts < ts {
//                return Err(String::from("key is locked"));
//            }
//        }

        let mut read_opt = ReadOptions::new();
        read_opt.set_timestamp(ts);
        match self.db.get_opt(key, &read_opt)? {
            Some(v) => {
                let mut value = v.to_vec();
                let res = &value[TIMESTAMP_LEN..].to_vec();
                Ok(Some(res.clone()))
            }
            None => Ok(None)
        }
    }

    fn scan(&self, start: &Key, end: &Key, ts: u64) -> Result<Vec<Value>, String> {
        Err(String::from("not support"))
    }
}

fn encode_ts_to_value(ts: u64, value: &mut Value) -> Value {
    let mut res = Value::with_capacity(TIMESTAMP_LEN + value.len());
    let mut ts = ts;
    // res.append(ts.to_be_bytes());
    // TODO:  expected mutable reference, found array of 8 elements
    res.append(value);
    res
}

fn encode_ts_to_key(ts: u64, key: &mut Key) -> Key {
    let mut res = Key::with_capacity(TIMESTAMP_LEN + key.len());
    res.append(key);
//    let mut reverse_ts = u64::MAX - ts;
//    res.append(reverse_ts.to_be_bytes());
    // TODO: expected mutable reference, found array of 8 elements

    res
}

fn decode_ts_from_value(value: &Value) -> u64 {
    // u64::from_be_bytes(&value.as_slice()[..8].as_ref())
    // TODO: expected array of 8 elements, found &[u8]
    return 0;
}
