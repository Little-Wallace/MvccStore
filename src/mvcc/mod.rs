use std::sync::Arc;

pub mod user_timestamp;
pub mod tikv;
pub mod unistore;
pub mod memstore;
pub mod storage;

type Key = Vec<u8>;
type Value = Vec<u8>;

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_OLD: CfName = "old";
pub const ERR_KEY_LOCKED: &str = "key is locked";
pub const ERR_KEY_VERSION: &str = "key has been written";

pub enum StorageType {
    UserTimestampStorage,
    TiKVStorage,
    Unistore,
    Unknown,
}

pub trait MvccStorage {
    fn prewrite(&self, key: &Key, value: &Value, start_ts: u64) -> Result<(), String>;
    fn commit(&self, key: &Key, start_ts: u64, commit_ts: u64) -> Result<(), String>;
    fn rollback(&self, key: &Key, start_ts: u64) -> Result<(), String>;
    fn get(&self, key: &Key, ts: u64) -> Result<Option<Value>, String>;
    fn scan(&self, start: &Key, end: &Key, ts: u64) -> Result<Vec<Value>, String>;
}

