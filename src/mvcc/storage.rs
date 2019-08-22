use rocksdb::{DBOptions, ColumnFamilyOptions};
use super::StorageType;
use std::sync::Arc;
use super::user_timestamp::create_storage as create_ts_storage;
use super::user_timestamp::create_storage_cf as create_ts_storage_cf;
use super::tikv::create_storage as create_tikv_storage;
use super::tikv::create_storage_cf as create_tikv_storage_cf;
use super::MvccStorage;

pub fn create_storage_opt(path: &str, storage_type: StorageType, option: DBOptions) -> Result<Arc<dyn MvccStorage>, String> {
    match storage_type {
        StorageType::UserTimestampStorage => {
            return create_ts_storage(option, path);
        },
        StorageType::TiKVStorage => {
            return create_tikv_storage(option, path);
        }
        _ => Err(String::from("no support type to create"))
    }
}

pub fn create_storage(path: &str, storage_type: StorageType) -> Result<Arc<dyn MvccStorage>, String> {
    let mut option = DBOptions::default();
    option.create_if_missing(true);
    return create_storage_opt(path, storage_type, option);
}

pub fn create_storage_cf(path: &str, storage_type: StorageType, option: DBOptions, cfds: Vec<(&str, ColumnFamilyOptions)>) -> Result<Arc<dyn MvccStorage>, String> {
    match storage_type {
        StorageType::UserTimestampStorage => {
            return create_ts_storage_cf(option, path, cfds);
        },
        StorageType::TiKVStorage => {
            return create_tikv_storage_cf(option, path, cfds);
        }
        _ => Err(String::from("no support type to create"))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::Path;
    use std::str;
    use std::string::String;
    use tempdir::TempDir;
    use std::u64;
    use super::super::{Value, Key};
    use super::super::{MvccStorage, ERR_KEY_LOCKED, ERR_KEY_VERSION};


    fn prewrite(storage: &Arc<dyn MvccStorage>, key: &str, value: &str, ts: u64) -> Result<(), String>{
        let k = key.as_bytes().to_vec();
        let v = value.as_bytes().to_vec();
        storage.prewrite(&k, &v, ts)
    }

    fn commit(storage: &Arc<dyn MvccStorage>, key: &str, start_ts: u64, commit_ts: u64) -> Result<(), String> {
        let k = key.as_bytes().to_vec();
        storage.commit(&k, start_ts, commit_ts)
    }

    fn read(storage: &Arc<dyn MvccStorage>, key: &str, commit_ts: u64) -> Result<Option<Value>, String> {
        let k = key.as_bytes().to_vec();
        storage.get(&k, commit_ts)
    }

    fn inner_test_mvcc_prewrite(storage_type: StorageType) {
        let path = TempDir::new("_mvcc_prewrite").expect("");
        let storage = create_storage(path.path().to_str().unwrap(), storage_type).unwrap();
        prewrite(&storage, "abcd", "v1", 1).unwrap();
        let e = prewrite(&storage, "abcd", "v2", 1).err().unwrap();
        assert_eq!(e, ERR_KEY_LOCKED.to_string());
        commit(&storage, "abcd", 1, 2).unwrap();
        let value = read(&storage, "abcd", 3).unwrap();
        let e = prewrite(&storage, "abcd", "v2", 1).err().unwrap();
        assert_eq!(e, ERR_KEY_VERSION.to_string());
    }

    fn inner_test_mvcc_read(storage_type: StorageType) {
        let path = TempDir::new("_mvcc_read").expect("");
        let storage = create_storage(path.path().to_str().unwrap(), storage_type).unwrap();
        prewrite(&storage, "abcd", "v1", 1).unwrap();
        let e = read(&storage, "abcd", 1).err().unwrap();
        assert_eq!(e, ERR_KEY_LOCKED.to_string());
        let ret= read(&storage, "abcd", 0).unwrap();
        assert!(ret.is_none());
        commit(&storage, "abcd", 1, 2).unwrap();
        let ret = read(&storage, "abcd", 1).unwrap();
        assert!(ret.is_none());
        prewrite(&storage, "abcd", "v2", 3).unwrap();
        let ret = read(&storage, "abcd", 2).unwrap();
        assert!(ret.is_some());
        let value = ret.unwrap();
        assert_eq!(value.as_slice(), "v1".as_bytes());
        commit(&storage, "abcd", 3, 3).unwrap();
        let ret = read(&storage, "abcd", 2).unwrap().unwrap();
        assert_eq!(ret.as_slice(), "v1".as_bytes());
        let ret = read(&storage, "abcd", u64::MAX).unwrap().unwrap();
        assert_eq!(ret.as_slice(), "v2".as_bytes());
    }


    #[test]
    fn test_tikv_storage() {
        println!("====prewrite start");
        inner_test_mvcc_prewrite(StorageType::TiKVStorage);
        println!("====test read start");
        inner_test_mvcc_read(StorageType::TiKVStorage);
    }

    #[test]
    fn test_user_timestamp_storage() {
        println!("====prewrite start");
        inner_test_mvcc_prewrite(StorageType::UserTimestampStorage);
        println!("====test read start");
        inner_test_mvcc_read(StorageType::UserTimestampStorage);
    }
}
