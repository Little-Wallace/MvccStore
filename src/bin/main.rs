use mvccstore::mvcc::storage::create_storage_cf;
use mvccstore::mvcc::{StorageType, MvccStorage};
use clap::{App, Arg};
use rocksdb::{DBOptions, ColumnFamilyOptions};
use std::sync::Arc;
use rand::random;
use std::thread;
use rocksdb::rocksdb_options::u64_to_bytes;

const PREPARE_THREAD_NUM: usize = 4;

fn prepare(storage: &Arc<dyn MvccStorage>, key_num: usize, seq: bool, value_size: usize) {
    let mut sorted_kv = Vec::new();
    for i in 0..key_num {
        sorted_kv.push(i);
    }
    if !seq {
        for j in 0..key_num {
            let i = random();
            let j = random();
            if i != j {
                sorted_kv.swap(i, j);
            }
        }
    }
    let mut handlers = Vec::default();
    for i in 0..PREPARE_THREAD_NUM {
        let data_size = (sorted_kv.len() / PREPARE_THREAD_NUM);
        let cursor = i * data_size;
        let data = if i + 1 == PREPARE_THREAD_NUM {
            sorted_kv[cursor..].to_vec()
        } else {
            sorted_kv[cursor..(i + 1) * data_size].to_vec()
        };
        let store = storage.clone();
        let value = vec![1 as u8; value_size];
        let handle = thread::spawn(move || {
            println!("{} begin write {} keys", i, data.len());
            for j in data {
                let key = u64_to_bytes(j as u64);
                if store.prewrite(&key, &value, 1).is_ok() {
                    store.commit(&key, 1, 2);
                }
            }
            println!("{} end write keys", i);
        });
        handlers.push(handle);
    }
    for h in handlers.into_iter() {
        h.join();
    }
    println!("end prepare keys");
}

fn main() {
   let matches = App::new("MvccStore")
        .about("A toy storage, used to compare different mvcc storage models.")
        .arg(
            Arg::with_name("path")
                .short("p")
                .long("path")
                .value_name("PATH")
                .help("set the db path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("work")
                .value_name("work")
                .help("prepare or read")
                .possible_values(&[
                    "user_timestamp", "tikv",
                ])
                .takes_value(true),
        )
        .arg(
            Arg::with_name("type")
                .short("t")
                .long("type")
                .takes_value(true)
                .value_name("TYPE")
                .possible_values(&[
                    "user_timestamp", "tikv",
                ])
                .help("Set the storage type"),
        ).get_matches();
    let path = matches.value_of("path").unwrap();
    let db_type_str = matches.value_of("type").unwrap();
    let storage_type = match db_type_str {
        "user_timestamp" => StorageType::UserTimestampStorage,
        "tikv" => StorageType::TiKVStorage,
        _ => StorageType::Unknown
    };
    let mut options = DBOptions::default();
    options.create_if_missing(true);
    options.allow_concurrent_memtable_write(true);
    options.set_writable_file_max_buffer_size(8 * 1024 * 1024);
    let mut cf = ColumnFamilyOptions::new();
    cf.set_write_buffer_size(2 * 1024 * 1024);
    let storage = create_storage_cf(path, storage_type, options, vec![("default", cf),]).unwrap();
    println!("========begin prepare data");
    prepare(&storage, 100000, true, 128);
    println!("========end prepare data");
}
