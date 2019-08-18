use mvccstore::mvcc::{create_default_storage, StorageType};
use clap::{App, Arg};

fn main() {
   let matches = App::new("MvccStore")
        .about("A toy storage, used to compare different mvcc storage models.")
        .arg(
            Arg::with_name("path")
                .short("P")
                .long("path")
                .value_name("PATH")
                .help("set the db path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("type")
                .short("t")
                .alias("path")
                .takes_value(true)
                .value_name("TYPE")
                .possible_values(&[
                    "user_timestamp", "tikv",
                ])
                .help("Set the log level"),
        ).get_matches();
    let path = matches.value_of("path").unwrap();
    let db_type_str = matches.value_of("type").unwrap();
    let storage_type = match db_type_str {
        "user_timestamp" => StorageType::UserTimestampStorage,
        "tikv" => StorageType::TiKVStorage,
        _ => StorageType::Unknown
    };
    let storage = create_default_storage(path, storage_type).unwrap();
    println!("Hello, world!");
}
