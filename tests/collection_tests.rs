use std::thread;

// use dustdata::{collection::Transaction, DustData, DustDataConfig};

#[test]
pub fn collection_insert_operation() {
    //     DustDataConfig::new().data_path("test_data").build();

    //     let dustdata = DustData::new().unwrap();

    //     let collection = dustdata.collection::<i32>("test_collection").unwrap();

    // let mut threads = Vec::new();

    // for i in 0..10 {
    //     let collection = collection.clone();
    //     let tx = thread::spawn(move || {
    //         let mut xact = collection.branch_start().unwrap();
    //         xact.insert(&i.to_string(), i).unwrap();

    //         collection.commit(xact).unwrap();
    //     });

    //     threads.push(tx);
    // }

    // for tx in threads {
    //     tx.join().unwrap();
    // }

    // for i in 0..10 {
    //     let value = collection.get(&i.to_string()).unwrap().unwrap();

    //     assert_eq!(value, i);
    // }
}
