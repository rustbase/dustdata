use bson::Document;

use crate::bloom::BloomFilter;
use std::io::{Read, Write};

pub fn check_if_filter_exists(path: &str) -> bool {
    let _path = std::path::Path::new(path).join("filter");

    _path.exists()
}

pub fn write_filter(path: &str, filter: &BloomFilter) {
    let _path = std::path::Path::new(path).join("filter");

    let mut doc = bson::Document::new();

    let mut bitvec_comp = Vec::new();
    lzzzz::lz4f::compress_to_vec(
        &filter.bitvec.clone(),
        &mut bitvec_comp,
        &lzzzz::lz4f::Preferences::default(),
    )
    .expect("error on compressing bitvec");

    doc.insert("b", hex::encode(bitvec_comp));
    doc.insert("h", filter.hashes);

    let mut file = std::fs::File::create(_path).unwrap();
    file.write_all(&bson::to_vec(&doc).expect("Failed to serialize filter"))
        .unwrap();

    file.sync_all().unwrap();
}

pub fn read_filter(path: &str) -> BloomFilter {
    let _path = std::path::Path::new(path).join("filter");

    let mut file = std::fs::File::open(_path).unwrap();
    let mut bytes_to_read: Vec<u8> = Vec::new();
    file.read_to_end(&mut bytes_to_read).unwrap();

    let doc: Document = bson::from_slice(&bytes_to_read).unwrap();

    let bitvec_comp = doc.get("b").unwrap().as_str().unwrap().to_string();
    let hashes = doc.get("h").unwrap().as_i64().unwrap();

    let bitvec_comp_hex = hex::decode(bitvec_comp).unwrap();

    let mut bitvec = Vec::new();
    lzzzz::lz4f::decompress_to_vec(&bitvec_comp_hex, &mut bitvec).unwrap();

    BloomFilter { bitvec, hashes }
}
