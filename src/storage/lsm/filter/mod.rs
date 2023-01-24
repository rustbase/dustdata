use crate::bloom::BloomFilter;
use lz4::{Decoder, EncoderBuilder};
use std::{
    io::{Read, Write},
    path,
};

pub fn check_if_filter_exists(path: &path::Path) -> bool {
    let _path = path.join("filter");

    _path.exists()
}

pub fn write_filter(path: &path::Path, filter: &BloomFilter) {
    let _path = path.join("filter");

    if !check_if_filter_exists(path) {
        std::fs::create_dir_all(_path.clone()).unwrap();
    }

    // bitvec

    let bitvec_file = std::fs::File::create(_path.join("bitvec")).unwrap();

    let mut encoder = EncoderBuilder::new()
        .level(4)
        .build(bitvec_file)
        .expect("cannot create encoder");

    encoder.write_all(&filter.bitvec).unwrap();

    encoder.flush().unwrap();

    // hashes

    let mut hashes_file = std::fs::File::create(_path.join("hashes")).unwrap();
    hashes_file.write_all(&filter.hashes.to_le_bytes()).unwrap();

    hashes_file.sync_all().unwrap();
}

pub fn read_filter(path: &path::Path) -> BloomFilter {
    let _path = path.join("filter");

    // bitvec

    let bitvec_file = std::fs::File::open(_path.join("bitvec")).unwrap();
    let mut decoder = Decoder::new(bitvec_file).unwrap();

    let mut bitvec: Vec<u8> = Vec::new();
    decoder.read_to_end(&mut bitvec).unwrap();

    // hashes

    let mut hashes_file = std::fs::File::open(_path.join("hashes")).unwrap();
    let mut hashes_read: Vec<u8> = Vec::new();
    hashes_file.read_to_end(&mut hashes_read).unwrap();

    let hashes = i64::from_le_bytes(hashes_read.try_into().unwrap());

    BloomFilter { bitvec, hashes }
}
