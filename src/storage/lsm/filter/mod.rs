use crate::bloom::BloomFilter;
use std::io::{Read, Write};

pub fn check_if_filter_exists(path: &str) -> bool {
    let _path = std::path::Path::new(path).join("filter");

    _path.exists()
}

pub fn write_filter(path: &str, filter: &BloomFilter) {
    let _path = std::path::Path::new(path).join("filter");

    if !check_if_filter_exists(path) {
        std::fs::create_dir_all(_path.clone()).unwrap();
    }

    let mut bitvec_comp = Vec::new();
    lzzzz::lz4f::compress_to_vec(
        &filter.bitvec.clone(),
        &mut bitvec_comp,
        &lzzzz::lz4f::Preferences::default(),
    )
    .expect("error on compressing bitvec");

    let mut bitvec_file = std::fs::File::create(_path.join("bitvec")).unwrap();
    bitvec_file.write_all(&bitvec_comp).unwrap();

    bitvec_file.sync_all().unwrap();

    let mut hashes_file = std::fs::File::create(_path.join("hashes")).unwrap();
    hashes_file.write_all(&filter.hashes.to_ne_bytes()).unwrap();

    hashes_file.sync_all().unwrap();
}

pub fn read_filter(path: &str) -> BloomFilter {
    let _path = std::path::Path::new(path).join("filter");

    let mut bitvec_file = std::fs::File::open(_path.join("bitvec")).unwrap();
    let mut bitvec_read: Vec<u8> = Vec::new();
    bitvec_file.read_to_end(&mut bitvec_read).unwrap();

    let mut bitvec = Vec::new();
    lzzzz::lz4f::decompress_to_vec(&bitvec_read, &mut bitvec).unwrap();

    let mut hashes_file = std::fs::File::open(_path.join("hashes")).unwrap();
    let mut hashes_read: Vec<u8> = Vec::new();
    hashes_file.read_to_end(&mut hashes_read).unwrap();

    let hashes = i64::from_ne_bytes(hashes_read.try_into().unwrap());

    BloomFilter { bitvec, hashes }
}
