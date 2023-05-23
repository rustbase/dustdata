use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path;

use super::error::{Error, ErrorKind, Result};

fn get_last_file_index(path: path::PathBuf) -> usize {
    let files = fs::read_dir(path).unwrap();

    files
        .filter(|segment| {
            let segment = segment.as_ref().unwrap();

            segment.file_name().to_str().unwrap().ends_with(".db")
        })
        .count()
}

pub struct Segment;

impl Segment {
    /// Returns the segment in bytes and the offset of each document
    pub fn from_tree(tree: &HashMap<String, bson::Bson>) -> (Vec<u8>, Vec<(&String, u64)>) {
        let mut segment = Vec::new();
        let mut offsets = Vec::new();

        for (key, value) in tree.iter() {
            let offset = segment.len() as u64;

            // we need to wrap the value in a document
            let value_to_doc = bson::doc! {
                "_": value,
            };

            // extend the segment (the document length is already in the bson document)
            let bytes_value = bson::to_vec(&value_to_doc).unwrap();
            segment.extend_from_slice(&bytes_value);

            // push the key and the offset

            offsets.push((key, offset));
        }

        (segment, offsets)
    }

    pub fn read_with_offset(offset: u64, segment: Vec<u8>) -> Result<Option<bson::Bson>> {
        // read the first bytes to see document length
        let mut bson_length = [0; 4];

        let mut cursor = std::io::Cursor::new(segment);

        // seek to offset pos and read the first byte
        cursor.seek(SeekFrom::Start(offset)).unwrap();
        cursor.read_exact(&mut bson_length).unwrap();

        let bson_length = i32::from_le_bytes(bson_length);
        // now we know the document length, we can read the document

        let mut document_bytes = vec![0; bson_length as usize];

        // go to the offset again and read the document
        cursor.seek(SeekFrom::Start(offset)).unwrap();
        cursor.read_exact(&mut document_bytes).unwrap();

        // deserialize the document
        let doc: bson::Document =
            bson::from_slice(&document_bytes).map_err(|_| Error::new(ErrorKind::Corrupted))?;

        let bson = doc.get("_").unwrap().clone();

        Ok(Some(bson)) // done
    }
}

#[derive(Clone)]
pub struct SSTable {
    path: path::PathBuf,
}

impl SSTable {
    pub fn new(sstable_path: path::PathBuf) -> Self {
        if !path::Path::new(&sstable_path).exists() {
            std::fs::create_dir_all(&sstable_path).unwrap();
        }

        Self { path: sstable_path }
    }

    pub fn write_segment_file(&self, segment: Vec<u8>) -> std::io::Result<usize> {
        // write metadata into segment
        let metadata = bson::doc! {
            "version": env!("CARGO_PKG_VERSION"),
        };

        let mut full_file = Vec::new();
        full_file.extend_from_slice(&bson::to_vec(&metadata).unwrap());

        full_file.extend_from_slice(&segment);

        let segment_index = get_last_file_index(self.path.clone());
        let filename = format!("Data_{}.db", segment_index);

        fs::write(self.path.join(filename), full_file)?;

        Ok(segment_index)
    }

    pub fn read_segment_file(&self, segment_index: usize) -> std::io::Result<Vec<u8>> {
        let filename = format!("Data_{}.db", segment_index);

        let segment_with_metadata = fs::read(self.path.join(filename))?;

        let mut metadata_length = [0; 4];
        metadata_length.copy_from_slice(&segment_with_metadata[0..4]);

        let metadata_length = i32::from_le_bytes(metadata_length);

        let segment_without_metadata = segment_with_metadata.split_at(metadata_length as usize).1;

        Ok(segment_without_metadata.to_vec())
    }

    pub fn get(&self, file_index: &usize, offset: &u64) -> Result<Option<bson::Bson>> {
        let segment = self.read_segment_file(*file_index).unwrap();

        let document = Segment::read_with_offset(*offset, segment).unwrap();

        Ok(document)
    }
}
