use std::collections::BTreeMap;
use std::fs;
use std::os::unix::prelude::FileExt;
use std::path;

use bson::Document;

use super::writer::Writer;

fn get_last_index(path: path::PathBuf) -> usize {
    let segments = fs::read_dir(path).unwrap();

    segments
        .filter(|segment| {
            let segment = segment.as_ref().unwrap();

            segment.file_name().to_str().unwrap().ends_with(".db")
        })
        .count()
}

fn create_filename(path: path::PathBuf) -> String {
    let count = get_last_index(path);

    format!("Data_{}_{}.db", count, env!("CARGO_PKG_VERSION"))
}

fn get_file_that_starts_with_index(path: path::PathBuf, index: usize) -> String {
    let mut segments = fs::read_dir(path).unwrap();
    let segment = segments.find(|segment| {
        let segment = segment.as_ref().unwrap();

        segment
            .file_name()
            .to_str()
            .unwrap()
            .starts_with(&format!("Data_{}", index))
    });

    segment
        .unwrap()
        .unwrap()
        .file_name()
        .to_str()
        .unwrap()
        .to_string()
}

impl Writer for Segment {
    fn file(&self) -> fs::File {
        self.file.try_clone().unwrap()
    }

    fn file_index(&self) -> usize {
        self.file_index
    }
}

#[derive(Clone, Debug)]
pub struct Token {
    pub key: String,
    pub value: bson::Document,
    pub segment_offset: usize,
}

pub struct Segment {
    pub path_data: String,
    pub file: fs::File,
    pub file_index: usize,
}

impl Segment {
    pub fn new(path: &str) -> Self {
        let _path = path::Path::new(path).join("data");

        if !_path.exists() {
            fs::create_dir_all(_path.clone()).unwrap();
        }

        let filename = create_filename(_path.clone());
        let count = get_last_index(_path.clone());

        let file_path = _path.join(filename);

        Self {
            path_data: file_path.clone().to_str().unwrap().to_string(),
            file: fs::File::create(file_path).unwrap(),
            file_index: count,
        }
    }

    pub fn read_with_offset(offset: String, path: String) -> Option<bson::Bson> {
        let splited_offset = offset.split('_').collect::<Vec<&str>>();
        let file_index = splited_offset[0].parse::<u64>().unwrap();
        let offset = splited_offset[1].parse::<u64>().unwrap();

        let path = path::Path::new(&path).join("data");
        let file_path = path.join(get_file_that_starts_with_index(
            (*path).to_path_buf(),
            file_index as usize,
        ));

        if !path.exists() {
            return None;
        }

        let file = fs::File::open(file_path).unwrap();

        let mut document_length = [0; 1];
        file.read_at(&mut document_length, offset).unwrap();

        let mut document_bytes = vec![0; document_length[0] as usize];
        file.read_at(&mut document_bytes, offset).unwrap();

        let document: Document = bson::from_slice(&document_bytes).unwrap();

        Some(document.get("_value").unwrap().clone())
    }

    pub fn write(&mut self, key: &str, value: bson::Bson) -> (String, String) {
        // Returns the key and the offset to put in sparse index
        (key.to_string(), self.persist(key, value).unwrap())
    }

    pub fn from_tree(
        tree: &BTreeMap<String, bson::Bson>,
        path: &str,
    ) -> (Segment, Vec<(String, String)>) {
        let mut segment = Segment::new(path);

        let mut tree = tree.iter().collect::<Vec<_>>();
        tree.sort_by(|a, b| a.0.cmp(b.0));

        let mut tokens: Vec<(String, String)> = Vec::new();

        for (key, value) in tree.iter() {
            let token = segment.write(&(*key).clone(), (*value).clone());
            tokens.push(token);
        }

        (segment, tokens)
    }
}
