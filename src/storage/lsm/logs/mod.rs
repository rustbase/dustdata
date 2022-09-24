use bson::doc;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const SEGMENT_SEPARATOR: u8 = 0xAC;

pub enum Method {
    Insert(String, bson::Bson),
    Delete(String),
    Update(String, bson::Bson),
}

pub fn get_index(path: PathBuf) -> i32 {
    let mut index = -1;
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let file_index = file_name.split('_').next().unwrap().parse::<i32>().unwrap();
            if file_index > index {
                index = file_index;
            }
        }
    }
    index + 1
}

pub fn find_file_by_index(path: PathBuf, index: i32) -> Option<PathBuf> {
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            let file_name = path.file_name().unwrap().to_str().unwrap();

            if file_name.starts_with(index.to_string().as_str()) {
                return Some(path);
            }
        }
    }

    None
}

pub struct Logs {
    file: fs::File,
    path: PathBuf,
}

impl Logs {
    pub fn new(path: String) -> Self {
        let folder = Path::new(&path).join("logs");

        if !folder.exists() {
            fs::create_dir_all(folder.clone()).unwrap();
        }

        let index = get_index(folder.clone());
        let path = folder.join(format!("{}_log", index));

        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .unwrap();

        Self { file, path: folder }
    }

    pub fn write(&mut self, method: Method) {
        let doc = match method {
            Method::Insert(key, value) => {
                let doc = doc! {
                    "method": "insert",
                    "key": key,
                    "value": value,
                };

                doc
            }
            Method::Delete(key) => {
                let doc = doc! {
                    "method": "delete",
                    "key": key,
                };

                doc
            }
            Method::Update(key, value) => {
                let doc = doc! {
                    "method": "update",
                    "key": key,
                    "value": value,
                };

                doc
            }
        };

        let mut bytes = bson::to_vec(&doc).unwrap();
        bytes.push(SEGMENT_SEPARATOR);

        self.file.write_all(&bytes).unwrap();
    }

    pub fn read(&mut self, log_index: Option<i32>) -> Vec<Method> {
        let mut bytes = Vec::new();

        if let Some(log_index) = log_index {
            let path = find_file_by_index(self.path.clone(), log_index).unwrap();

            let mut file = fs::OpenOptions::new().read(true).open(path).unwrap();
            file.read_to_end(&mut bytes).unwrap();
        } else {
            self.file.read_to_end(&mut bytes).unwrap();
        }

        let mut index = 0;
        let mut segments = Vec::new();
        for (i, byte) in bytes.iter().enumerate() {
            if *byte == SEGMENT_SEPARATOR {
                segments.push(bytes[index..i].to_vec());
                index = i + 1;
            }
        }

        let mut methods = Vec::new();

        for segment in segments {
            let doc: bson::Document = bson::from_slice(&segment).unwrap();

            let method = doc.get_str("method").unwrap();
            let key = doc.get_str("key").unwrap().to_string();
            let value = doc.get("value").unwrap().clone();

            let method = match method {
                "insert" => Method::Insert(key, value),
                "delete" => Method::Delete(key),
                "update" => Method::Update(key, value),

                _ => panic!("Invalid method"),
            };

            methods.push(method);
        }

        methods
    }
}
