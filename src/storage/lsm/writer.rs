use std::fs::File;
use std::io::Write;

pub const SEGMENT_SEPARATOR: u8 = 0xAC;

pub trait Writer {
    fn file(&self) -> File;
    fn file_index(&self) -> usize;

    /// # Returns
    /// - `Ok(String)` if the write was successful returns the offset
    /// - `Err(())` if the write failed
    fn persist(&mut self, key: &str, value: bson::Bson) -> Result<String, String> {
        let temp_doc = bson::doc! {
            "_key": key,
            "_value": value,
        };

        let mut file = self.file();

        let offset = file.metadata().unwrap().len();

        let mut bytes = bson::to_vec(&temp_doc).unwrap();
        bytes.push(SEGMENT_SEPARATOR);

        file.write_all(&bytes).unwrap();

        Ok(format!("{}_{}", self.file_index(), offset))
    }
}
