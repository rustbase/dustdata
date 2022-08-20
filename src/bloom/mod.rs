// https://www.youtube.com/watch?v=uoITg1iUj7A
// Bloom filter: https://en.wikipedia.org/wiki/Bloom_filter

fn num_bits(size: i32, fp_rate: f64) -> i32 {
    let num = -1.0f64 * size as f64 * fp_rate.ln();
    let den = 2.0f64.ln().powf(2.0);
    (num / den).ceil() as i32
}

fn num_hashes(m: i32, n: i32) -> i32 {
    ((m as f64 / n as f64) * 2.0f64).ceil() as i32
}

pub struct BloomFilter {
    pub bitvec: Vec<u8>,
    pub hashes: i32,
}

impl BloomFilter {
    pub fn new(fp_rate: f64, size: i32) -> Self {
        let m = num_bits(size, fp_rate);
        let k = num_hashes(m, size);

        BloomFilter {
            bitvec: vec![0; m.try_into().unwrap()],
            hashes: k,
        }
    }

    pub fn insert(&mut self, value: &str) {
        for i in 0..self.hashes {
            let index = fasthash::murmur3::hash32_with_seed(value, i as u32)
                % (self.bitvec.len() as u32 * 8);
            let pos = index as usize;
            self.bitvec[pos / 8] |= 1 << (pos % 8);
        }
    }

    pub fn contains(&self, value: &str) -> bool {
        for i in 0..self.hashes {
            let index = fasthash::murmur3::hash32_with_seed(value, i as u32)
                % (self.bitvec.len() as u32 * 8);
            let pos = index as usize;
            if (1 << (pos % 8)) & self.bitvec[pos / 8] == 0 {
                return false;
            }
        }

        true
    }

    pub fn clear(&mut self) {
        self.bitvec = vec![0; self.bitvec.len()];
    }

    pub fn delete(&mut self, value: &str) {
        for i in 0..self.hashes {
            let index = fasthash::murmur3::hash32_with_seed(value, i as u32)
                % (self.bitvec.len() as u32 * 8);
            let pos = index as usize;
            self.bitvec[pos / 8] &= !(1 << (pos % 8));
        }
    }

    pub fn get_bitvec(&self) -> &Vec<u8> {
        &self.bitvec
    }

    pub fn get_hashes(&self) -> i32 {
        self.hashes
    }
}
