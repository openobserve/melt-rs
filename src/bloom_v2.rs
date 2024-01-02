use bitvec::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct BloomFilter {
    bitset: BitVec,
    num_bits: usize,
    hashes: u64,
}

impl BloomFilter {
    pub fn new(num_bits: usize, hashes: u64) -> Self {
        Self {
            bitset: bitvec![0; num_bits],
            num_bits,
            hashes,
        }
    }

    pub fn add<T: Hash>(&mut self, elem: &T) {
        for i in 0..self.hashes {
            let mut hasher = DefaultHasher::new();
            elem.hash(&mut hasher);
            hasher.write_usize(i as usize);
            let hash = hasher.finish();

            let idx = (hash as usize) % self.num_bits;
            self.bitset.set(idx, true);
        }
    }

    pub fn get_bitset(&self) -> &BitVec {
        &self.bitset
    }
}

pub fn estimate_parameters(n: usize, p: f64) -> (usize, u64) {
    let m = ((-1.0 * n as f64 * p.ln()) / (2.0_f64.ln().powi(2))).ceil() as u128;
    let k = ((2.0_f64.ln() * m as f64) / n as f64).ceil() as u64;
    (m as usize, k)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let num_bits = 100;
        let hashes = 5;
        let bf = BloomFilter::new(num_bits, hashes);
        assert_eq!(bf.num_bits, num_bits);
        assert_eq!(bf.hashes, hashes);
        assert_eq!(bf.bitset.len(), num_bits);
    }

    #[test]
    fn test_add() {
        let mut bf = BloomFilter::new(100, 5);
        bf.add(&"hello");
        let bitset = bf.get_bitset();
        assert!(bitset.any());
    }
}
