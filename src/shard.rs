use serde::{Deserialize, Serialize};

use crate::bloom::BloomFilter;
use crate::bucket::Bucket;
use bitvec::prelude::*;

#[derive(Serialize, Deserialize, Clone)]
pub struct Shard {
    bucket: Vec<Bucket>,
    bloom_size: usize,
    bloom_k: u64,
}

impl Shard {
    pub fn new(bloom_size: usize, bloom_k: u64) -> Self {
        Self {
            bucket: vec![],
            bloom_size,
            bloom_k,
        }
    }

    pub fn get_m(&self) -> usize {
        return self.bloom_size;
    }
    pub fn get_k(&self) -> u64 {
        return self.bloom_k;
    }

    pub fn add_message(&mut self, trigrams: &[String], key: usize) {
        self.get_bucket().add_message(trigrams, key)
    }

    #[inline(always)]
    pub fn search(&self, trigrams: &[String]) -> Vec<usize> {
        let query_bits = self.get_query_bits(trigrams);
        return self
            .bucket
            .iter()
            .map(|b| b.search(&query_bits))
            .flatten()
            .collect();
    }

    #[inline(always)]
    pub fn search_or(&self, trigrams: &[String]) -> Vec<usize> {
        let query_bits = self.get_query_bits(trigrams);
        return self
            .bucket
            .iter()
            .map(|b| b.search_or(&query_bits))
            .flatten()
            .collect();
    }

    #[inline(always)]
    fn get_query_bits(&self, trigrams: &[String]) -> BitVec {
        let mut bloom_filter = BloomFilter::new(self.bloom_size * 128, self.bloom_k);
        trigrams.iter().for_each(|t| bloom_filter.add(t));
        Self::get_set_bits(bloom_filter.get_bitset())
    }

    #[inline(always)]
    fn get_bucket(&mut self) -> &mut Bucket {
        if self.bucket.is_empty() || self.bucket.last().unwrap().is_full() {
            self.bucket.push(Bucket::new(self.bloom_size, self.bloom_k));
        }
        self.bucket.last_mut().unwrap()
    }

    // #[inline(always)]
    // fn get_set_bits(bits: &Vec<u128>) -> Vec<u128> {
    //     let mut set_bits = Vec::new();
    //     for (i, &bit) in bits.iter().enumerate() {
    //         for j in 0..128 {
    //             if bit & (1 << j) != 0 {
    //                 set_bits.push((i as u128) * 128 + j as u128);
    //             }
    //         }
    //     }
    //     set_bits
    // }
    #[inline(always)]
    fn get_set_bits(bits: &BitVec) -> BitVec {
        let mut result = bitvec![0; bits.len()];
        for (index, bit) in bits.iter().enumerate() {
            result.set(index, *bit);
        }
        result
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     // Helper function to create a Vec<u128> with set bits at given positions.
//     fn create_bit_vector(set_bits: &[usize]) -> Vec<u128> {
//         let mut bit_vec = vec![0u128; (set_bits.iter().max().unwrap_or(&0) + 127) / 128];
//         for &bit in set_bits {
//             let idx = bit / 128;
//             let bit_idx = bit % 128;
//             bit_vec[idx] |= 1u128 << bit_idx;
//         }
//         bit_vec
//     }

//     #[test]
//     fn test_get_set_bits_empty() {
//         let bits = vec![0u128; 1];
//         assert!(Shard::get_set_bits(&bits).is_empty());
//     }

//     #[test]
//     fn test_get_set_bits_single() {
//         let bits = create_bit_vector(&[0]);
//         assert_eq!(Shard::get_set_bits(&bits), vec![0u128]);
//     }

//     #[test]
//     fn test_get_set_bits_multiple() {
//         let bits = create_bit_vector(&[0, 1, 2, 128, 129, 130]);
//         assert_eq!(Shard::get_set_bits(&bits), vec![0u128, 1, 2, 128, 129, 130]);
//     }

//     #[test]
//     fn test_get_set_bits_sparse() {
//         let bits = create_bit_vector(&[0, 63, 64, 127, 128, 191, 192, 255]);
//         assert_eq!(
//             Shard::get_set_bits(&bits),
//             vec![0u128, 63, 64, 127, 128, 191, 192, 255]
//         );
//     }

//     #[test]
//     fn test_get_set_bits_high_index() {
//         let bits = create_bit_vector(&[1024, 2048, 3072]);
//         assert_eq!(Shard::get_set_bits(&bits), vec![1024u128, 2048, 3072]);
//     }
// }

// // Rest of the Shard implementation...
