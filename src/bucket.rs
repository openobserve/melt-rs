use crate::bloom::BloomFilter;
use bitvec::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Bucket {
    messages: Vec<usize>,
    bloom_filter: BitVec,
    bloom_count: u8,
    bloom_size: usize,
    bloom_k: u64,
}

impl Bucket {
    pub fn new(bloom_size: usize, bloom_k: u64) -> Self {
        Self {
            messages: vec![0; 128],
            bloom_filter: bitvec![0; bloom_size * 128],
            bloom_count: 0,
            bloom_size,
            bloom_k,
        }
    }

    pub fn add_message_v2(&mut self, trigrams: &[String], key: usize) {
        let mut bloom_filter = BloomFilter::new(self.bloom_size * 128, self.bloom_k);

        trigrams.iter().for_each(|v| bloom_filter.add(v));
        self.add_bloom(bloom_filter.get_bitset());
        self.messages[(self.bloom_count - 1) as usize] = key;
    }

    pub fn add_message(&mut self, trigrams: &[String], key: usize) {
        let mut bloom_filter = BloomFilter::new(self.bloom_size * 128, self.bloom_k);
        trigrams.iter().for_each(|v| bloom_filter.add(v));
        self.add_bloom(bloom_filter.get_bitset());
        self.messages[(self.bloom_count - 1) as usize] = key;
    }
    // // add current document to the bloom index
    // fn add_bloom(&mut self, vec: &[u128]) {
    //     for i in 0..self.bloom_size * 128 as usize {
    //         if vec[i / 128] & (1 << (i % 128)) != 0 {
    //             self.bloom_filter[i] |= 1u128 << (self.bloom_count);
    //         }
    //     }
    //     self.bloom_count += 1
    // }
    fn add_bloom(&mut self, bitvec: &BitVec) {
        for i in 0..self.bloom_size * 128 {
            if bitvec[i] {
                self.bloom_filter.set(
                    i + (self.bloom_count as usize * self.bloom_size * 128),
                    true,
                );
            }
        }
        self.bloom_count += 1
    }

    pub fn is_full(&self) -> bool {
        self.bloom_count == 128
    }

    #[inline(always)]
    pub fn search(&self, query_bits: &BitVec) -> Vec<usize> {
        let mut results = Vec::new();

        for segment_start in (0..self.bloom_filter.len()).step_by(self.bloom_size * 128) {
            let mut match_found = true;

            for index in 0..self.bloom_size * 128 {
                if query_bits[index] && !self.bloom_filter[segment_start + index] {
                    match_found = false;
                    break;
                }
            }

            if match_found {
                results.push(self.messages[(segment_start / (self.bloom_size * 128)) as usize]);
            }
        }

        results
    }

    #[inline(always)]
    pub fn search_or(&self, query_bits: &BitVec) -> Vec<usize> {
        let mut results = Vec::new();

        for segment_start in (0..self.bloom_filter.len()).step_by(self.bloom_size * 128) {
            let mut match_found = false;

            for index in 0..self.bloom_size * 128 {
                if query_bits[index] && self.bloom_filter[segment_start + index] {
                    match_found = true;
                    break;
                }
            }

            if match_found {
                results.push(self.messages[(segment_start / (self.bloom_size * 128)) as usize]);
            }
        }

        results
    }
}
