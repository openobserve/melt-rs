use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::{Error, Read};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::bigrams::bigram;
use crate::bloom::estimate_parameters;
use crate::shard::Shard;
use crate::trigrams::trigram;

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct SearchIndex {
    shards: Vec<Shard>,
    size: usize,
    prob: f64,
}

impl SearchIndex {
    pub fn default() -> SearchIndex {
        SearchIndex {
            shards: vec![],
            size: 0,
            prob: 0.6,
        }
    }
    pub fn default_with_prob(prob: f64) -> SearchIndex {
        SearchIndex {
            shards: vec![],
            size: 0,
            prob,
        }
    }
    pub fn clear(&mut self) {
        self.size = 0;
        self.shards.clear();
    }

    pub fn add(&mut self, item: &str) -> usize {
        let grams = grams(item);
        let (m, k) = estimate_parameters(grams.len(), self.prob);
        match self
            .shards
            .iter_mut()
            .find(|s| s.get_m() == m && s.get_k() == k)
        {
            None => {
                let mut shard = Shard::new(m, k);
                shard.add_message(&grams, self.size);
                self.shards.push(shard);
            }
            Some(shard) => shard.add_message(&grams, self.size),
        };
        self.size += 1;
        self.size - 1
    }

    pub fn search(&self, query: &str, exact: bool) -> Vec<usize> {
        if query.is_empty() {
            if self.size == 0 {
                return vec![];
            }
            return (0..=self.size - 1).collect::<Vec<usize>>();
        }
        let trigrams = if exact {
            grams(query)
        } else {
            query.split(" ").flat_map(|q| grams(q)).collect()
        };
        if trigrams.is_empty() {
            return vec![];
        }
        let results: Vec<_> = self
            .shards
            .par_iter()
            .flat_map(|shard| shard.search(&trigrams))
            .collect();
        results
    }

    pub fn search_or(&self, query: &str) -> Vec<usize> {
        let grams = query
            .split(" ")
            .flat_map(|q| grams(q))
            .collect::<Vec<String>>();
        if grams.is_empty() {
            return vec![];
        }
        let results: Vec<_> = self
            .shards
            .par_iter()
            .flat_map(|shard| shard.search_or(&grams))
            .collect();
        results
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn get_prob(&self) -> f64 {
        self.prob
    }

    pub fn get_size_bytes(&self) -> usize {
        let serialized: Vec<u8> = bincode::serialize(&self.shards).unwrap();
        serialized.len()
    }
}

fn get_file_as_byte_vec(filename: &str) -> Result<Vec<u8>, Error> {
    let mut f = File::open(&filename)?;
    let metadata = fs::metadata(&filename)?;
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer)?;

    Ok(buffer)
}

fn grams(query: &str) -> Vec<String> {
    let query = query.to_lowercase();
    let mut vec = trigram(&query);
    vec.extend(bigram(&query));
    vec.extend(
        query
            .chars()
            .map(|c| c.to_string())
            .collect::<HashSet<String>>(),
    );
    vec
}

#[test]
fn test_search_non_case_sens() {
    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let i = index.add(item);
    let string = "hello".to_string();
    let vec = index.search(string.as_str(), true);
    let res = vec.first().unwrap();
    assert_eq!(*res, i as usize);

    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let i = index.add(item);
    let string = "Hello".to_string();
    let vec = index.search(string.as_str(), true);
    let res = vec.first().unwrap();
    assert_eq!(*res, i as usize);

    let item = "Hello, wor杯ld!";
    index.add(item);
    let string = "He3llo".to_string();
    let vec = index.search(string.as_str(), true);
    let res = vec.first().unwrap_or(&(0 as usize));
    assert_eq!(*res, 0 as usize);
}

#[test]
fn test_search_not_exact() {
    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let _ = index.add(item);
    let string = "hello wor".to_string();
    let vec = index.search(string.as_str(), true);
    assert_eq!(0, vec.len());

    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let _ = index.add(item);
    let string = "hello wor".to_string();
    let vec = index.search(string.as_str(), false);
    assert_eq!(1, vec.len());
}

#[test]
fn test_search_or() {
    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let _ = index.add(item);
    let string = "hello there".to_string();
    let vec = index.search_or(string.as_str());
    assert_eq!(1, vec.len());
}

#[test]
fn test_search_just_get_length_of_index() {
    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let _ = index.add(item);
    assert_eq!(1, index.size);
}

#[test]
fn test_search_just_get_length_of_index_multiple_objects() {
    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let _ = index.add(item);
    let item = "Hello, wor杯ld!2";
    let _ = index.add(item);
    assert_eq!(2, index.size);
}

#[test]
fn test_search_just_return_both_elements_when_black_search() {
    let mut index = SearchIndex::default();

    let item = "Hello, wor杯ld!";
    let first = index.add(item);

    let vec = index.search("", true);
    assert_eq!(0, *vec.first().unwrap());
    assert_eq!(first, *vec.first().unwrap());

    let item2 = "Hello, wor杯ld!";
    let second = index.add(item2);

    let vec = index.search("", true);
    assert_eq!(1, *vec.last().unwrap());
    assert_eq!(second, *vec.last().unwrap());
}

#[test]
fn test_search_with_0_index() {
    let index = SearchIndex::default();
    let string = "hello there".to_string();
    let vec = index.search_or(string.as_str());
    assert_eq!(0, vec.len());
}
