use ahash::AHashMap;
use std::option::Option;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interner {
    map: AHashMap<String, u64>,
    vec: Vec<String>,
}

impl Interner {
    pub fn new() -> Self {
        Interner {
            map: AHashMap::new(),
            vec: Vec::new(),
        }
    }

    pub fn get(&self, s: &str) -> Option<u64> {
        self.map.get(s).cloned()
    }

    pub fn ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.vec.iter().enumerate().map(|(i, _)| i as u64)
    }

    pub fn intern(&mut self, s: &str) -> u64 {
        if let Some(&id) = self.map.get(s) {
            id
        } else {
            let id = self.vec.len() as u64;
            self.map.insert(s.to_owned(), id);
            self.vec.push(s.to_owned());
            id
        }
    }

    pub fn resolve(&self, id: u64) -> Option<&String> {
        self.vec.get(id as usize)
    }
}