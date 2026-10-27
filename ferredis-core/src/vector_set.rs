//! Redis Vector set (VADD/VSIM/…).

use bytes::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSet {
    pub dim: usize,
    /// Attributs JSON/texte par élément (VSETATTR / VGETATTR).
    pub attrs: HashMap<Bytes, Bytes>,
    pub vectors: HashMap<Bytes, Vec<f32>>,
    /// Graphe de liens approximatif (VLINKS) — kNN naïf mis à jour à l'insertion.
    pub links: HashMap<Bytes, Vec<Bytes>>,
    pub quantization: Quantization,
    pub m: usize,
    pub ef: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quantization {
    F32,
    Q8,
    Bin,
}

impl Default for Quantization {
    fn default() -> Self {
        Self::F32
    }
}

impl VectorSet {
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            attrs: HashMap::new(),
            vectors: HashMap::new(),
            links: HashMap::new(),
            quantization: Quantization::F32,
            m: 16,
            ef: 200,
        }
    }

    pub fn approx_bytes(&self) -> usize {
        self.vectors
            .iter()
            .map(|(k, v)| k.len() + v.len() * 4)
            .sum::<usize>()
            + self.attrs.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
            + self
                .links
                .iter()
                .map(|(k, v)| k.len() + v.iter().map(|x| x.len()).sum::<usize>())
                .sum::<usize>()
    }

    pub fn add(&mut self, name: Bytes, values: Vec<f32>) -> Result<bool, &'static str> {
        if values.is_empty() {
            return Err("ERR invalid vector dimension");
        }
        if values.len() != self.dim && self.dim != 0 {
            return Err("ERR invalid vector dimension");
        }
        if self.dim == 0 {
            self.dim = values.len();
        }
        let existed = self.vectors.contains_key(&name);
        self.vectors.insert(name.clone(), values);
        self.rebuild_links_for(&name);
        Ok(!existed)
    }

    fn rebuild_links_for(&mut self, name: &Bytes) {
        let Some(query) = self.vectors.get(name).cloned() else {
            return;
        };
        let m = self.m.max(1);
        let mut scores: Vec<(Bytes, f64)> = self
            .vectors
            .iter()
            .filter(|(k, _)| *k != name)
            .map(|(k, v)| (k.clone(), cosine(&query, v)))
            .collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(m);
        self.links
            .insert(name.clone(), scores.into_iter().map(|(k, _)| k).collect());
    }

    pub fn rem(&mut self, name: &[u8]) -> bool {
        let key = Bytes::copy_from_slice(name);
        let removed = self.vectors.remove(name).is_some();
        self.attrs.remove(name);
        self.links.remove(name);
        for links in self.links.values_mut() {
            links.retain(|x| x != &key);
        }
        removed
    }

    pub fn sim(&self, query: &[f32], count: usize) -> Vec<(Bytes, f64)> {
        let mut scores: Vec<_> = self
            .vectors
            .iter()
            .filter_map(|(k, v)| {
                if v.len() != query.len() {
                    return None;
                }
                Some((k.clone(), cosine(query, v)))
            })
            .collect();
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(count.max(1));
        scores
    }

    pub fn card(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_member(&self, name: &[u8]) -> bool {
        self.vectors.contains_key(name)
    }

    pub fn emb(&self, name: &[u8]) -> Option<&[f32]> {
        self.vectors.get(name).map(|v| v.as_slice())
    }

    pub fn set_attr(&mut self, name: Bytes, attr: Bytes) -> bool {
        if !self.vectors.contains_key(&name) {
            return false;
        }
        self.attrs.insert(name, attr);
        true
    }

    pub fn get_attr(&self, name: &[u8]) -> Option<&Bytes> {
        self.attrs.get(name)
    }

    pub fn links_of(&self, name: &[u8]) -> Option<&[Bytes]> {
        self.links.get(name).map(|v| v.as_slice())
    }

    pub fn rand_member(&self, count: isize) -> Vec<Bytes> {
        let keys: Vec<_> = self.vectors.keys().cloned().collect();
        if keys.is_empty() {
            return Vec::new();
        }
        if count >= 0 {
            let n = (count as usize).min(keys.len());
            keys.into_iter().take(n).collect()
        } else {
            let n = (-count) as usize;
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                out.push(keys[i % keys.len()].clone());
            }
            out
        }
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for (&x, &y) in a.iter().zip(b.iter()) {
        let xf = f64::from(x);
        let yf = f64::from(y);
        dot += xf * yf;
        na += xf * xf;
        nb += yf * yf;
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}
