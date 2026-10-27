//! Structures probabilistes Redis 8 : Bloom, Cuckoo, CMS, TopK, t-digest.

use bytes::Bytes;
use std::collections::HashMap;

fn murmur64(data: &[u8], seed: u64) -> u64 {
    // MurmurHash3 64-bit finalizer style (simple, stable).
    let mut h = seed ^ (data.len() as u64).wrapping_mul(0xc6a4a7935bd1e995);
    let mut i = 0;
    while i + 8 <= data.len() {
        let mut k = u64::from_le_bytes(data[i..i + 8].try_into().unwrap());
        k = k.wrapping_mul(0xc6a4a7935bd1e995);
        k ^= k >> 47;
        k = k.wrapping_mul(0xc6a4a7935bd1e995);
        h ^= k;
        h = h.wrapping_mul(0xc6a4a7935bd1e995);
        i += 8;
    }
    let rem = &data[i..];
    let mut k = 0u64;
    for (j, &b) in rem.iter().enumerate() {
        k |= u64::from(b) << (8 * j);
    }
    if !rem.is_empty() {
        k = k.wrapping_mul(0xc6a4a7935bd1e995);
        k ^= k >> 47;
        k = k.wrapping_mul(0xc6a4a7935bd1e995);
        h ^= k;
        h = h.wrapping_mul(0xc6a4a7935bd1e995);
    }
    h ^= h >> 47;
    h = h.wrapping_mul(0xc6a4a7935bd1e995);
    h ^= h >> 47;
    h
}

#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    pub bits: Vec<u8>,
    pub n_hashes: u32,
    pub size: u64, // bits
    pub items: u64,
    pub expansion: u32,
    pub filters: Vec<BloomFilter>, // scaling chain
}

impl BloomFilter {
    pub fn reserve(error_rate: f64, capacity: u64, expansion: u32) -> Self {
        let err = error_rate.clamp(1e-9, 0.5);
        let n = capacity.max(1) as f64;
        let m = (-(n * err.ln()) / (2f64.ln().powi(2))).ceil() as u64;
        let k = ((m as f64 / n) * 2f64.ln()).ceil() as u32;
        let bytes = ((m + 7) / 8) as usize;
        Self {
            bits: vec![0; bytes.max(1)],
            n_hashes: k.max(1),
            size: m.max(8),
            items: 0,
            expansion: expansion.max(1),
            filters: Vec::new(),
        }
    }

    fn set_bit(&mut self, i: u64) {
        let idx = (i % self.size) as usize;
        self.bits[idx / 8] |= 1 << (idx % 8);
    }

    fn get_bit(&self, i: u64) -> bool {
        let idx = (i % self.size) as usize;
        self.bits[idx / 8] & (1 << (idx % 8)) != 0
    }

    fn hashes(&self, item: &[u8]) -> impl Iterator<Item = u64> + '_ {
        let h1 = murmur64(item, 0);
        let h2 = murmur64(item, 0x9e3779b97f4a7c15);
        let k = self.n_hashes;
        (0..k).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)))
    }

    pub fn add(&mut self, item: &[u8]) -> bool {
        if self.exists(item) {
            return false;
        }
        // Scaling : si plein, ajouter un sous-filtre.
        if self.items >= self.size / 8 && self.expansion > 0 {
            let sub = BloomFilter::reserve(0.01, self.size / 8 * u64::from(self.expansion), 0);
            self.filters.push(sub);
        }
        if let Some(f) = self.filters.last_mut() {
            for h in f.hashes(item).collect::<Vec<_>>() {
                f.set_bit(h);
            }
            f.items += 1;
            return true;
        }
        for h in self.hashes(item).collect::<Vec<_>>() {
            self.set_bit(h);
        }
        self.items += 1;
        true
    }

    pub fn exists(&self, item: &[u8]) -> bool {
        let in_main = self.hashes(item).all(|h| self.get_bit(h));
        if in_main {
            return true;
        }
        self.filters.iter().any(|f| f.hashes(item).all(|h| f.get_bit(h)))
    }

    pub fn card(&self) -> u64 {
        self.items + self.filters.iter().map(|f| f.items).sum::<u64>()
    }

    pub fn info_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("Capacity", (self.size / 8).to_string()),
            ("Size", self.bits.len().to_string()),
            ("Number of filters", (1 + self.filters.len()).to_string()),
            ("Number of items inserted", self.card().to_string()),
            ("Expansion rate", self.expansion.to_string()),
        ]
    }

    /// Dump binaire unique (iter=1) pour SCANDUMP/LOADCHUNK.
    pub fn scandump(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.size as u64).to_le_bytes());
        out.extend_from_slice(&(self.n_hashes as u32).to_le_bytes());
        out.extend_from_slice(&(self.items as u64).to_le_bytes());
        out.extend_from_slice(&(self.expansion as u32).to_le_bytes());
        out.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
        out.extend_from_slice(&(self.filters.len() as u32).to_le_bytes());
        for f in &self.filters {
            out.extend_from_slice(&f.scandump());
        }
        out
    }

    pub fn loadchunk(data: &[u8]) -> Option<Self> {
        let mut i = 0;
        fn take<'a>(data: &'a [u8], i: &mut usize, n: usize) -> Option<&'a [u8]> {
            if *i + n > data.len() {
                return None;
            }
            let s = &data[*i..*i + n];
            *i += n;
            Some(s)
        }
        let size = u64::from_le_bytes(take(data, &mut i, 8)?.try_into().ok()?);
        let n_hashes = u32::from_le_bytes(take(data, &mut i, 4)?.try_into().ok()?);
        let items = u64::from_le_bytes(take(data, &mut i, 8)?.try_into().ok()?);
        let expansion = u32::from_le_bytes(take(data, &mut i, 4)?.try_into().ok()?);
        let blen = u32::from_le_bytes(take(data, &mut i, 4)?.try_into().ok()?) as usize;
        let bits = take(data, &mut i, blen)?.to_vec();
        let n_filters = u32::from_le_bytes(take(data, &mut i, 4)?.try_into().ok()?) as usize;
        let mut filters = Vec::with_capacity(n_filters);
        for _ in 0..n_filters {
            let rest = &data[i..];
            let f = BloomFilter::loadchunk(rest)?;
            // advance by re-encoding length (scandump of child)
            let enc = f.scandump();
            i += enc.len();
            filters.push(f);
        }
        Some(Self {
            bits,
            n_hashes,
            size,
            items,
            expansion,
            filters,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CuckooFilter {
    pub buckets: Vec<u16>, // 2 fingerprints per bucket packed
    pub bucket_size: usize,
    pub max_kicks: u32,
    pub items: u64,
    pub num_buckets: usize,
}

impl CuckooFilter {
    pub fn reserve(capacity: u64, bucket_size: usize, max_kicks: u32) -> Self {
        let bs = bucket_size.max(2);
        let num_buckets = ((capacity as usize / bs).next_power_of_two()).max(2);
        Self {
            buckets: vec![0; num_buckets * bs],
            bucket_size: bs,
            max_kicks: max_kicks.max(1),
            items: 0,
            num_buckets,
        }
    }

    fn fingerprint(item: &[u8]) -> u16 {
        let h = murmur64(item, 1) as u16;
        if h == 0 {
            1
        } else {
            h
        }
    }

    fn indices(item: &[u8], fp: u16, n: usize) -> (usize, usize) {
        let h = murmur64(item, 2) as usize % n;
        let alt = (h ^ (fp as usize).wrapping_mul(0x5bd1e995)) % n;
        (h, alt)
    }

    fn bucket_has(&self, b: usize, fp: u16) -> bool {
        let base = b * self.bucket_size;
        self.buckets[base..base + self.bucket_size]
            .iter()
            .any(|&x| x == fp)
    }

    fn bucket_insert(&mut self, b: usize, fp: u16) -> bool {
        let base = b * self.bucket_size;
        for slot in self.buckets[base..base + self.bucket_size].iter_mut() {
            if *slot == 0 {
                *slot = fp;
                return true;
            }
        }
        false
    }

    pub fn add(&mut self, item: &[u8]) -> bool {
        if self.exists(item) {
            return false;
        }
        let mut fp = Self::fingerprint(item);
        let (i1, i2) = Self::indices(item, fp, self.num_buckets);
        if self.bucket_insert(i1, fp) || self.bucket_insert(i2, fp) {
            self.items += 1;
            return true;
        }
        let mut i = if murmur64(item, 3) & 1 == 0 { i1 } else { i2 };
        for _ in 0..self.max_kicks {
            let base = i * self.bucket_size;
            let slot = (murmur64(&[fp as u8], 4) as usize) % self.bucket_size;
            std::mem::swap(&mut self.buckets[base + slot], &mut fp);
            i = (i ^ (fp as usize).wrapping_mul(0x5bd1e995)) % self.num_buckets;
            if self.bucket_insert(i, fp) {
                self.items += 1;
                return true;
            }
        }
        false
    }

    pub fn exists(&self, item: &[u8]) -> bool {
        let fp = Self::fingerprint(item);
        let (i1, i2) = Self::indices(item, fp, self.num_buckets);
        self.bucket_has(i1, fp) || self.bucket_has(i2, fp)
    }

    pub fn del(&mut self, item: &[u8]) -> bool {
        let fp = Self::fingerprint(item);
        let (i1, i2) = Self::indices(item, fp, self.num_buckets);
        for b in [i1, i2] {
            let base = b * self.bucket_size;
            for slot in self.buckets[base..base + self.bucket_size].iter_mut() {
                if *slot == fp {
                    *slot = 0;
                    self.items = self.items.saturating_sub(1);
                    return true;
                }
            }
        }
        false
    }

    pub fn count(&self, item: &[u8]) -> u64 {
        let fp = Self::fingerprint(item);
        let (i1, i2) = Self::indices(item, fp, self.num_buckets);
        let mut n = 0u64;
        for b in [i1, i2] {
            let base = b * self.bucket_size;
            for &slot in &self.buckets[base..base + self.bucket_size] {
                if slot == fp {
                    n += 1;
                }
            }
        }
        n
    }

    pub fn info_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("Size", self.buckets.len().to_string()),
            ("Number of buckets", self.num_buckets.to_string()),
            ("Number of filter buckets", self.bucket_size.to_string()),
            ("Number of items inserted", self.items.to_string()),
            ("Number of items deleted", "0".into()),
            ("Bucket size", self.bucket_size.to_string()),
            ("Max iterations", self.max_kicks.to_string()),
            ("Expansion rate", "1".into()),
        ]
    }

    pub fn scandump(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.num_buckets as u32).to_le_bytes());
        out.extend_from_slice(&(self.bucket_size as u32).to_le_bytes());
        out.extend_from_slice(&(self.max_kicks as u32).to_le_bytes());
        out.extend_from_slice(&(self.items as u64).to_le_bytes());
        out.extend_from_slice(&(self.buckets.len() as u32).to_le_bytes());
        for &b in &self.buckets {
            out.extend_from_slice(&b.to_le_bytes());
        }
        out
    }

    pub fn loadchunk(data: &[u8]) -> Option<Self> {
        if data.len() < 20 {
            return None;
        }
        let num_buckets = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let bucket_size = u32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
        let max_kicks = u32::from_le_bytes(data[8..12].try_into().ok()?);
        let items = u64::from_le_bytes(data[12..20].try_into().ok()?);
        let blen = u32::from_le_bytes(data[20..24].try_into().ok()?) as usize;
        if data.len() < 24 + blen * 2 {
            return None;
        }
        let mut buckets = Vec::with_capacity(blen);
        for i in 0..blen {
            let o = 24 + i * 2;
            buckets.push(u16::from_le_bytes(data[o..o + 2].try_into().ok()?));
        }
        Some(Self {
            buckets,
            bucket_size,
            max_kicks,
            items,
            num_buckets,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CountMinSketch {
    pub width: usize,
    pub depth: usize,
    pub counters: Vec<u64>,
}

impl CountMinSketch {
    pub fn init_by_dim(width: usize, depth: usize) -> Self {
        let w = width.max(1);
        let d = depth.max(1);
        Self {
            width: w,
            depth: d,
            counters: vec![0; w * d],
        }
    }

    pub fn init_by_prob(error: f64, prob: f64) -> Self {
        let w = ((2.71828 / error).ceil() as usize).max(1);
        let d = (((-(prob.ln())) / (2.71828_f64).ln()).ceil() as usize).max(1);
        Self::init_by_dim(w, d)
    }

    pub fn incrby(&mut self, item: &[u8], delta: u64) {
        for d in 0..self.depth {
            let h = murmur64(item, d as u64 + 10) as usize % self.width;
            self.counters[d * self.width + h] =
                self.counters[d * self.width + h].saturating_add(delta);
        }
    }

    pub fn query(&self, item: &[u8]) -> u64 {
        let mut min = u64::MAX;
        for d in 0..self.depth {
            let h = murmur64(item, d as u64 + 10) as usize % self.width;
            min = min.min(self.counters[d * self.width + h]);
        }
        if min == u64::MAX {
            0
        } else {
            min
        }
    }

    pub fn merge(&mut self, others: &[&CountMinSketch], weights: &[u64]) -> Result<(), &'static str> {
        for o in others {
            if o.width != self.width || o.depth != self.depth {
                return Err("ERR width/depth mismatch");
            }
        }
        for (idx, o) in others.iter().enumerate() {
            let w = weights.get(idx).copied().unwrap_or(1).max(1);
            for (i, c) in o.counters.iter().enumerate() {
                self.counters[i] = self.counters[i].saturating_add(c.saturating_mul(w));
            }
        }
        Ok(())
    }

    pub fn info_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("width", self.width.to_string()),
            ("depth", self.depth.to_string()),
            ("count", self.counters.iter().sum::<u64>().to_string()),
        ]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopK {
    pub k: usize,
    pub width: usize,
    pub depth: usize,
    pub decay: f64,
    pub heap: HashMap<Bytes, u64>,
    pub cms: CountMinSketch,
}

impl TopK {
    pub fn reserve(k: usize, width: usize, depth: usize, decay: f64) -> Self {
        Self {
            k: k.max(1),
            width: width.max(1),
            depth: depth.max(1),
            decay: if decay <= 0.0 { 0.9 } else { decay },
            heap: HashMap::new(),
            cms: CountMinSketch::init_by_dim(width.max(1), depth.max(1)),
        }
    }

    pub fn add(&mut self, item: Bytes) -> Option<Bytes> {
        self.incrby(item, 1)
    }

    pub fn incrby(&mut self, item: Bytes, delta: u64) -> Option<Bytes> {
        let d = delta.max(1);
        self.cms.incrby(&item, d);
        let count = self.cms.query(&item);
        if self.heap.contains_key(&item) {
            self.heap.insert(item, count);
            return None;
        }
        if self.heap.len() < self.k {
            self.heap.insert(item, count);
            return None;
        }
        let (min_key, min_val) = self
            .heap
            .iter()
            .min_by_key(|(_, v)| *v)
            .map(|(k, v)| (k.clone(), *v))
            .unwrap();
        if count > min_val {
            self.heap.remove(&min_key);
            self.heap.insert(item, count);
            Some(min_key)
        } else {
            None
        }
    }

    pub fn query(&self, item: &[u8]) -> bool {
        self.heap.keys().any(|k| k.as_ref() == item)
    }

    pub fn count(&self, item: &[u8]) -> u64 {
        self.cms.query(item)
    }

    pub fn list(&self) -> Vec<(Bytes, u64)> {
        let mut v: Vec<_> = self.heap.iter().map(|(k, v)| (k.clone(), *v)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v
    }

    pub fn info_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("k", self.k.to_string()),
            ("width", self.width.to_string()),
            ("depth", self.depth.to_string()),
            ("decay", format!("{}", self.decay)),
        ]
    }
}

/// t-digest approximatif (centroids triés).
#[derive(Debug, Clone, PartialEq)]
pub struct TDigest {
    pub compression: f64,
    pub centroids: Vec<(f64, f64)>, // mean, weight
    pub min: f64,
    pub max: f64,
    pub count: f64,
}

impl TDigest {
    pub fn create(compression: f64) -> Self {
        Self {
            compression: compression.max(10.0),
            centroids: Vec::new(),
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            count: 0.0,
        }
    }

    pub fn add(&mut self, values: &[f64]) {
        for &x in values {
            if !x.is_finite() {
                continue;
            }
            self.min = self.min.min(x);
            self.max = self.max.max(x);
            self.centroids.push((x, 1.0));
            self.count += 1.0;
            if self.centroids.len() > self.compression as usize * 2 {
                self.compress();
            }
        }
    }

    fn compress(&mut self) {
        if self.centroids.is_empty() {
            return;
        }
        self.centroids
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let mut merged = Vec::new();
        let mut cur = self.centroids[0];
        for &(m, w) in self.centroids.iter().skip(1) {
            if cur.1 + w <= self.count / self.compression {
                let tw = cur.1 + w;
                cur = ((cur.0 * cur.1 + m * w) / tw, tw);
            } else {
                merged.push(cur);
                cur = (m, w);
            }
        }
        merged.push(cur);
        self.centroids = merged;
    }

    pub fn quantile(&mut self, q: f64) -> f64 {
        self.compress();
        if self.centroids.is_empty() {
            return f64::NAN;
        }
        let q = q.clamp(0.0, 1.0);
        let target = q * self.count;
        let mut cum = 0.0;
        for &(m, w) in &self.centroids {
            cum += w;
            if cum >= target {
                return m;
            }
        }
        self.max
    }

    pub fn cdf(&mut self, x: f64) -> f64 {
        self.compress();
        if self.count == 0.0 {
            return f64::NAN;
        }
        let mut cum = 0.0;
        for &(m, w) in &self.centroids {
            if m > x {
                break;
            }
            cum += w;
        }
        cum / self.count
    }

    pub fn reset(&mut self) {
        self.centroids.clear();
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
        self.count = 0.0;
    }

    pub fn merge(&mut self, others: &[&TDigest]) {
        for o in others {
            self.compression = self.compression.max(o.compression);
            for &(m, w) in &o.centroids {
                self.centroids.push((m, w));
                self.count += w;
                self.min = self.min.min(m);
                self.max = self.max.max(m);
            }
            if o.min.is_finite() {
                self.min = self.min.min(o.min);
            }
            if o.max.is_finite() {
                self.max = self.max.max(o.max);
            }
        }
        self.compress();
    }

    pub fn trimmed_mean(&mut self, lo: f64, hi: f64) -> f64 {
        self.compress();
        if self.count == 0.0 || lo >= hi {
            return f64::NAN;
        }
        let lo_t = lo.clamp(0.0, 1.0) * self.count;
        let hi_t = hi.clamp(0.0, 1.0) * self.count;
        let mut cum = 0.0;
        let mut sum = 0.0;
        let mut wsum = 0.0;
        for &(m, w) in &self.centroids {
            let next = cum + w;
            if next > lo_t && cum < hi_t {
                let take = (next.min(hi_t) - cum.max(lo_t)).max(0.0);
                sum += m * take;
                wsum += take;
            }
            cum = next;
            if cum >= hi_t {
                break;
            }
        }
        if wsum == 0.0 {
            f64::NAN
        } else {
            sum / wsum
        }
    }

    pub fn rank(&mut self, x: f64) -> f64 {
        self.cdf(x) * self.count
    }

    pub fn revrank(&mut self, x: f64) -> f64 {
        self.count - self.rank(x)
    }

    pub fn byrank(&mut self, r: f64) -> f64 {
        if self.count == 0.0 {
            return f64::NAN;
        }
        self.quantile((r / self.count).clamp(0.0, 1.0))
    }

    pub fn byrevrank(&mut self, r: f64) -> f64 {
        if self.count == 0.0 {
            return f64::NAN;
        }
        self.quantile(1.0 - (r / self.count).clamp(0.0, 1.0))
    }

    pub fn info_pairs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("Compression", format!("{}", self.compression)),
            ("Capacity", (self.compression as usize * 2).to_string()),
            ("Merged nodes", self.centroids.len().to_string()),
            ("Unmerged nodes", "0".into()),
            ("Weight", format!("{}", self.count)),
            ("Min", format!("{}", self.min)),
            ("Max", format!("{}", self.max)),
        ]
    }
}
