//! Redis value types stored in memory.

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Score ordonné d'un sorted set (jamais NaN, `-0.0` normalisé en `+0.0`).
#[derive(Debug, Clone, Copy)]
pub struct Score(pub f64);

impl Score {
    #[inline]
    pub fn new(v: f64) -> Self {
        // `v + 0.0` transforme -0.0 en +0.0 ; l'ordre total gère le reste.
        Self(v + 0.0)
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == Ordering::Equal
    }
}
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Borne d'un intervalle de score (`(1.5` = exclusive, `-inf`, `+inf`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreBound {
    pub value: f64,
    pub exclusive: bool,
}

impl ScoreBound {
    pub fn parse(raw: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(raw).ok()?;
        let (exclusive, body) = match s.strip_prefix('(') {
            Some(rest) => (true, rest),
            None => (false, s),
        };
        let value = parse_score(body)?;
        Some(Self { value, exclusive })
    }
}

/// Parse un score Redis (`inf`, `+inf`, `-inf` acceptés, NaN refusé).
pub fn parse_score(s: &str) -> Option<f64> {
    let v: f64 = s.trim().parse().ok()?;
    if v.is_nan() {
        None
    } else {
        Some(v)
    }
}

/// Borne lexicographique (`[a`, `(a`, `-`, `+`) des commandes `*BYLEX`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    NegInf,
    PosInf,
    Incl(Bytes),
    Excl(Bytes),
}

impl LexBound {
    pub fn parse(raw: &[u8]) -> Option<Self> {
        match raw.first()? {
            b'+' if raw.len() == 1 => Some(Self::PosInf),
            b'-' if raw.len() == 1 => Some(Self::NegInf),
            b'[' => Some(Self::Incl(Bytes::copy_from_slice(&raw[1..]))),
            b'(' => Some(Self::Excl(Bytes::copy_from_slice(&raw[1..]))),
            _ => None,
        }
    }

    fn order_key(&self) -> (u8, &[u8]) {
        match self {
            Self::NegInf => (0, b""),
            Self::Incl(v) | Self::Excl(v) => (1, v),
            Self::PosInf => (2, b""),
        }
    }

    fn admits_min(&self, m: &[u8]) -> bool {
        match self {
            Self::NegInf => true,
            Self::PosInf => false,
            Self::Incl(v) => m >= &v[..],
            Self::Excl(v) => m > &v[..],
        }
    }

    fn admits_max(&self, m: &[u8]) -> bool {
        match self {
            Self::PosInf => true,
            Self::NegInf => false,
            Self::Incl(v) => m <= &v[..],
            Self::Excl(v) => m < &v[..],
        }
    }

    fn btree_bound(&self, score: Score, lower: bool) -> Bound<(Score, Bytes)> {
        match self {
            Self::Incl(v) => Bound::Included((score, v.clone())),
            Self::Excl(v) => Bound::Excluded((score, v.clone())),
            Self::NegInf if lower => Bound::Included((score, Bytes::new())),
            _ => Bound::Unbounded,
        }
    }
}

/// Intervalle d'un sorted set : rangs, scores ou ordre lexicographique.
#[derive(Debug, Clone, PartialEq)]
pub enum ZRange {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

type ZIter<'a> = Box<dyn Iterator<Item = &'a (Score, Bytes)> + 'a>;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZSet {
    /// member -> score
    pub members: HashMap<Bytes, f64>,
    /// Ordre (score, membre) — parcours et intervalles en O(log n + k).
    pub by_score: BTreeSet<(Score, Bytes)>,
}

impl ZSet {
    /// Insère ou met à jour. Retourne `true` si le membre est nouveau.
    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        let score = Score::new(score).0;
        match self.members.insert(member.clone(), score) {
            Some(old) => {
                if Score::new(old) != Score::new(score) {
                    self.by_score.remove(&(Score::new(old), member.clone()));
                    self.by_score.insert((Score::new(score), member));
                }
                false
            }
            None => {
                self.by_score.insert((Score::new(score), member));
                true
            }
        }
    }

    pub fn remove(&mut self, member: &Bytes) -> bool {
        match self.members.remove(member) {
            Some(score) => {
                self.by_score.remove(&(Score::new(score), member.clone()));
                true
            }
            None => false,
        }
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Normalise `[start, stop]` (indices négatifs permis) en `(skip, take)`.
    fn rank_window(&self, start: i64, stop: i64) -> Option<(usize, usize)> {
        let len = self.len() as i64;
        let start = if start < 0 { (len + start).max(0) } else { start };
        let stop = if stop < 0 { len + stop } else { stop.min(len - 1) };
        if len == 0 || stop < 0 || start >= len || start > stop {
            return None;
        }
        Some((start as usize, (stop - start + 1) as usize))
    }

    pub fn range_by_rank(&self, start: i64, stop: i64, rev: bool) -> Vec<(Bytes, f64)> {
        self.range(&ZRange::Rank(start, stop), rev, 0, None)
    }

    /// Parcours ordonné (ou inverse) des éléments couverts par `r`.
    pub fn iter_range<'a>(&'a self, r: &ZRange, rev: bool) -> ZIter<'a> {
        match r {
            ZRange::Rank(start, stop) => {
                let Some((skip, take)) = self.rank_window(*start, *stop) else {
                    return Box::new(std::iter::empty());
                };
                if rev {
                    Box::new(self.by_score.iter().rev().skip(skip).take(take))
                } else {
                    Box::new(self.by_score.iter().skip(skip).take(take))
                }
            }
            ZRange::Score(min, max) => {
                let Some(bounds) = Self::score_bounds(*min, *max) else {
                    return Box::new(std::iter::empty());
                };
                let it = self.by_score.range(bounds);
                if rev {
                    Box::new(it.rev())
                } else {
                    Box::new(it)
                }
            }
            ZRange::Lex(min, max) => self.iter_lex(min, max, rev),
        }
    }

    /// Intervalle lexicographique : O(log n + k) si tous les scores sont égaux
    /// (cas d'usage documenté par Redis), parcours linéaire sinon.
    fn iter_lex<'a>(&'a self, min: &LexBound, max: &LexBound, rev: bool) -> ZIter<'a> {
        let c = min.order_key().cmp(&max.order_key());
        let empty = c == Ordering::Greater
            || (c == Ordering::Equal
                && (matches!(min, LexBound::Excl(_)) || matches!(max, LexBound::Excl(_))));
        let (Some(first), Some(last)) = (self.by_score.first(), self.by_score.last()) else {
            return Box::new(std::iter::empty());
        };
        if empty || matches!(min, LexBound::PosInf) || matches!(max, LexBound::NegInf) {
            return Box::new(std::iter::empty());
        }
        if first.0 == last.0 {
            let s = first.0;
            let it = self
                .by_score
                .range((min.btree_bound(s, true), max.btree_bound(s, false)));
            return if rev { Box::new(it.rev()) } else { Box::new(it) };
        }
        let (min, max) = (min.clone(), max.clone());
        if rev {
            Box::new(
                self.by_score
                    .iter()
                    .rev()
                    .skip_while(move |(_, m)| !max.admits_max(m))
                    .take_while(move |(_, m)| min.admits_min(m)),
            )
        } else {
            Box::new(
                self.by_score
                    .iter()
                    .skip_while(move |(_, m)| !min.admits_min(m))
                    .take_while(move |(_, m)| max.admits_max(m)),
            )
        }
    }

    /// Éléments couverts par `r`, avec `offset`/`count` (LIMIT).
    pub fn range(&self, r: &ZRange, rev: bool, offset: usize, count: Option<usize>) -> Vec<(Bytes, f64)> {
        self.iter_range(r, rev)
            .skip(offset)
            .take(count.unwrap_or(usize::MAX))
            .map(|(s, m)| (m.clone(), s.0))
            .collect()
    }

    pub fn count_in(&self, r: &ZRange) -> usize {
        self.iter_range(r, false).count()
    }

    /// Supprime les éléments couverts par `r`, retourne le nombre retiré.
    pub fn remove_in(&mut self, r: &ZRange) -> usize {
        let doomed: Vec<(Score, Bytes)> = self.iter_range(r, false).cloned().collect();
        for (s, m) in &doomed {
            self.by_score.remove(&(*s, m.clone()));
            self.members.remove(m);
        }
        doomed.len()
    }

    /// Bornes de clé BTree couvrant `[min, max]`. `None` si l'intervalle est vide.
    fn score_bounds(
        min: ScoreBound,
        max: ScoreBound,
    ) -> Option<(Bound<(Score, Bytes)>, Bound<(Score, Bytes)>)> {
        if min.value > max.value || (min.value == max.value && (min.exclusive || max.exclusive)) {
            return None;
        }
        let lo_score = if min.exclusive {
            if min.value == f64::INFINITY {
                return None;
            }
            min.value.next_up()
        } else {
            min.value
        };
        let lo = Bound::Included((Score::new(lo_score), Bytes::new()));
        let hi = if max.exclusive {
            Bound::Excluded((Score::new(max.value), Bytes::new()))
        } else if max.value == f64::INFINITY {
            Bound::Unbounded
        } else {
            Bound::Excluded((Score::new(max.value.next_up()), Bytes::new()))
        };
        Some((lo, hi))
    }

    /// Retire et retourne l'élément de plus petit (`max = false`) ou plus grand score.
    pub fn pop(&mut self, max: bool) -> Option<(Bytes, f64)> {
        let (score, member) = if max {
            self.by_score.pop_last()?
        } else {
            self.by_score.pop_first()?
        };
        self.members.remove(&member);
        Some((member, score.0))
    }

    /// Rang (0-based) d'un membre, en O(n) (pas de skiplist).
    pub fn rank(&self, member: &Bytes, rev: bool) -> Option<usize> {
        let score = Score::new(self.score(member)?);
        let key = (score, member.clone());
        if rev {
            Some(self.by_score.range((Bound::Excluded(key), Bound::Unbounded)).count())
        } else {
            Some(self.by_score.range(..key).count())
        }
    }
}

/// Identifiant de message stream (`ms-seq`), comparable et indexable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StreamId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamId {
    pub const ZERO: Self = Self { ms: 0, seq: 0 };

    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    pub fn to_string_id(self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    pub fn is_zero(self) -> bool {
        self.ms == 0 && self.seq == 0
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Compteur `entries_read` invalide (lag indisponible), comme Redis `SCG_INVALID_ENTRIES_READ`.
pub const STREAM_ENTRIES_READ_INVALID: i64 = -1;

#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub id: StreamId,
    pub fields: Vec<(Bytes, Bytes)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamNack {
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub active_time_ms: i64,
    /// IDs encore en PEL pour ce consommateur (index inverse).
    pub pending: BTreeSet<StreamId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamGroup {
    pub name: Bytes,
    pub last_id: StreamId,
    pub entries_read: i64,
    pub consumers: HashMap<Bytes, StreamConsumer>,
    pub pel: BTreeMap<StreamId, StreamNack>,
}

impl StreamGroup {
    pub fn new(name: Bytes, last_id: StreamId, entries_read: i64) -> Self {
        Self {
            name,
            last_id,
            entries_read,
            consumers: HashMap::new(),
            pel: BTreeMap::new(),
        }
    }

    pub fn lag(&self, entries_added: u64) -> Option<i64> {
        if self.entries_read == STREAM_ENTRIES_READ_INVALID {
            None
        } else {
            Some(entries_added as i64 - self.entries_read)
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stream {
    pub entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    /// Premier ID encore présent (0-0 si vide).
    pub first_id: StreamId,
    /// Premier ID jamais enregistré (même après trim/delete).
    pub recorded_first_id: StreamId,
    pub max_deleted_id: StreamId,
    pub entries_added: u64,
    pub groups: HashMap<Bytes, StreamGroup>,
}

impl Stream {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn refresh_first_id(&mut self) {
        self.first_id = self.entries.keys().next().copied().unwrap_or(StreamId::ZERO);
    }

    /// Génère / valide l'ID pour XADD et met à jour `last_id` (sans encore insérer).
    pub fn alloc_id(&mut self, id_spec: &str) -> Result<StreamId, &'static str> {
        let id = if id_spec == "*" {
            let mut ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if ms < self.last_id.ms {
                ms = self.last_id.ms;
            }
            let seq = if ms == self.last_id.ms {
                self.last_id.seq.checked_add(1).ok_or("ERR sequence overflow")?
            } else {
                0
            };
            StreamId::new(ms, seq)
        } else if let Some(ms_s) = id_spec.strip_suffix("-*") {
            let ms: u64 = ms_s
                .parse()
                .map_err(|_| "ERR Invalid stream ID specified as stream command argument")?;
            let seq = if ms == self.last_id.ms {
                self.last_id.seq.checked_add(1).ok_or("ERR sequence overflow")?
            } else if ms > self.last_id.ms {
                0
            } else {
                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            };
            StreamId::new(ms, seq)
        } else {
            let id = parse_stream_id(id_spec)?;
            if id.is_zero() {
                return Err("ERR The ID specified in XADD must be greater than 0-0");
            }
            if id <= self.last_id {
                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
            id
        };
        self.last_id = id;
        Ok(id)
    }

    pub fn insert_entry(&mut self, id: StreamId, fields: Vec<(Bytes, Bytes)>) {
        if self.entries.is_empty() {
            self.first_id = id;
            if self.recorded_first_id.is_zero() {
                self.recorded_first_id = id;
            }
        }
        self.entries.insert(id, fields);
        self.entries_added = self.entries_added.saturating_add(1);
        self.last_id = id;
    }

    /// Compteur entries_read valide pour un ID de création/SETID (heuristique Redis 7).
    pub fn entries_read_for_id(&self, id: StreamId) -> i64 {
        if id.is_zero() {
            return 0;
        }
        if !self.entries.is_empty() {
            if id == self.last_id {
                return self.entries_added as i64;
            }
            if let Some(&first) = self.entries.keys().next() {
                if id == first || (first > StreamId::ZERO && id < first) {
                    return (self.entries_added - self.entries.len() as u64) as i64;
                }
            }
        } else if id == self.last_id {
            return self.entries_added as i64;
        }
        STREAM_ENTRIES_READ_INVALID
    }

    pub fn mark_deleted(&mut self, id: StreamId) {
        if id > self.max_deleted_id {
            self.max_deleted_id = id;
        }
        for g in self.groups.values_mut() {
            if g.entries_read != STREAM_ENTRIES_READ_INVALID
                && g.last_id < self.last_id
                && id > g.last_id
                && id <= self.last_id
            {
                g.entries_read = STREAM_ENTRIES_READ_INVALID;
            }
        }
    }

    pub fn after_group_delivery(&mut self, group: &mut StreamGroup, delivered_last: Option<StreamId>) {
        if let Some(id) = delivered_last {
            group.last_id = id;
            if id == self.last_id {
                group.entries_read = self.entries_added as i64;
            } else if group.entries_read != STREAM_ENTRIES_READ_INVALID {
                group.entries_read = group.entries_read.saturating_add(1);
            }
        }
    }
}

/// Parse `ms-seq`, `ms` (seq=0) ou `0`.
pub fn parse_stream_id(id: &str) -> Result<StreamId, &'static str> {
    if id == "-" {
        return Ok(StreamId::ZERO);
    }
    if id == "+" {
        return Ok(StreamId::new(u64::MAX, u64::MAX));
    }
    let (ms_s, seq_s) = match id.split_once('-') {
        Some((a, b)) => (a, Some(b)),
        None => (id, None),
    };
    let ms: u64 = ms_s
        .parse()
        .map_err(|_| "ERR Invalid stream ID specified as stream command argument")?;
    let seq: u64 = match seq_s {
        None | Some("") => 0,
        Some(s) => s
            .parse()
            .map_err(|_| "ERR Invalid stream ID specified as stream command argument")?,
    };
    Ok(StreamId::new(ms, seq))
}

/// Borne de plage XRANGE/XREVRANGE : exclusive si préfixe `(`.
pub fn parse_stream_range_id(raw: &str, is_end: bool) -> Result<(StreamId, bool), &'static str> {
    if raw == "-" {
        return Ok((StreamId::ZERO, false));
    }
    if raw == "+" {
        return Ok((StreamId::new(u64::MAX, u64::MAX), false));
    }
    let exclusive = raw.starts_with('(');
    let id = parse_stream_id(if exclusive { &raw[1..] } else { raw })?;
    let _ = is_end;
    Ok((id, exclusive))
}

pub fn stream_id_gt(a: &str, b: &str) -> bool {
    match (parse_stream_id(a), parse_stream_id(b)) {
        (Ok(a), Ok(b)) => a > b,
        _ => false,
    }
}

/// Politique d'insertion des doublons TimeSeries (RedisTimeSeries).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TsDuplicatePolicy {
    #[default]
    Block,
    First,
    Last,
    Min,
    Max,
    Sum,
}

impl TsDuplicatePolicy {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "BLOCK" => Self::Block,
            "FIRST" => Self::First,
            "LAST" => Self::Last,
            "MIN" => Self::Min,
            "MAX" => Self::Max,
            "SUM" => Self::Sum,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::First => "first",
            Self::Last => "last",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
        }
    }
}

/// Agrégateur de compaction TimeSeries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsAggregator {
    Avg,
    Sum,
    Min,
    Max,
    Range,
    Count,
    First,
    Last,
    StdP,
    StdS,
    VarP,
    VarS,
    Twa,
}

impl TsAggregator {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "avg" => Self::Avg,
            "sum" => Self::Sum,
            "min" => Self::Min,
            "max" => Self::Max,
            "range" => Self::Range,
            "count" => Self::Count,
            "first" => Self::First,
            "last" => Self::Last,
            "std.p" => Self::StdP,
            "std.s" => Self::StdS,
            "var.p" => Self::VarP,
            "var.s" => Self::VarS,
            "twa" => Self::Twa,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Range => "range",
            Self::Count => "count",
            Self::First => "first",
            Self::Last => "last",
            Self::StdP => "std.p",
            Self::StdS => "std.s",
            Self::VarP => "var.p",
            Self::VarS => "var.s",
            Self::Twa => "twa",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TsRule {
    pub dest_key: Bytes,
    pub aggregator: TsAggregator,
    pub bucket_ms: u64,
    pub align_timestamp: Option<i64>,
}

/// Série temporelle embarquée (RedisTimeSeries).
#[derive(Debug, Clone, PartialEq)]
pub struct TimeSeries {
    pub retention_ms: u64,
    pub chunk_size: usize,
    pub duplicate_policy: TsDuplicatePolicy,
    pub labels: BTreeMap<String, String>,
    /// timestamp ms → valeur
    pub samples: BTreeMap<i64, f64>,
    pub rules: Vec<TsRule>,
    pub source_key: Option<Bytes>,
    pub ignore_max_time_diff: u64,
    pub ignore_max_val_diff: f64,
}

impl Default for TimeSeries {
    fn default() -> Self {
        Self {
            retention_ms: 0,
            chunk_size: 4096,
            duplicate_policy: TsDuplicatePolicy::Block,
            labels: BTreeMap::new(),
            samples: BTreeMap::new(),
            rules: Vec::new(),
            source_key: None,
            ignore_max_time_diff: 0,
            ignore_max_val_diff: 0.0,
        }
    }
}

impl TimeSeries {
    pub fn total_samples(&self) -> usize {
        self.samples.len()
    }

    pub fn trim_retention(&mut self) {
        if self.retention_ms == 0 || self.samples.is_empty() {
            return;
        }
        let Some((&last, _)) = self.samples.iter().next_back() else {
            return;
        };
        let cutoff = last.saturating_sub(self.retention_ms as i64);
        while let Some((&ts, _)) = self.samples.iter().next() {
            if ts < cutoff {
                self.samples.remove(&ts);
            } else {
                break;
            }
        }
    }

    /// Insère un point. `Ok(true)` = nouveau, `Ok(false)` = mis à jour, `Err` = bloqué.
    pub fn add(&mut self, ts: i64, value: f64) -> Result<bool, &'static str> {
        if let Some(&existing) = self.samples.get(&ts) {
            let next = match self.duplicate_policy {
                TsDuplicatePolicy::Block => {
                    return Err(
                        "ERR TSDB: Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode",
                    )
                }
                TsDuplicatePolicy::First => existing,
                TsDuplicatePolicy::Last => value,
                TsDuplicatePolicy::Min => existing.min(value),
                TsDuplicatePolicy::Max => existing.max(value),
                TsDuplicatePolicy::Sum => existing + value,
            };
            self.samples.insert(ts, next);
            self.trim_retention();
            return Ok(false);
        }
        self.samples.insert(ts, value);
        self.trim_retention();
        Ok(true)
    }

    pub fn range(
        &self,
        from: i64,
        to: i64,
        reverse: bool,
        count: Option<usize>,
    ) -> Vec<(i64, f64)> {
        let iter = self.samples.range(from..=to);
        let mut out: Vec<(i64, f64)> = if reverse {
            iter.rev().map(|(&t, &v)| (t, v)).collect()
        } else {
            iter.map(|(&t, &v)| (t, v)).collect()
        };
        if let Some(n) = count {
            out.truncate(n);
        }
        out
    }
}

/// Champ de hash avec expiration optionnelle (ms Unix absolues — Redis 7.4+).
#[derive(Debug, Clone, PartialEq)]
pub struct HashField {
    pub value: Bytes,
    pub expires_at_ms: Option<i64>,
}

impl HashField {
    #[inline]
    pub fn new(value: Bytes) -> Self {
        Self {
            value,
            expires_at_ms: None,
        }
    }

    #[inline]
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms.is_some_and(|t| t <= now_ms)
    }
}

/// Conditions `NX|XX|GT|LT` pour HEXPIRE* (une seule active).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashExpireCond {
    #[default]
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

/// Codes de retour Redis par champ pour HEXPIRE*.
pub mod hash_expire_code {
    pub const MISSING: i64 = -2;
    pub const CONDITION_NOT_MET: i64 = 0;
    pub const SET: i64 = 1;
    pub const DELETED: i64 = 2;
}

/// Hash Redis avec TTL par champ — un seul modèle (décision P11).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HashObject {
    pub fields: HashMap<Bytes, HashField>,
}

impl HashObject {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_map(map: HashMap<Bytes, Bytes>) -> Self {
        Self {
            fields: map
                .into_iter()
                .map(|(k, v)| (k, HashField::new(v)))
                .collect(),
        }
    }

    /// Supprime les champs expirés ; retourne le nombre retiré.
    pub fn purge_expired(&mut self, now_ms: i64) -> usize {
        let before = self.fields.len();
        self.fields.retain(|_, f| !f.is_expired(now_ms));
        before - self.fields.len()
    }

    pub fn live_len(&self) -> usize {
        let now = crate::util::now_unix_ms();
        self.fields.values().filter(|f| !f.is_expired(now)).count()
    }

    pub fn is_empty(&self) -> bool {
        self.live_len() == 0
    }

    pub fn get(&self, field: &[u8]) -> Option<&Bytes> {
        let now = crate::util::now_unix_ms();
        self.fields
            .get(field)
            .filter(|f| !f.is_expired(now))
            .map(|f| &f.value)
    }

    pub fn get_field(&self, field: &[u8]) -> Option<&HashField> {
        let now = crate::util::now_unix_ms();
        self.fields.get(field).filter(|f| !f.is_expired(now))
    }

    /// HSET : conserve le TTL existant du champ (parité Redis 7.4).
    pub fn insert(&mut self, field: Bytes, value: Bytes) -> bool {
        let now = crate::util::now_unix_ms();
        match self.fields.get_mut(&field) {
            Some(f) if !f.is_expired(now) => {
                f.value = value;
                false
            }
            _ => {
                self.fields.insert(field, HashField::new(value));
                true
            }
        }
    }

    pub fn insert_with_expire(&mut self, field: Bytes, value: Bytes, expires_at_ms: Option<i64>) -> bool {
        let is_new = !self
            .fields
            .get(&field)
            .is_some_and(|f| !f.is_expired(crate::util::now_unix_ms()));
        self.fields.insert(
            field,
            HashField {
                value,
                expires_at_ms,
            },
        );
        is_new
    }

    pub fn remove(&mut self, field: &[u8]) -> Option<Bytes> {
        let now = crate::util::now_unix_ms();
        match self.fields.remove(field) {
            Some(f) if !f.is_expired(now) => Some(f.value),
            _ => None,
        }
    }

    pub fn contains(&self, field: &[u8]) -> bool {
        self.get(field).is_some()
    }

    pub fn iter_live(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> + '_ {
        let now = crate::util::now_unix_ms();
        self.fields
            .iter()
            .filter(move |(_, f)| !f.is_expired(now))
            .map(|(k, f)| (k, &f.value))
    }

    pub fn pairs(&self) -> Vec<(Bytes, Bytes)> {
        self.iter_live()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn has_any_field_expire(&self) -> bool {
        let now = crate::util::now_unix_ms();
        self.fields
            .values()
            .any(|f| f.expires_at_ms.is_some() && !f.is_expired(now))
    }

    /// Applique une expiration abs ms ; `expire_at_ms == None` = PERSIST.
    /// Si `expire_at_ms` est déjà passé → champ supprimé (code DELETED).
    pub fn apply_expire(
        &mut self,
        field: &[u8],
        expire_at_ms: Option<i64>,
        cond: HashExpireCond,
        now_ms: i64,
    ) -> i64 {
        use hash_expire_code::*;
        let Some(f) = self.fields.get_mut(field) else {
            return MISSING;
        };
        if f.is_expired(now_ms) {
            self.fields.remove(field);
            return MISSING;
        }
        let cur = f.expires_at_ms;
        match cond {
            HashExpireCond::Nx if cur.is_some() => return CONDITION_NOT_MET,
            HashExpireCond::Xx if cur.is_none() => return CONDITION_NOT_MET,
            HashExpireCond::Gt => match (expire_at_ms, cur) {
                (Some(n), Some(c)) if n <= c => return CONDITION_NOT_MET,
                (_, None) => return CONDITION_NOT_MET,
                _ => {}
            },
            HashExpireCond::Lt => match (expire_at_ms, cur) {
                (Some(n), Some(c)) if n >= c => return CONDITION_NOT_MET,
                (_, None) => return CONDITION_NOT_MET,
                _ => {}
            },
            _ => {}
        }
        match expire_at_ms {
            Some(t) if t <= now_ms => {
                self.fields.remove(field);
                DELETED
            }
            other => {
                f.expires_at_ms = other;
                SET
            }
        }
    }

    pub fn persist_field(&mut self, field: &[u8]) -> i64 {
        let now = crate::util::now_unix_ms();
        match self.fields.get_mut(field) {
            None => hash_expire_code::MISSING,
            Some(f) if f.is_expired(now) => {
                self.fields.remove(field);
                hash_expire_code::MISSING
            }
            Some(f) if f.expires_at_ms.is_some() => {
                f.expires_at_ms = None;
                1
            }
            Some(_) => -1,
        }
    }

    pub fn ttl_ms(&self, field: &[u8]) -> i64 {
        let now = crate::util::now_unix_ms();
        match self.fields.get(field) {
            None => -2,
            Some(f) if f.is_expired(now) => -2,
            Some(f) => match f.expires_at_ms {
                None => -1,
                Some(t) => (t - now).max(0),
            },
        }
    }

    pub fn expire_time_ms(&self, field: &[u8]) -> i64 {
        let now = crate::util::now_unix_ms();
        match self.fields.get(field) {
            None => -2,
            Some(f) if f.is_expired(now) => -2,
            Some(f) => f.expires_at_ms.unwrap_or(-1),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RedisValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    Hash(HashObject),
    Set(crate::encoding::SetObject),
    ZSet(ZSet),
    Stream(Stream),
    /// Document JSON (RedisJSON) — `TYPE` = `ReJSON-RL`.
    Json(serde_json::Value),
    /// TimeSeries — `TYPE` = `TSDB-TYPE`.
    TimeSeries(TimeSeries),
    Bloom(crate::probabilistic::BloomFilter),
    Cuckoo(crate::probabilistic::CuckooFilter),
    Cms(crate::probabilistic::CountMinSketch),
    TopK(crate::probabilistic::TopK),
    TDigest(crate::probabilistic::TDigest),
    VectorSet(crate::vector_set::VectorSet),
}

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::List(_) => "list",
            Self::Hash(_) => "hash",
            Self::Set(_) => "set",
            Self::ZSet(_) => "zset",
            Self::Stream(_) => "stream",
            Self::Json(_) => "ReJSON-RL",
            Self::TimeSeries(_) => "TSDB-TYPE",
            Self::Bloom(_) => "MBbloom--",
            Self::Cuckoo(_) => "MBbloomCF",
            Self::Cms(_) => "CMSketch",
            Self::TopK(_) => "TopK",
            Self::TDigest(_) => "t-digest",
            Self::VectorSet(_) => "vectorset",
        }
    }

    /// Nombre d'éléments (1 pour une string) — utilisé pour les conteneurs vides.
    pub fn is_empty_container(&self) -> bool {
        match self {
            Self::String(_)
            | Self::Stream(_)
            | Self::Json(_)
            | Self::TimeSeries(_)
            | Self::Bloom(_)
            | Self::Cuckoo(_)
            | Self::Cms(_)
            | Self::TopK(_)
            | Self::TDigest(_)
            | Self::VectorSet(_) => false,
            Self::List(l) => l.is_empty(),
            Self::Hash(h) => h.is_empty(),
            Self::Set(s) => s.is_empty(),
            Self::ZSet(z) => z.is_empty(),
        }
    }

    /// Estimation grossière de l'empreinte mémoire (octets).
    pub fn approx_bytes(&self) -> usize {
        match self {
            Self::String(s) => s.len(),
            Self::List(l) => l.iter().map(|b| b.len() + 16).sum(),
            Self::Hash(h) => h
                .fields
                .iter()
                .map(|(k, f)| k.len() + f.value.len() + 40)
                .sum(),
            Self::Set(s) => s.approx_bytes(),
            Self::ZSet(z) => z.members.keys().map(|b| b.len() + 56).sum(),
            Self::Stream(s) => {
                let entries: usize = s
                    .entries
                    .iter()
                    .map(|(id, fields)| {
                        id.to_string_id().len()
                            + fields.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
                    })
                    .sum();
                let groups: usize = s.groups.values().map(|g| {
                    g.name.len()
                        + g.pel.len() * 48
                        + g.consumers.values().map(|c| c.name.len() + c.pending.len() * 16).sum::<usize>()
                }).sum();
                entries + groups
            }
            Self::Json(v) => v.to_string().len(),
            Self::TimeSeries(ts) => {
                ts.samples.len() * 24
                    + ts.labels.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
            }
            Self::Bloom(b) => b.bits.len() + b.filters.iter().map(|f| f.bits.len()).sum::<usize>(),
            Self::Cuckoo(c) => c.buckets.len() * 2,
            Self::Cms(c) => c.counters.len() * 8,
            Self::TopK(t) => t.heap.len() * 32 + t.cms.counters.len() * 8,
            Self::TDigest(t) => t.centroids.len() * 16,
            Self::VectorSet(v) => v.approx_bytes(),
        }
    }
}

static NEXT_VERSION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub value: RedisValue,
    /// Absolute expiry instant; None = no expiry.
    pub expires_at: Option<Instant>,
    /// Identité de l'écriture : un SET de la même valeur invalide quand même un WATCH.
    pub version: u64,
    /// Dernier accès (OBJECT IDLETIME / LRU).
    pub last_access: Instant,
    /// Compteur LFU approximatif (0–255) — OBJECT FREQ.
    pub lfu_freq: u8,
}

impl Entry {
    pub fn new(value: RedisValue) -> Self {
        Self {
            value,
            expires_at: None,
            version: NEXT_VERSION.fetch_add(1, AtomicOrdering::Relaxed),
            last_access: Instant::now(),
            lfu_freq: 5,
        }
    }

    pub fn with_ttl(value: RedisValue, ttl: Duration) -> Self {
        Self {
            expires_at: Some(Instant::now() + ttl),
            ..Self::new(value)
        }
    }

    /// Invalide les clients WATCH : toute mutation de contenu doit l'appeler (DRY).
    #[inline]
    pub fn mark_written(&mut self) {
        self.version = NEXT_VERSION.fetch_add(1, AtomicOrdering::Relaxed);
    }

    /// Met à jour LRU/LFU (sauf CLIENT NO-TOUCH).
    pub fn record_access(&mut self) {
        self.last_access = Instant::now();
        // Probabiliste type Redis LFU : incrément saturant.
        if self.lfu_freq < 255 {
            self.lfu_freq = self.lfu_freq.saturating_add(1);
        }
    }

    pub fn idle_secs(&self) -> i64 {
        self.last_access.elapsed().as_secs() as i64
    }

    /// Encoding réel (intset/listpack/…) — plus un simple label de seuil.
    pub fn object_encoding(&self) -> &'static str {
        use RedisValue::*;
        match &self.value {
            String(s) if crate::mem::is_compressed_payload(s) => "raw",
            String(s) if std::str::from_utf8(s).ok().and_then(|t| t.parse::<i64>().ok()).is_some() => {
                "int"
            }
            String(s) if s.len() <= 44 => "embstr",
            String(_) => "raw",
            List(l) if l.len() <= 128 => "listpack",
            List(_) => "quicklist",
            Hash(h) if h.live_len() <= crate::encoding::HASH_LISTPACK_MAX_ENTRIES
                && h.iter_live().all(|(k, v)| {
                    crate::encoding::ListPackHash::fits_entry(k, v)
                }) =>
            {
                "listpack"
            }
            Hash(_) => "hashtable",
            Set(s) => s.encoding(),
            ZSet(z) if z.len() <= 128 => "listpack",
            ZSet(_) => "skiplist",
            Stream(_) => "stream",
            Json(_) => "ReJSON-RL",
            TimeSeries(_) => "TSDB-TYPE",
            Bloom(_) => "MBbloom--",
            Cuckoo(_) => "MBbloomCF",
            Cms(_) => "CMSketch",
            TopK(_) => "TopK",
            TDigest(_) => "t-digest",
            VectorSet(_) => "vectorset",
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|t| Instant::now() >= t)
            .unwrap_or(false)
    }

    pub fn ttl_secs(&self) -> i64 {
        match self.expires_at {
            None => -1,
            Some(at) => {
                let now = Instant::now();
                if now >= at {
                    -2
                } else {
                    // Redis arrondit à la seconde la plus proche.
                    ((at.duration_since(now).as_millis() + 500) / 1000) as i64
                }
            }
        }
    }

    pub fn pttl_ms(&self) -> i64 {
        match self.expires_at {
            None => -1,
            Some(at) => {
                let now = Instant::now();
                if now >= at {
                    -2
                } else {
                    at.duration_since(now).as_millis() as i64
                }
            }
        }
    }

    /// Instant d'expiration en ms Unix (None si pas de TTL).
    pub fn expire_at_unix_ms(&self) -> Option<i64> {
        let ms = self.pttl_ms();
        if ms < 0 {
            return None;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Some(now_ms + ms)
    }

    pub fn set_expire_secs(&mut self, secs: i64) {
        self.set_expire_ms(secs.saturating_mul(1000));
    }

    /// TTL relatif en ms (plafonné à ~100 ans pour éviter tout dépassement d'`Instant`).
    pub fn set_expire_ms(&mut self, ms: i64) {
        const MAX_TTL_MS: i64 = 3_153_600_000_000;
        if ms <= 0 {
            self.expires_at = Some(Instant::now());
        } else {
            self.expires_at =
                Some(Instant::now() + Duration::from_millis(ms.min(MAX_TTL_MS) as u64));
        }
    }

    pub fn set_expire_at_unix_ms(&mut self, unix_ms: i64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let delta = unix_ms - now_ms;
        self.set_expire_ms(delta);
    }

    pub fn persist(&mut self) {
        self.expires_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn bound(v: f64) -> ScoreBound {
        ScoreBound { value: v, exclusive: false }
    }

    #[test]
    fn negative_scores_are_ordered() {
        let mut z = ZSet::default();
        z.insert(b("a"), 5.0);
        z.insert(b("b"), -3.0);
        z.insert(b("c"), 0.0);
        z.insert(b("d"), -0.0);
        let order: Vec<_> = z.range_by_rank(0, -1, false).into_iter().map(|(m, _)| m).collect();
        assert_eq!(order, vec![b("b"), b("c"), b("d"), b("a")]);
    }

    #[test]
    fn score_ranges_and_pop() {
        let mut z = ZSet::default();
        for (i, m) in ["a", "b", "c", "d"].iter().enumerate() {
            z.insert(b(m), i as f64);
        }
        let all = ZRange::Score(bound(f64::NEG_INFINITY), bound(f64::INFINITY));
        assert_eq!(z.range(&all, false, 0, None).len(), 4);
        let excl = ZRange::Score(
            ScoreBound { value: 1.0, exclusive: true },
            ScoreBound { value: 3.0, exclusive: true },
        );
        assert_eq!(z.range(&excl, false, 0, None), vec![(b("c"), 2.0)]);
        assert_eq!(z.count_in(&ZRange::Score(bound(1.0), bound(2.0))), 2);
        assert_eq!(z.pop(false), Some((b("a"), 0.0)));
        assert_eq!(z.pop(true), Some((b("d"), 3.0)));
        assert_eq!(z.rank(&b("c"), false), Some(1));
        assert_eq!(z.rank(&b("c"), true), Some(0));
        assert_eq!(z.remove_in(&ZRange::Score(bound(1.0), bound(1.0))), 1);
        assert_eq!(z.len(), 1);
    }

    fn lex(min: &str, max: &str) -> ZRange {
        ZRange::Lex(LexBound::parse(min.as_bytes()).unwrap(), LexBound::parse(max.as_bytes()).unwrap())
    }

    fn members(v: Vec<(Bytes, f64)>) -> Vec<Bytes> {
        v.into_iter().map(|(m, _)| m).collect()
    }

    #[test]
    fn lex_ranges_uniform_and_mixed() {
        let mut z = ZSet::default();
        for m in ["a", "b", "c", "d", "e"] {
            z.insert(b(m), 0.0);
        }
        assert_eq!(members(z.range(&lex("[b", "(d"), false, 0, None)), vec![b("b"), b("c")]);
        assert_eq!(members(z.range(&lex("-", "+"), true, 1, Some(2))), vec![b("d"), b("c")]);
        assert_eq!(z.count_in(&lex("(a", "[e")), 4);
        assert_eq!(z.count_in(&lex("+", "-")), 0);
        assert_eq!(z.count_in(&lex("[c", "(c")), 0);
        assert_eq!(z.count_in(&lex("[c", "[c")), 1);
        assert!(LexBound::parse(b"c").is_none());
        assert!(LexBound::parse(b"").is_none());
        assert_eq!(z.remove_in(&lex("[d", "+")), 2);
        z.insert(b("z"), 1.0);
        assert_eq!(members(z.range(&lex("[b", "+"), false, 0, None)), vec![b("b"), b("c"), b("z")]);
    }
}
