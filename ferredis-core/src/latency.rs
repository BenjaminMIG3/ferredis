//! LATENCY MONITOR (modèle Redis : un échantillon max par seconde et par évènement, 160 au plus)
//! et histogrammes de latence par commande (LATENCY HISTOGRAM).

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const HISTORY_CAP: usize = 160;
const GRAPH_COLS: usize = 80;
const GRAPH_ROWS: usize = 4;
/// Paliers de l'histogramme : 1024 ns · 2^k, k ∈ [0, 20] (1 µs … ~1 s).
const BUCKETS: usize = 21;

#[derive(Clone, Copy, Debug)]
struct Sample {
    time: u64,
    latency: u64,
}

#[derive(Debug, Default)]
struct Series {
    samples: VecDeque<Sample>,
    max: u64,
}

/// Appels et compteurs par palier d'une commande.
#[derive(Debug, Default)]
struct Histogram {
    calls: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

/// Borne haute équivalente (`hdr_histogram`, 2 chiffres significatifs) du palier `k`, en ns.
fn bucket_high_ns(k: usize) -> u64 {
    (1032u64 << k) - 1
}

fn bucket_of(ns: u64) -> usize {
    let ns = ns.clamp(1, 1_000_000_000);
    (0..BUCKETS).find(|&k| ns <= bucket_high_ns(k)).unwrap_or(BUCKETS - 1)
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Dernier échantillon d'un évènement : `(nom, horodatage s, latence ms, max ms)`.
pub type LatestEntry = (String, u64, u64, u64);

#[derive(Debug)]
pub struct LatencyMonitor {
    threshold_ms: AtomicU64,
    events: Mutex<HashMap<String, Series>>,
    histograms: DashMap<String, Histogram>,
}

impl LatencyMonitor {
    pub fn new(threshold_ms: u64) -> Self {
        Self { threshold_ms: AtomicU64::new(threshold_ms), events: Mutex::new(HashMap::new()), histograms: DashMap::new() }
    }

    pub fn set_threshold_ms(&self, ms: u64) {
        self.threshold_ms.store(ms, Ordering::Relaxed);
    }

    pub fn threshold_ms(&self) -> u64 {
        self.threshold_ms.load(Ordering::Relaxed)
    }

    /// `latencyAddSampleIfNeeded` : seuil nul = moniteur désactivé.
    pub fn record(&self, event: &str, duration: Duration) {
        let ms = duration.as_millis() as u64;
        let threshold = self.threshold_ms();
        if threshold == 0 || ms < threshold {
            return;
        }
        self.add_sample(event, now_secs(), ms);
    }

    fn add_sample(&self, event: &str, time: u64, latency: u64) {
        let mut events = self.events.lock();
        let s = events.entry(event.to_string()).or_default();
        s.max = s.max.max(latency);
        match s.samples.back_mut() {
            Some(last) if last.time == time => last.latency = last.latency.max(latency),
            _ => {
                if s.samples.len() >= HISTORY_CAP {
                    s.samples.pop_front();
                }
                s.samples.push_back(Sample { time, latency });
            }
        }
    }

    /// Commande exécutée : histogramme puis évènement `command` / `fast-command`.
    pub fn record_command(&self, name: &str, duration: Duration, fast: bool) {
        let k = bucket_of(duration.as_micros() as u64 * 1000);
        let bump = |h: &Histogram| {
            h.calls.fetch_add(1, Ordering::Relaxed);
            h.buckets[k].fetch_add(1, Ordering::Relaxed);
        };
        match self.histograms.get(name) {
            Some(h) => bump(&h),
            None => bump(&self.histograms.entry(name.to_string()).or_default()),
        }
        self.record(if fast { "fast-command" } else { "command" }, duration);
    }

    /// LATENCY RESET [event …] : nombre d'évènements effacés.
    pub fn reset(&self, events: &[String]) -> usize {
        let mut map = self.events.lock();
        if events.is_empty() {
            let n = map.len();
            map.clear();
            return n;
        }
        events.iter().filter(|e| map.remove(e.as_str()).is_some()).count()
    }

    pub fn latest(&self) -> Vec<LatestEntry> {
        let mut v: Vec<_> = self
            .events
            .lock()
            .iter()
            .filter_map(|(k, s)| s.samples.back().map(|l| (k.clone(), l.time, l.latency, s.max)))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    pub fn history_for(&self, event: &str) -> Vec<(u64, u64)> {
        self.events
            .lock()
            .get(event)
            .map(|s| s.samples.iter().map(|x| (x.time, x.latency)).collect())
            .unwrap_or_default()
    }

    /// LATENCY GRAPH (`latencyCommandGenSparkeline`) ; `None` sans échantillon.
    pub fn graph(&self, event: &str) -> Option<String> {
        let events = self.events.lock();
        let s = events.get(event).filter(|s| !s.samples.is_empty())?;
        let now = now_secs();
        let samples: Vec<(u64, String)> = s
            .samples
            .iter()
            .map(|x| {
                let e = now.saturating_sub(x.time);
                let label = match e {
                    0..=59 => format!("{e}s"),
                    60..=3599 => format!("{}m", e / 60),
                    3600..=86_399 => format!("{}h", e / 3600),
                    _ => format!("{}d", e / 86_400),
                };
                (x.latency, label)
            })
            .collect();
        let hi = samples.iter().map(|x| x.0).max().unwrap_or(0);
        let lo = samples.iter().map(|x| x.0).min().unwrap_or(0);
        let mut out = format!("{event} - high {hi} ms, low {lo} ms (all time high {} ms)\n", s.max);
        out.push_str(&"-".repeat(GRAPH_COLS));
        out.push('\n');
        render_sparkline(&mut out, &samples, lo as f64, hi as f64);
        Some(out)
    }

    /// LATENCY HISTOGRAM : `(commande, appels, [(µs, cumul)])`, triés par nom.
    pub fn histograms(&self, names: &[String]) -> Vec<(String, u64, Vec<(u64, u64)>)> {
        let one = |name: &str, h: &Histogram| {
            let calls = h.calls.load(Ordering::Relaxed);
            let mut cumulative = 0;
            let mut points = Vec::new();
            for (k, b) in h.buckets.iter().enumerate() {
                let c = b.load(Ordering::Relaxed);
                if c > 0 {
                    cumulative += c;
                    points.push((bucket_high_ns(k) / 1000, cumulative));
                }
            }
            (name.to_string(), calls, points)
        };
        let mut out: Vec<_> = if names.is_empty() {
            self.histograms.iter().map(|e| one(e.key(), e.value())).collect()
        } else {
            let mut seen = std::collections::HashSet::new();
            names
                .iter()
                .filter(|n| seen.insert(n.as_str()))
                .filter_map(|n| self.histograms.get(n.as_str()).map(|h| one(n, &h)))
                .collect()
        };
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}

/// `sparklineRender` (mode FILL, 4 lignes, libellés verticaux sous le graphe).
fn render_sparkline(out: &mut String, samples: &[(u64, String)], min: f64, max: f64) {
    const FILL: [char; 3] = ['_', 'o', '#'];
    let steps = (FILL.len() * GRAPH_ROWS) as f64;
    let relmax = if max - min == 0.0 { 1.0 } else { max - min };
    for (n, chunk) in samples.chunks(GRAPH_COLS).enumerate() {
        if n != 0 {
            out.push('\n');
        }
        let step_of = |v: u64| {
            let s = (((v as f64 - min) * steps) as i64) as f64 / relmax;
            (s as i64).clamp(0, steps as i64 - 1)
        };
        let mut row = 0;
        loop {
            let mut line = vec![' '; chunk.len()];
            let mut more = false;
            if row < GRAPH_ROWS {
                more = true;
                for (j, (v, _)) in chunk.iter().enumerate() {
                    let idx = step_of(*v) - ((GRAPH_ROWS - row - 1) * FILL.len()) as i64;
                    if (0..FILL.len() as i64).contains(&idx) {
                        line[j] = FILL[idx as usize];
                    } else if idx >= FILL.len() as i64 {
                        line[j] = '|';
                    }
                }
            } else if row == GRAPH_ROWS {
                more = true;
            } else {
                let at = row - GRAPH_ROWS - 1;
                for (j, (_, label)) in chunk.iter().enumerate() {
                    if let Some(c) = label.as_bytes().get(at) {
                        line[j] = *c as char;
                        more = true;
                    }
                }
            }
            if !more {
                break;
            }
            out.extend(line);
            out.push('\n');
            row += 1;
        }
    }
}

pub type SharedLatency = Arc<LatencyMonitor>;

pub fn shared_latency() -> SharedLatency {
    Arc::new(LatencyMonitor::new(0))
}

/// LATENCY DOCTOR : synthèse lisible des évènements enregistrés.
pub fn doctor_report(lat: &LatencyMonitor) -> String {
    let latest = lat.latest();
    if latest.is_empty() {
        return "Dave, no latency spike was observed during the lifetime of this Redis instance, not in the slightest bit. I honestly think you ought to sleep better tonight.\n".into();
    }
    let mut out = String::from("Dave, I have observed latency spikes in this Redis instance. You don't mind talking about it, do you Dave?\n\n");
    for (i, (ev, _, _, max)) in latest.iter().enumerate() {
        let hist = lat.history_for(ev);
        let avg = hist.iter().map(|h| h.1).sum::<u64>() / hist.len().max(1) as u64;
        let _ = writeln!(
            out,
            "{}. {ev}: {} latency spikes (average {avg}ms, mean deviation 0ms, period 0 sec). Worst all time event {max}ms.",
            i + 1,
            hist.len()
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_merge_per_second() {
        let m = LatencyMonitor::new(1);
        m.add_sample("ev", 10, 5);
        m.add_sample("ev", 10, 9);
        m.add_sample("ev", 11, 3);
        assert_eq!(m.history_for("ev"), vec![(10, 9), (11, 3)]);
        assert_eq!(m.latest(), vec![("ev".to_string(), 11, 3, 9)]);
        assert_eq!(m.reset(&["nope".into()]), 0);
        assert_eq!(m.reset(&[]), 1);
    }

    #[test]
    fn disabled_when_threshold_zero() {
        let m = LatencyMonitor::new(0);
        m.record("ev", Duration::from_secs(1));
        assert!(m.latest().is_empty());
    }

    #[test]
    fn histogram_buckets_are_power_of_two() {
        let m = LatencyMonitor::new(0);
        m.record_command("get", Duration::from_nanos(500), true);
        m.record_command("get", Duration::from_micros(3), true);
        m.record_command("get", Duration::from_micros(40), true);
        let h = m.histograms(&[]);
        assert_eq!(h, vec![("get".to_string(), 3, vec![(1, 1), (4, 2), (66, 3)])]);
    }

    #[test]
    fn graph_layout() {
        let m = LatencyMonitor::new(1);
        let now = now_secs();
        m.add_sample("ev", now, 10);
        m.add_sample("ev", now + 1, 20);
        let g = m.graph("ev").unwrap();
        let lines: Vec<&str> = g.lines().collect();
        assert_eq!(lines[0], "ev - high 20 ms, low 10 ms (all time high 20 ms)");
        assert_eq!(lines[1].len(), GRAPH_COLS);
        assert_eq!(lines[2], " #");
        assert_eq!(lines[5], "_|");
        assert_eq!(lines[6], "  ");
        assert!(lines[7].starts_with('0'));
        assert!(m.graph("missing").is_none());
    }
}
