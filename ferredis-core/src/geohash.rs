//! Geohash 52 bits identique à Redis (`geohash.c`, `geohash_helper.c`) : encodage des scores,
//! voisins, estimation du pas de recherche et distances (formule de haversine).

pub(crate) const STEP_MAX: u8 = 26;
pub(crate) const LAT_MIN: f64 = -85.051_128_78;
pub(crate) const LAT_MAX: f64 = 85.051_128_78;
pub(crate) const LONG_MIN: f64 = -180.0;
pub(crate) const LONG_MAX: f64 = 180.0;
const EARTH_RADIUS_M: f64 = 6_372_797.560_856;
const MERCATOR_MAX: f64 = 20_037_726.37;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Range {
    pub min: f64,
    pub max: f64,
}

pub(crate) const LONG_RANGE: Range = Range { min: LONG_MIN, max: LONG_MAX };
pub(crate) const LAT_RANGE: Range = Range { min: LAT_MIN, max: LAT_MAX };

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) struct Bits {
    pub bits: u64,
    pub step: u8,
}

impl Bits {
    pub(crate) fn is_zero(self) -> bool {
        self.bits == 0 && self.step == 0
    }

    /// Score aligné sur 52 bits (`geohashAlign52Bits`).
    pub(crate) fn align52(self) -> u64 {
        self.bits << (52 - self.step as u32 * 2)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Area {
    pub hash: Bits,
    pub longitude: Range,
    pub latitude: Range,
}

fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const B: [u64; 5] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
    ];
    const S: [u32; 5] = [1, 2, 4, 8, 16];
    let (mut x, mut y) = (xlo as u64, ylo as u64);
    for i in (0..5).rev() {
        x = (x | (x << S[i])) & B[i];
        y = (y | (y << S[i])) & B[i];
    }
    x | (y << 1)
}

fn deinterleave64(interleaved: u64) -> u64 {
    const B: [u64; 6] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0F0F_0F0F_0F0F_0F0F,
        0x00FF_00FF_00FF_00FF,
        0x0000_FFFF_0000_FFFF,
        0x0000_0000_FFFF_FFFF,
    ];
    const S: [u32; 6] = [0, 1, 2, 4, 8, 16];
    let (mut x, mut y) = (interleaved, interleaved >> 1);
    for i in 0..6 {
        x = (x | (x >> S[i])) & B[i];
        y = (y | (y >> S[i])) & B[i];
    }
    x | (y << 32)
}

/// `geohashEncode` : `None` si les coordonnées sont hors des limites supportées.
pub(crate) fn encode(long_range: Range, lat_range: Range, lon: f64, lat: f64, step: u8) -> Option<Bits> {
    if step == 0 || step > 32 {
        return None;
    }
    if !(LONG_MIN..=LONG_MAX).contains(&lon) || !(LAT_MIN..=LAT_MAX).contains(&lat) {
        return None;
    }
    if lat < lat_range.min || lat > lat_range.max || lon < long_range.min || lon > long_range.max {
        return None;
    }
    let scale = (1u64 << step) as f64;
    let lat_offset = (lat - lat_range.min) / (lat_range.max - lat_range.min) * scale;
    let long_offset = (lon - long_range.min) / (long_range.max - long_range.min) * scale;
    Some(Bits { bits: interleave64(lat_offset as u32, long_offset as u32), step })
}

pub(crate) fn encode_wgs84(lon: f64, lat: f64, step: u8) -> Option<Bits> {
    encode(LONG_RANGE, LAT_RANGE, lon, lat, step)
}

/// `geohashDecode`.
pub(crate) fn decode(long_range: Range, lat_range: Range, hash: Bits) -> Option<Area> {
    if hash.is_zero() {
        return None;
    }
    let sep = deinterleave64(hash.bits);
    let (ilato, ilono) = (sep as u32 as f64, (sep >> 32) as u32 as f64);
    let div = (1u64 << hash.step) as f64;
    let (lat_scale, long_scale) = (lat_range.max - lat_range.min, long_range.max - long_range.min);
    Some(Area {
        hash,
        latitude: Range {
            min: lat_range.min + (ilato / div) * lat_scale,
            max: lat_range.min + ((ilato + 1.0) / div) * lat_scale,
        },
        longitude: Range {
            min: long_range.min + (ilono / div) * long_scale,
            max: long_range.min + ((ilono + 1.0) / div) * long_scale,
        },
    })
}

/// Centre d'une zone, borné aux limites WGS84 supportées : `(lon, lat)`.
pub(crate) fn area_center(a: &Area) -> (f64, f64) {
    let lon = ((a.longitude.min + a.longitude.max) / 2.0).clamp(LONG_MIN, LONG_MAX);
    let lat = ((a.latitude.min + a.latitude.max) / 2.0).clamp(LAT_MIN, LAT_MAX);
    (lon, lat)
}

/// Score d'un membre GEO → `(lon, lat)`.
pub(crate) fn decode_score(score: f64) -> Option<(f64, f64)> {
    let hash = Bits { bits: score as u64, step: STEP_MAX };
    decode(LONG_RANGE, LAT_RANGE, hash).map(|a| area_center(&a))
}

fn move_x(h: &mut Bits, d: i8) {
    if d == 0 {
        return;
    }
    let shift = 64 - h.step as u32 * 2;
    let mut x = h.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let y = h.bits & 0x5555_5555_5555_5555;
    let zz = 0x5555_5555_5555_5555u64 >> shift;
    if d > 0 {
        x = x.wrapping_add(zz + 1);
    } else {
        x |= zz;
        x = x.wrapping_sub(zz + 1);
    }
    x &= 0xaaaa_aaaa_aaaa_aaaau64 >> shift;
    h.bits = x | y;
}

fn move_y(h: &mut Bits, d: i8) {
    if d == 0 {
        return;
    }
    let shift = 64 - h.step as u32 * 2;
    let x = h.bits & 0xaaaa_aaaa_aaaa_aaaa;
    let mut y = h.bits & 0x5555_5555_5555_5555;
    let zz = 0xaaaa_aaaa_aaaa_aaaau64 >> shift;
    if d > 0 {
        y = y.wrapping_add(zz + 1);
    } else {
        y |= zz;
        y = y.wrapping_sub(zz + 1);
    }
    y &= 0x5555_5555_5555_5555u64 >> shift;
    h.bits = x | y;
}

/// Voisins dans l'ordre de parcours de Redis : N, S, E, O, NE, NO, SE, SO.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Neighbors {
    pub north: Bits,
    pub south: Bits,
    pub east: Bits,
    pub west: Bits,
    pub north_east: Bits,
    pub north_west: Bits,
    pub south_east: Bits,
    pub south_west: Bits,
}

pub(crate) fn neighbors(h: Bits) -> Neighbors {
    let shifted = |dx: i8, dy: i8| {
        let mut n = h;
        move_x(&mut n, dx);
        move_y(&mut n, dy);
        n
    };
    Neighbors {
        east: shifted(1, 0),
        west: shifted(-1, 0),
        south: shifted(0, -1),
        north: shifted(0, 1),
        north_west: shifted(-1, 1),
        south_west: shifted(-1, -1),
        north_east: shifted(1, 1),
        south_east: shifted(1, -1),
    }
}

fn deg_rad(a: f64) -> f64 {
    a * (std::f64::consts::PI / 180.0)
}

fn rad_deg(a: f64) -> f64 {
    a / (std::f64::consts::PI / 180.0)
}

pub(crate) fn lat_distance(lat1: f64, lat2: f64) -> f64 {
    EARTH_RADIUS_M * (deg_rad(lat2) - deg_rad(lat1)).abs()
}

/// `geohashGetDistance` (haversine, raccourci quand les longitudes coïncident).
pub(crate) fn distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let v = ((deg_rad(lon2) - deg_rad(lon1)) / 2.0).sin();
    if v == 0.0 {
        return lat_distance(lat1, lat2);
    }
    let (lat1r, lat2r) = (deg_rad(lat1), deg_rad(lat2));
    let u = ((lat2r - lat1r) / 2.0).sin();
    let a = u * u + lat1r.cos() * lat2r.cos() * v * v;
    2.0 * EARTH_RADIUS_M * a.sqrt().asin()
}

/// Forme de recherche (dimensions déjà converties en mètres).
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Shape {
    Radius(f64),
    Box { width: f64, height: f64 },
}

/// Distance au centre si le point est dans la forme.
pub(crate) fn within(shape: Shape, center: (f64, f64), p: (f64, f64)) -> Option<f64> {
    match shape {
        Shape::Radius(r) => {
            let d = distance(center.0, center.1, p.0, p.1);
            (d <= r).then_some(d)
        }
        Shape::Box { width, height } => {
            if lat_distance(p.1, center.1) > height / 2.0 {
                return None;
            }
            if distance(p.0, p.1, center.0, p.1) > width / 2.0 {
                return None;
            }
            Some(distance(center.0, center.1, p.0, p.1))
        }
    }
}

fn estimate_steps(mut range_m: f64, lat: f64) -> u8 {
    if range_m == 0.0 {
        return 26;
    }
    let mut step: i32 = 1;
    while range_m < MERCATOR_MAX {
        range_m *= 2.0;
        step += 1;
    }
    step -= 2;
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }
    step.clamp(1, 26) as u8
}

/// `[min_lon, min_lat, max_lon, max_lat]` de la forme.
fn bounding_box(shape: Shape, (lon, lat): (f64, f64)) -> [f64; 4] {
    let (height, width) = match shape {
        Shape::Radius(r) => (r, r),
        Shape::Box { width, height } => (height / 2.0, width / 2.0),
    };
    let lat_delta = rad_deg(height / EARTH_RADIUS_M);
    let long_delta_top = rad_deg(width / EARTH_RADIUS_M / deg_rad(lat + lat_delta).cos());
    let long_delta_bottom = rad_deg(width / EARTH_RADIUS_M / deg_rad(lat - lat_delta).cos());
    let delta = if lat < 0.0 { long_delta_bottom } else { long_delta_top };
    [lon - delta, lat - lat_delta, lon + delta, lat + lat_delta]
}

/// `geohashCalculateAreasByShapeWGS84` : boîte centrale + 8 voisins (inutiles mis à zéro),
/// dans l'ordre de parcours de `membersOfAllNeighbors`.
pub(crate) fn search_areas(shape: Shape, center: (f64, f64)) -> [Bits; 9] {
    let [min_lon, min_lat, max_lon, max_lat] = bounding_box(shape, center);
    let radius_m = match shape {
        Shape::Radius(r) => r,
        Shape::Box { width, height } => ((width / 2.0).powi(2) + (height / 2.0).powi(2)).sqrt(),
    };
    let mut steps = estimate_steps(radius_m, center.1);
    let compute = |steps: u8| {
        let hash = encode_wgs84(center.0, center.1, steps).unwrap_or_default();
        let hash = Bits { step: steps, ..hash };
        (hash, neighbors(hash), decode(LONG_RANGE, LAT_RANGE, hash))
    };
    let (mut hash, mut n, mut area) = compute(steps);
    let dec = |b: Bits| decode(LONG_RANGE, LAT_RANGE, b);
    let decrease = dec(n.north).is_some_and(|a| a.latitude.max < max_lat)
        || dec(n.south).is_some_and(|a| a.latitude.min > min_lat)
        || dec(n.east).is_some_and(|a| a.longitude.max < max_lon)
        || dec(n.west).is_some_and(|a| a.longitude.min > min_lon);
    if steps > 1 && decrease {
        steps -= 1;
        (hash, n, area) = compute(steps);
    }
    if let (true, Some(area)) = (steps >= 2, area) {
        let zero = Bits::default();
        if area.latitude.min < min_lat {
            (n.south, n.south_west, n.south_east) = (zero, zero, zero);
        }
        if area.latitude.max > max_lat {
            (n.north, n.north_east, n.north_west) = (zero, zero, zero);
        }
        if area.longitude.min < min_lon {
            (n.west, n.south_west, n.north_west) = (zero, zero, zero);
        }
        if area.longitude.max > max_lon {
            (n.east, n.south_east, n.north_east) = (zero, zero, zero);
        }
    }
    [hash, n.north, n.south, n.east, n.west, n.north_east, n.north_west, n.south_east, n.south_west]
}

/// Intervalle de scores `[min, max)` couvert par une boîte.
pub(crate) fn box_scores(h: Bits) -> (f64, f64) {
    let min = h.align52();
    let max = Bits { bits: h.bits.wrapping_add(1), step: h.step }.align52();
    (min as f64, max as f64)
}

/// Chaîne geohash standard (11 caractères, latitude ±90) d'un score Redis.
pub(crate) fn standard_string(score: f64) -> Option<String> {
    const ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let (lon, lat) = decode_score(score)?;
    let hash = encode(LONG_RANGE, Range { min: -90.0, max: 90.0 }, lon, lat, STEP_MAX)?;
    Some(
        (0..11)
            .map(|i| {
                let idx = if i == 10 { 0 } else { (hash.bits >> (52 - (i + 1) * 5)) & 0x1f };
                ALPHABET[idx as usize] as char
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interleave_roundtrip() {
        let v = interleave64(0x1234_5678, 0x9abc_def0);
        let sep = deinterleave64(v);
        assert_eq!(sep as u32, 0x1234_5678);
        assert_eq!((sep >> 32) as u32, 0x9abc_def0);
    }

    #[test]
    fn palermo_matches_redis() {
        // Valeurs documentées par Redis (GEOADD Sicily 13.361389 38.115556 "Palermo").
        let score = encode_wgs84(13.361389, 38.115556, STEP_MAX).unwrap().align52();
        assert_eq!(score, 3_479_099_956_230_698);
        let (lon, lat) = decode_score(score as f64).unwrap();
        assert!((lon - 13.361_389_338_970_184).abs() < 1e-12);
        assert!((lat - 38.115_556_395_496_3).abs() < 1e-12);
        assert_eq!(standard_string(score as f64).as_deref(), Some("sqc8b49rny0"));
        let catania = encode_wgs84(15.087269, 37.502669, STEP_MAX).unwrap().align52() as f64;
        let (lon2, lat2) = decode_score(catania).unwrap();
        let d = distance(lon, lat, lon2, lat2);
        assert_eq!(format!("{d:.4}"), "166274.1516");
    }

    #[test]
    fn steps_and_areas() {
        assert_eq!(estimate_steps(0.0, 0.0), 26);
        assert!(estimate_steps(1000.0, 10.0) > estimate_steps(1000.0, 85.0));
        let areas = search_areas(Shape::Radius(200_000.0), (15.0, 37.0));
        assert!(!areas[0].is_zero());
    }
}
