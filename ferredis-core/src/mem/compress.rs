//! Compression LZ4 transparente des valeurs string (magic `FRZC`).

use bytes::{BufMut, Bytes, BytesMut};

const MAGIC: &[u8; 4] = b"FRZC";

/// Compresse si `len >= threshold` et si le gain est réel.
pub fn compress_string_bytes(raw: &[u8], threshold: usize) -> Option<Bytes> {
    if threshold == 0 || raw.len() < threshold {
        return None;
    }
    let compressed = lz4_flex::compress_prepend_size(raw);
    if compressed.len() + 8 >= raw.len() {
        return None;
    }
    let mut out = BytesMut::with_capacity(8 + compressed.len());
    out.extend_from_slice(MAGIC);
    out.put_u32_le(raw.len() as u32);
    out.extend_from_slice(&compressed);
    Some(out.freeze())
}

pub fn is_compressed_payload(b: &[u8]) -> bool {
    b.len() >= 8 && &b[..4] == MAGIC
}

pub fn decompress_string_bytes(b: &[u8]) -> Bytes {
    if !is_compressed_payload(b) {
        return Bytes::copy_from_slice(b);
    }
    let payload = &b[8..];
    match lz4_flex::decompress_size_prepended(payload) {
        Ok(v) => Bytes::from(v),
        Err(_) => Bytes::copy_from_slice(b),
    }
}
