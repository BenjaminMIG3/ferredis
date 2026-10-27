//! Codec RDB Redis natif (v11) : SAVE/BGSAVE, DUMP/RESTORE, preamble AOF, migration FRDB001.

mod crc64;
mod file;
mod frdb;
mod io;
mod listpack;
mod stream_codec;
mod value_codec;

pub use crc64::crc64;
pub use file::{
    create_dump_payload, decode_rdb, decode_rdb_or_frdb, encode_rdb_bytes, encode_rdb_bytes_full,
    load_rdb_file, load_rdb_file_full, save_rdb_file, save_rdb_file_full, verify_and_decode_dump,
    RdbSnapshot,
};
pub use io::{RdbError, RDB_VERSION};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{Entry, RedisValue, Stream, StreamId};
    use bytes::Bytes;
    use std::collections::BTreeMap;

    #[test]
    fn stream_dump_roundtrip() {
        let mut s = Stream::default();
        let id = StreamId::new(1, 0);
        s.entries
            .insert(id, vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))]);
        s.last_id = id;
        s.first_id = id;
        s.recorded_first_id = id;
        s.entries_added = 1;
        let v = RedisValue::Stream(s);
        let p = create_dump_payload(&v);
        let got = verify_and_decode_dump(&p).unwrap();
        match got {
            RedisValue::Stream(gs) => {
                assert_eq!(gs.entries.len(), 1);
                assert_eq!(gs.entries_added, 1);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rdb_with_expire_roundtrip() {
        let mut e = Entry::new(RedisValue::String(Bytes::from_static(b"x")));
        e.expires_at = Some(std::time::Instant::now() + std::time::Duration::from_secs(60));
        let dump = vec![(0usize, vec![(Bytes::from_static(b"k"), e)])];
        let bytes = encode_rdb_bytes(&dump, &[]).unwrap();
        let snap = decode_rdb(&bytes).unwrap();
        assert!(snap.dbs[0].1[0].1.expires_at.is_some());
    }

    #[test]
    fn empty_rdb_decodes() {
        let bytes = encode_rdb_bytes(&[], &[]).unwrap();
        let snap = decode_rdb(&bytes).unwrap();
        assert!(snap.dbs.is_empty());
        let _ = BTreeMap::<u8, u8>::new();
    }
}
