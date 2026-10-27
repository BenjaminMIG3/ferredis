//! CRC64 Redis (`poly 0xad93d23594c935a9`, ReflectIn/Out, XorIn=all-ones).

const POLY: u64 = 0xad93d23594c935a9;

fn reflect(mut data: u64, data_len: usize) -> u64 {
    let mut ret = data & 1;
    for _ in 1..data_len {
        data >>= 1;
        ret = (ret << 1) | (data & 1);
    }
    ret
}

/// CRC64 Redis : init `0`, résultat sur `"123456789"` = `0xe9c6d914c4b8d9ca`.
pub fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    for &c in data {
        let mut i = 0x01u8;
        loop {
            let mut bit = crc & 0x8000_0000_0000_0000;
            if c & i != 0 {
                bit = if bit == 0 { 1 } else { 0 };
            }
            crc <<= 1;
            if bit != 0 {
                crc ^= POLY;
            }
            if i == 0x80 {
                break;
            }
            i <<= 1;
        }
    }
    reflect(crc, 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_redis_vector() {
        assert_eq!(crc64(0, b"123456789"), 0xe9c6d914c4b8d9ca);
    }
}
