pub fn adler32(data: &[u8]) -> u32 {
    let mut a: u32 = 1;
    let mut b: u32 = 0;
    for x in data {
        a = (a + *x as u32) % 65521;
        b = (a + b) % 65521;
    }
    (b << 16) | a
}

pub use murmur::murmur3;

mod murmur {
    use std::convert::TryInto;

    // This is a direct port of MurmurHash3_x86_32 from:
    //   https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp

    #[inline]
    fn fmix32(mut h: u32) -> u32 {
        h ^= h >> 16;
        h = h.wrapping_mul(0x85ebca6b);
        h ^= h >> 13;
        h = h.wrapping_mul(0xc2b2ae35);
        h ^= h >> 16;
        h
    }

    pub fn murmur3(data: &[u8], seed: u32) -> u32 {
        let len = data.len() as u32;
        let nblocks = (len / 4) as usize;
        let mut h1 = seed;

        const C1: u32 = 0xcc9e2d51;
        const C2: u32 = 0x1b873593;

        for i in 0..nblocks {
            let off = i * 4;
            let block: [u8; 4] = data[off..off + 4].try_into().unwrap();
            let mut k1 = u32::from_le_bytes(block);

            k1 = k1.wrapping_mul(C1);
            k1 = k1.rotate_left(15);
            k1 = k1.wrapping_mul(C2);

            h1 ^= k1;
            h1 = h1.rotate_left(13);
            h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
        }

        let tail = &data[nblocks * 4..];
        let mut k1 = match tail.len() {
            0 => 0,
            1 => tail[0] as u32,
            2 => (tail[1] as u32) << 8 | tail[0] as u32,
            3 => (tail[2] as u32) << 16 | (tail[1] as u32) << 8 | tail[0] as u32,
            _ => unreachable!(),
        };
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);

        h1 ^= k1;

        h1 ^= len;

        fmix32(h1)
    }

    #[cfg(test)]
    mod tests {
        use rand::Rng;
        use std::io::Cursor;

        #[test]
        fn test() {
            fn compare(input: &[u8]) -> u32 {
                let expected = murmur3::murmur3_32(&mut Cursor::new(input), 0).unwrap();
                let actual = super::murmur3(input, 0);
                assert_eq!(expected, actual);
                actual
            }

            for _i in 0..100 {
                let mut rng = rand::thread_rng();
                let len: usize = rng.gen_range(0..1000);
                let mut buf = vec![0u8; len];
                rng.fill(&mut buf[..]);
                let h = compare(&buf);
                println!("len={}, hash={}", len, h);
            }
        }
    }
}
