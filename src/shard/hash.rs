//! A hash function every node agrees on.

pub fn stable_hash(input: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;

    for byte in input.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }

    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);

    hash ^ (hash >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_values_are_pinned() {
        assert_eq!(stable_hash(""), 0xf52a_15e9_a9b5_e89b);
        assert_eq!(stable_hash("a"), 0x02c0_bdbf_4814_20f8);
        assert_eq!(stable_hash("user:101"), 0x23e1_7d8d_9df6_23cc);
        assert_eq!(stable_hash("node-1-vnode-0"), 0x6bbc_e8b8_1b46_464e);
        assert_eq!(stable_hash("partition-0"), 0xcf66_28e1_ba96_ea97);
    }

    #[test]
    fn similar_inputs_spread_far_apart() {
        let a = stable_hash("user:1");
        let b = stable_hash("user:2");

        assert!((a ^ b).count_ones() > 16);
    }
}
