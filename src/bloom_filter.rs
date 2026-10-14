use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<bool>,
    k: u32,
}

impl BloomFilter {
    pub fn new(m: usize, k: u32) -> Self {
        assert!(m > 0, "Bloom filter must contain at least one bit");
        assert!(k > 0, "Bloom filter must use at least one hash function");

        Self {
            bits: vec![false; m],
            k,
        }
    }

    /// Create a Bloom filter sized for approximately `item_count` keys.
    pub fn for_items(item_count: usize) -> Self {
        let bits_per_key = 10;

        let bit_count = item_count.saturating_mul(bits_per_key).max(64);

        Self::new(bit_count, 7)
    }

    /// Generate all hash positions for a key.
    ///
    /// We return a Vec instead of an iterator because the iterator
    /// would need to borrow both `self` and `key`, creating a lifetime
    /// relationship that is unnecessary here.
    fn hashes(&self, key: &str) -> Vec<usize> {
        (0..self.k)
            .map(|i| {
                let mut hasher = DefaultHasher::new();

                (key, i).hash(&mut hasher);

                (hasher.finish() as usize) % self.bits.len()
            })
            .collect()
    }

    /// Insert a key into the Bloom filter.
    pub fn insert(&mut self, key: &str) {
        for index in self.hashes(key) {
            self.bits[index] = true;
        }
    }

    /// Check whether a key might exist.
    ///
    /// false => definitely does not exist
    /// true  => might exist
    pub fn might_contain(&self, key: &str) -> bool {
        self.hashes(key).into_iter().all(|index| self.bits[index])
    }

    pub fn bit_count(&self) -> usize {
        self.bits.len()
    }

    pub fn hash_count(&self) -> u32 {
        self.k
    }

    /// Theoretical false-positive probability.
    ///
    /// Formula:
    ///
    /// `p = (1 - e^(-kn/m))^k`
    ///
    /// where:
    ///     m = number of bits
    ///     k = number of hash functions
    ///     n = number of inserted keys
    pub fn estimated_false_positive_rate(&self, n: usize) -> f64 {
        let m = self.bits.len() as f64;
        let k = self.k as f64;
        let n = n as f64;

        (1.0 - (-k * n / m).exp()).powf(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_are_always_maybe_present() {
        let mut bloom = BloomFilter::for_items(100);

        bloom.insert("apple");
        bloom.insert("banana");
        bloom.insert("mango");

        assert!(bloom.might_contain("apple"));
        assert!(bloom.might_contain("banana"));
        assert!(bloom.might_contain("mango"));
    }

    #[test]
    fn bloom_filter_can_reject_absent_keys() {
        let mut bloom = BloomFilter::for_items(100);

        bloom.insert("apple");
        bloom.insert("banana");

        assert!(!bloom.might_contain("definitely-not-present"));
    }

    #[test]
    fn theoretical_false_positive_rate_is_reasonable() {
        let bloom = BloomFilter::for_items(100);

        let rate = bloom.estimated_false_positive_rate(100);

        assert!(rate > 0.0);
        assert!(rate < 0.1);
    }

    #[test]
    fn configuration_is_correct() {
        let bloom = BloomFilter::for_items(100);

        assert_eq!(bloom.hash_count(), 7);
        assert!(bloom.bit_count() >= 1000);
    }
}
