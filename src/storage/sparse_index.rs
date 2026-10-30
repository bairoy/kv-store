#[derive(Debug, Clone)]
pub struct SparseIndex {
    entries: Vec<(String, u64)>,
}

impl SparseIndex {
    pub fn new(entries: Vec<(String, u64)>) -> Self {
        Self { entries }
    }

    pub fn range_for(&self, key: &str) -> Option<(u64, Option<u64>)> {
        if self.entries.is_empty() {
            return None;
        }

        match self
            .entries
            .binary_search_by(|(indexed_key, _)| indexed_key.as_str().cmp(key))
        {
            Ok(index) => {
                let start = self.entries[index].1;

                let end = self.entries.get(index + 1).map(|entry| entry.1);

                Some((start, end))
            }

            Err(0) => None,

            Err(index) => {
                let start = self.entries[index - 1].1;

                let end = self.entries.get(index).map(|entry| entry.1);

                Some((start, end))
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> &[(String, u64)] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index() -> SparseIndex {
        SparseIndex::new(vec![
            ("apple".to_string(), 0),
            ("banana".to_string(), 100),
            ("mango".to_string(), 200),
        ])
    }

    #[test]
    fn exact_match() {
        let index = index();

        assert_eq!(index.range_for("banana"), Some((100, Some(200))));
    }

    #[test]
    fn key_between_entries() {
        let index = index();

        assert_eq!(index.range_for("grape"), Some((100, Some(200))));
    }

    #[test]
    fn key_after_last_entry() {
        let index = index();

        assert_eq!(index.range_for("zebra"), Some((200, None)));
    }

    #[test]
    fn key_before_first_entry() {
        let index = index();

        assert_eq!(index.range_for("aardvark"), None);
    }

    #[test]
    fn empty_index() {
        let index = SparseIndex::new(Vec::new());

        assert_eq!(index.range_for("anything"), None);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }
}
