use std::collections::BTreeMap;

pub struct Memtable {
    data: BTreeMap<String, Option<String>>,
    size_bytes: usize,
}

impl Memtable {
    pub const FLUSH_THRESHOLD: usize = 4 * 1024 * 1024;

    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    pub fn set(&mut self, key: String, value: String) {
        self.size_bytes += key.len() + value.len();

        self.data.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: String) {
        self.size_bytes += key.len();

        self.data.insert(key, None);
    }

    pub fn get(&self, key: &str) -> Option<&Option<String>> {
        self.data.get(key)
    }

    pub fn should_flush(&self) -> bool {
        self.size_bytes >= Self::FLUSH_THRESHOLD
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Option<String>)> {
        self.data.iter()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let mut memtable = Memtable::new();

        memtable.set("name".to_string(), "Baiju".to_string());

        assert_eq!(memtable.get("name"), Some(&Some("Baiju".to_string())));
    }

    #[test]
    fn update_replaces_value() {
        let mut memtable = Memtable::new();

        memtable.set("name".to_string(), "Baiju".to_string());
        memtable.set("name".to_string(), "Rahul".to_string());

        assert_eq!(memtable.get("name"), Some(&Some("Rahul".to_string())));
    }

    #[test]
    fn delete_creates_tombstone() {
        let mut memtable = Memtable::new();

        memtable.set("name".to_string(), "Baiju".to_string());
        memtable.delete("name".to_string());

        assert_eq!(memtable.get("name"), Some(&None));
    }

    #[test]
    fn missing_key_is_different_from_tombstone() {
        let mut memtable = Memtable::new();

        assert_eq!(memtable.get("name"), None);

        memtable.delete("name".to_string());

        assert_eq!(memtable.get("name"), Some(&None));
    }

    #[test]
    fn keys_are_sorted() {
        let mut memtable = Memtable::new();

        memtable.set("name".to_string(), "Baiju".to_string());
        memtable.set("age".to_string(), "21".to_string());
        memtable.set("country".to_string(), "Nepal".to_string());

        let keys: Vec<&str> = memtable.iter().map(|(key, _)| key.as_str()).collect();

        assert_eq!(keys, vec!["age", "country", "name"]);
    }

    #[test]
    fn empty_memtable() {
        let memtable = Memtable::new();

        assert!(memtable.is_empty());
        assert_eq!(memtable.len(), 0);
        assert_eq!(memtable.size_bytes(), 0);
        assert!(!memtable.should_flush());
    }
}
