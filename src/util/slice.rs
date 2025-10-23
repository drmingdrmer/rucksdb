use std::cmp::Ordering;
use std::fmt;

#[derive(Clone, PartialEq, Eq)]
pub struct Slice {
    data: Vec<u8>,
}

impl Slice {
    pub fn new(data: Vec<u8>) -> Self {
        Slice { data }
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Slice {
            data: data.to_vec(),
        }
    }

    pub fn empty() -> Self {
        Slice { data: Vec::new() }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn compare(&self, other: &Slice) -> Ordering {
        self.data.cmp(&other.data)
    }

    pub fn starts_with(&self, prefix: &Slice) -> bool {
        self.data.starts_with(&prefix.data)
    }
}

impl From<Vec<u8>> for Slice {
    fn from(data: Vec<u8>) -> Self {
        Slice::new(data)
    }
}

impl From<&[u8]> for Slice {
    fn from(data: &[u8]) -> Self {
        Slice::from_bytes(data)
    }
}

impl From<String> for Slice {
    fn from(s: String) -> Self {
        Slice::new(s.into_bytes())
    }
}

impl From<&str> for Slice {
    fn from(s: &str) -> Self {
        Slice::from_bytes(s.as_bytes())
    }
}

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl PartialOrd for Slice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Slice {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl fmt::Debug for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.data) {
            Ok(s) => write!(f, "Slice(\"{s}\")"),
            Err(_) => write!(f, "Slice({:?})", self.data),
        }
    }
}

impl fmt::Display for Slice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(&self.data) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => write!(f, "{:?}", self.data),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_creation() {
        let s1 = Slice::from("hello");
        assert_eq!(s1.size(), 5);
        assert_eq!(s1.data(), b"hello");
    }

    #[test]
    fn test_slice_compare() {
        let s1 = Slice::from("abc");
        let s2 = Slice::from("def");
        assert!(s1 < s2);
        assert_eq!(s1.compare(&s2), Ordering::Less);
    }

    #[test]
    fn test_slice_empty() {
        let s = Slice::empty();
        assert!(s.is_empty());
        assert_eq!(s.size(), 0);
    }

    #[test]
    fn test_slice_starts_with() {
        let s = Slice::from("hello world");
        let prefix = Slice::from("hello");
        assert!(s.starts_with(&prefix));
    }
}
