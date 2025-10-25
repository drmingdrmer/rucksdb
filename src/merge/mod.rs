use crate::{Result, Slice};

/// Trait for implementing custom merge operators
///
/// A merge operator defines how to combine a base value with a sequence of
/// merge operands. This enables efficient atomic read-modify-write operations.
///
/// # Examples
///
/// ```ignore
/// struct CounterMerge;
///
/// impl MergeOperator for CounterMerge {
///     fn name(&self) -> &str {
///         "CounterMerge"
///     }
///
///     fn full_merge(&self, key: &Slice, existing_value: Option<&Slice>, operands: &[Slice]) -> Result<Slice> {
///         let mut sum = existing_value
///             .and_then(|v| String::from_utf8_lossy(v.data()).parse::<i64>().ok())
///             .unwrap_or(0);
///
///         for operand in operands {
///             if let Ok(delta) = String::from_utf8_lossy(operand.data()).parse::<i64>() {
///                 sum += delta;
///             }
///         }
///
///         Ok(Slice::from(sum.to_string()))
///     }
/// }
/// ```
pub trait MergeOperator: Send + Sync {
    /// Returns the name of this merge operator
    fn name(&self) -> &str;

    /// Combines a base value with a sequence of merge operands
    ///
    /// # Arguments
    /// * `key` - The key being merged
    /// * `existing_value` - The existing value (None if key doesn't exist)
    /// * `operands` - Sequence of merge operands to apply
    ///
    /// # Returns
    /// The merged value, or an error if the merge cannot be performed
    fn full_merge(
        &self,
        key: &Slice,
        existing_value: Option<&Slice>,
        operands: &[Slice],
    ) -> Result<Slice>;

    /// Optional: Combines two merge operands
    ///
    /// This is called during compaction to combine multiple merge operands
    /// before the final full_merge. Implementing this can improve performance.
    fn partial_merge(&self, _key: &Slice, operands: &[Slice]) -> Option<Slice> {
        // Default: no partial merge support
        if operands.len() == 1 {
            Some(operands[0].clone())
        } else {
            None
        }
    }
}

/// Built-in merge operator for integer counters
///
/// Interprets values and operands as i64 integers and performs addition.
pub struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn name(&self) -> &str {
        "CounterMerge"
    }

    fn full_merge(
        &self,
        _key: &Slice,
        existing_value: Option<&Slice>,
        operands: &[Slice],
    ) -> Result<Slice> {
        let mut sum = existing_value
            .and_then(|v| String::from_utf8_lossy(v.data()).parse::<i64>().ok())
            .unwrap_or(0);

        for operand in operands {
            if let Ok(delta) = String::from_utf8_lossy(operand.data()).parse::<i64>() {
                sum += delta;
            }
        }

        Ok(Slice::from(sum.to_string()))
    }

    fn partial_merge(&self, _key: &Slice, operands: &[Slice]) -> Option<Slice> {
        if operands.is_empty() {
            return None;
        }

        let mut sum: i64 = 0;
        for operand in operands {
            if let Ok(delta) = String::from_utf8_lossy(operand.data()).parse::<i64>() {
                sum += delta;
            } else {
                return None; // Cannot partial merge non-integer operands
            }
        }

        Some(Slice::from(sum.to_string()))
    }
}

/// Built-in merge operator for appending strings
///
/// Concatenates all operands to the existing value.
pub struct StringAppendMerge {
    delimiter: String,
}

impl StringAppendMerge {
    pub fn new(delimiter: impl Into<String>) -> Self {
        StringAppendMerge {
            delimiter: delimiter.into(),
        }
    }
}

impl Default for StringAppendMerge {
    fn default() -> Self {
        StringAppendMerge::new("")
    }
}

impl MergeOperator for StringAppendMerge {
    fn name(&self) -> &str {
        "StringAppendMerge"
    }

    fn full_merge(
        &self,
        _key: &Slice,
        existing_value: Option<&Slice>,
        operands: &[Slice],
    ) -> Result<Slice> {
        let mut result = existing_value
            .map(|v| String::from_utf8_lossy(v.data()).to_string())
            .unwrap_or_default();

        for (i, operand) in operands.iter().enumerate() {
            if !result.is_empty() || i > 0 {
                result.push_str(&self.delimiter);
            }
            result.push_str(&String::from_utf8_lossy(operand.data()));
        }

        Ok(Slice::from(result))
    }

    fn partial_merge(&self, _key: &Slice, operands: &[Slice]) -> Option<Slice> {
        if operands.is_empty() {
            return None;
        }

        let mut result = String::new();
        for (i, operand) in operands.iter().enumerate() {
            if i > 0 {
                result.push_str(&self.delimiter);
            }
            result.push_str(&String::from_utf8_lossy(operand.data()));
        }

        Some(Slice::from(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_merge_with_existing_value() {
        let merge = CounterMerge;
        let key = Slice::from("counter");
        let existing = Slice::from("10");
        let operands = vec![Slice::from("5"), Slice::from("3"), Slice::from("-2")];

        let result = merge.full_merge(&key, Some(&existing), &operands).unwrap();
        assert_eq!(result.to_string(), "16"); // 10 + 5 + 3 - 2
    }

    #[test]
    fn test_counter_merge_without_existing_value() {
        let merge = CounterMerge;
        let key = Slice::from("counter");
        let operands = vec![Slice::from("5"), Slice::from("10")];

        let result = merge.full_merge(&key, None, &operands).unwrap();
        assert_eq!(result.to_string(), "15");
    }

    #[test]
    fn test_counter_partial_merge() {
        let merge = CounterMerge;
        let key = Slice::from("counter");
        let operands = vec![Slice::from("5"), Slice::from("3"), Slice::from("2")];

        let result = merge.partial_merge(&key, &operands).unwrap();
        assert_eq!(result.to_string(), "10");
    }

    #[test]
    fn test_string_append_merge_no_delimiter() {
        let merge = StringAppendMerge::default();
        let key = Slice::from("log");
        let existing = Slice::from("Hello");
        let operands = vec![Slice::from("World"), Slice::from("!")];

        let result = merge.full_merge(&key, Some(&existing), &operands).unwrap();
        assert_eq!(result.to_string(), "HelloWorld!");
    }

    #[test]
    fn test_string_append_merge_with_delimiter() {
        let merge = StringAppendMerge::new(",");
        let key = Slice::from("log");
        let existing = Slice::from("a");
        let operands = vec![Slice::from("b"), Slice::from("c")];

        let result = merge.full_merge(&key, Some(&existing), &operands).unwrap();
        assert_eq!(result.to_string(), "a,b,c");
    }

    #[test]
    fn test_string_append_partial_merge() {
        let merge = StringAppendMerge::new("-");
        let key = Slice::from("log");
        let operands = vec![Slice::from("foo"), Slice::from("bar")];

        let result = merge.partial_merge(&key, &operands).unwrap();
        assert_eq!(result.to_string(), "foo-bar");
    }

    #[test]
    fn test_string_append_no_existing_value() {
        let merge = StringAppendMerge::new(" ");
        let key = Slice::from("log");
        let operands = vec![Slice::from("Hello"), Slice::from("World")];

        let result = merge.full_merge(&key, None, &operands).unwrap();
        assert_eq!(result.to_string(), "Hello World");
    }
}
