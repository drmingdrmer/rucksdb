#![no_main]

use libfuzzer_sys::fuzz_target;
use rucksdb::memtable::memtable::InternalKey;
use rucksdb::util::Slice;

// Fuzz target for InternalKey encoding/decoding.
// Tests InternalKey encoding that previously had a bug with null bytes (0x00).
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 65535 {
        return;
    }

    let key_slice = Slice::from(data);

    let sequences = [0u64, 1, 42, u64::MAX / 2, u64::MAX - 1, u64::MAX];
    let value_types = [0u8, 1];

    for &seq in &sequences {
        for &vtype in &value_types {
            let internal_key = InternalKey::new(key_slice.clone(), seq, vtype);
            let encoded = internal_key.encode();

            match InternalKey::decode(&encoded) {
                Ok(decoded) => {
                    assert_eq!(decoded.user_key(), &key_slice,
                        "User key mismatch after encode/decode for key: {:?}", data);
                    assert_eq!(decoded.sequence(), seq,
                        "Sequence mismatch after encode/decode");
                    assert_eq!(decoded.value_type, vtype,
                        "Value type mismatch after encode/decode");
                }
                Err(_) => {
                    panic!("Failed to decode valid InternalKey for key: {:?}, seq: {}, type: {}",
                        data, seq, vtype);
                }
            }
        }
    }

    if data.len() > 1 {
        let key1 = Slice::from(&data[..1]);
        let key2 = Slice::from(data);

        let internal1 = InternalKey::new(key1.clone(), 100, 1);
        let internal2 = InternalKey::new(key2.clone(), 100, 1);

        let encoded1 = internal1.encode();
        let encoded2 = internal2.encode();

        assert_ne!(encoded1, encoded2,
            "Different keys should have different encodings: {:?} vs {:?}",
            &data[..1], data);
    }
});
