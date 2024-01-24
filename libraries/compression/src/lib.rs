mod compression;

pub use compression::CompressionDictionary;

#[cfg(test)]
mod tests {
    use std::fs;
    use crate::CompressionDictionary;

    #[test]
    fn load_test() {
        let file_path = "load_test_dictionary.bin";
        let _ = fs::remove_file(file_path);
        CompressionDictionary::load(file_path).expect("Failed to create empty file!");
        CompressionDictionary::load(file_path).expect("Failed to load empty file!");
        let _ = fs::remove_file(file_path);
    }

    #[test]
    fn add_test() {
        let file_path = "add_test_dictionary.bin";

        let _ = fs::remove_file(file_path);
        let mut dictionary = CompressionDictionary::load(file_path).expect("Failed to create empty file!");

        let n = 1000;
        let mut keys: Vec<Box<[u8]>> = vec![];
        for i in 0u64..n {
            keys.push(Box::from(format!("key{i}", i = if i % 3 == 0 { i + 1 } else { i }).as_bytes()));
        }

        dictionary.add(&keys).expect("Failed to fill the dictionary with keys!");

        let dictionary2 = CompressionDictionary::load(file_path).expect("Failed to load dictionary file!");

        for i in 0u64..n {
            let key: Box<[u8]> = Box::from(format!("key{i}", i = i).as_bytes());
            if i % 3 == 0 {
                assert!(!dictionary2.map.contains_key(&key), "This key shouldn't be in the dictionary!");
                assert!(!dictionary2.list.contains(&key), "This key shouldn't be in the dictionary!");
            } else {
                assert!(dictionary2.map.contains_key(&key), "This key should be in the dictionary!");
                assert!(dictionary2.list.contains(&key), "This key should be in the dictionary!");
            }
        }

        assert_eq!(dictionary2.list.len() as u64, n / 3 * 2 + 1, "Wrong number of keys in the dictionary!");

        let _ = fs::remove_file(file_path);
    }

    #[test]
    fn decode_test() {
        let file_path = "decode_test_dictionary.bin";

        let _ = fs::remove_file(file_path);
        let mut dictionary = CompressionDictionary::load(file_path).expect("Failed to create empty file!");

        let n = 1000;
        let mut keys: Vec<Box<[u8]>> = vec![];
        for i in 0u64..n {
            keys.push(Box::from(format!("key{i}", i = i * 2).as_bytes()));
        }

        dictionary.add(&keys).expect("Failed to fill the dictionary with keys!");

        let dictionary2 = CompressionDictionary::load(file_path).expect("Failed to load dictionary file!");

        for i in 0u64..n {
            let decoded_key: Box<[u8]> = Box::from(format!("key{i}", i = i * 2).as_bytes());
            let encoded_key = Box::from(i.to_ne_bytes());
            assert_eq!(dictionary2.decode(&encoded_key).expect("Key should exist!"), decoded_key, "Wrong key returned!");
        }

        let _ = fs::remove_file(file_path);
    }

    #[test]
    fn encode_test() {
        let file_path = "encode_test_dictionary.bin";

        let _ = fs::remove_file(file_path);
        let mut dictionary = CompressionDictionary::load(file_path).expect("Failed to create empty file!");

        let n = 10;
        let mut keys: Vec<Box<[u8]>> = vec![];
        for i in 0u64..n {
            keys.push(Box::from(format!("key{i}", i = i).as_bytes()));
        }

        dictionary.add(&keys).expect("Failed to fill the dictionary with keys!");

        let decoded_key: Box<[u8]> = Box::from(b"key1".as_slice());
        let decoded_key2: Box<[u8]> = Box::from(b"key11".as_slice());
        let encoded_key = Box::from(1u64.to_ne_bytes());
        let encoded_key2 = Box::from(11u64.to_ne_bytes());

        assert_eq!(dictionary.decode(&encoded_key).expect("Key should exist!"), decoded_key, "Wrong key returned!");

        let encoded_key_tmp = dictionary.encode(&decoded_key).expect("Key should exist!");
        assert_eq!(encoded_key_tmp, encoded_key, "Keys should match!");

        let encoded_key_tmp2 = dictionary.encode(&decoded_key2).expect("Key should be added!");
        assert_eq!(encoded_key_tmp2, encoded_key2, "Keys should match!");

        let _ = fs::remove_file(file_path);
    }
}
