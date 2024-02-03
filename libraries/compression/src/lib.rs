mod compression;

pub use compression::{variable_decode, variable_encode};
pub use compression::CompressionDictionary;

#[cfg(test)]
mod tests {
    use crate::compression::{variable_decode, variable_encode};
    use crate::CompressionDictionary;
    use std::fs::remove_dir_all;
    use std::fs;

    #[test]
    fn load_test() {
        let compression_dict_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let file_path = compression_dict_dir.path().join("test\\load_test_dictionary.bin").to_str().unwrap().to_owned();
        CompressionDictionary::load(file_path.as_str()).expect("Failed to create empty file!");
        CompressionDictionary::load(file_path.as_str()).expect("Failed to load empty file!");
        remove_dir_all(compression_dict_dir).expect("Unable to remove temp dir!");
    }

    #[test]
    fn add_test() {
        let compression_dict_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let file_path = compression_dict_dir.path().join("test\\add_test_dictionary.bin").to_str().unwrap().to_owned();
        let mut dictionary = CompressionDictionary::load(file_path.as_str()).expect("Failed to create empty file!");

        let n = 1000;
        let mut keys: Vec<Box<[u8]>> = vec![];
        for i in 0u64..n {
            keys.push(Box::from(format!("key{i}", i = if i % 3 == 0 { i + 1 } else { i }).as_bytes()));
        }

        dictionary.add(&keys).expect("Failed to fill the dictionary with keys!");

        let dictionary2 = CompressionDictionary::load(file_path.as_str()).expect("Failed to load dictionary file!");

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

        remove_dir_all(compression_dict_dir).expect("Unable to remove temp dir!");
    }

    #[test]
    fn decode_test() {
        let compression_dict_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let file_path = compression_dict_dir.path().join("test\\decode_test_dictionary.bin").to_str().unwrap().to_owned();
        let mut dictionary = CompressionDictionary::load(file_path.as_str()).expect("Failed to create empty file!");

        let n = 1000;
        let mut keys: Vec<Box<[u8]>> = vec![];
        for i in 0u64..n {
            keys.push(Box::from(format!("key{i}", i = i * 2).as_bytes()));
        }

        dictionary.add(&keys).expect("Failed to fill the dictionary with keys!");

        let dictionary2 = CompressionDictionary::load(file_path.as_str()).expect("Failed to load dictionary file!");

        for i in 0u64..n {
            let decoded_key: Box<[u8]> = Box::from(format!("key{i}", i = i * 2).as_bytes());
            let encoded_key = variable_encode(i as u128);
            assert_eq!(dictionary2.decode(&encoded_key).expect("Key should exist!"), decoded_key, "Wrong key returned!");
        }

        remove_dir_all(compression_dict_dir).expect("Unable to remove temp dir!");
    }

    #[test]
    fn encode_test() {
        let compression_dict_dir = tempfile::tempdir().expect("Failed to create temporary directory");
        let file_path = compression_dict_dir.path().join("test\\encode_test_dictionary.bin").to_str().unwrap().to_owned();
        let mut dictionary = CompressionDictionary::load(file_path.as_str()).expect("Failed to create empty file!");

        let n = 10;
        let mut keys: Vec<Box<[u8]>> = vec![];
        for i in 0u64..n {
            keys.push(Box::from(format!("key{i}", i = i).as_bytes()));
        }

        dictionary.add(&keys).expect("Failed to fill the dictionary with keys!");

        let decoded_key: Box<[u8]> = Box::from(b"key1".as_slice());
        let decoded_key2: Box<[u8]> = Box::from(b"key10".as_slice());
        let encoded_key = variable_encode(1u128);
        let encoded_key2 = variable_encode(10u128);

        assert_eq!(dictionary.decode(&encoded_key).expect("Key should exist!"), decoded_key, "Wrong key returned!");

        let encoded_key_tmp = dictionary.encode(&decoded_key).expect("Key should exist!");
        assert_eq!(encoded_key_tmp, encoded_key, "Keys should match!");

        let encoded_key_tmp2 = dictionary.encode(&decoded_key2).expect("Key should be added!");
        assert_eq!(encoded_key_tmp2, encoded_key2, "Keys should match!");

        let _ = fs::remove_file(file_path);
    }

    #[test]
    fn variable_encode_test() {
        let mut test = 0b110010001u128;
        let mut solution = vec![0b10000011u8, 0b00010001u8].into_boxed_slice();
        assert_eq!(variable_encode(test), solution, "Test 1 failed!");

        test = 0b00000000u128;
        solution = vec![0b00000000u8].into_boxed_slice();
        assert_eq!(variable_encode(test), solution, "Test 2 failed!");

        test = 0b11000000u128;
        solution = vec![0b10000001u8, 0b01000000u8].into_boxed_slice();
        assert_eq!(variable_encode(test), solution, "Test 3 failed!");

        test = 0b01000000u128;
        solution = vec![0b01000000u8].into_boxed_slice();
        assert_eq!(variable_encode(test), solution, "Test 4 failed!");

        test = 0b00011111_11111111_11111111u128;
        solution = vec![0b11111111u8, 0b11111111u8, 0b01111111u8].into_boxed_slice();
        assert_eq!(variable_encode(test), solution, "Test 5 failed!");
    }

    #[test]
    fn variable_decode_test() {
        let mut offset = 0;
        let mut buffer = Vec::<u8>::new();
        buffer.extend(vec![0b10000011u8, 0b00010001u8]);
        buffer.extend(vec![0b00000000u8]);
        buffer.extend(vec![0b11111111u8, 0b11111111u8, 0b01111111u8]);
        buffer.extend(vec![0b01000000u8]);

        let solution = 0b110010001u128;
        let (value, length) = variable_decode(buffer[offset..].as_ref());
        assert_eq!(value.unwrap(), solution, "Test 1 failed!");
        assert_eq!(length, 2, "Test 1 failed!");
        offset += length;

        let solution = 0b00000000u128;
        let (value, length) = variable_decode(buffer[offset..].as_ref());
        assert_eq!(value.unwrap(), solution, "Test 2 failed!");
        assert_eq!(length, 1, "Test 2 failed!");
        offset += length;

        let solution = 0b00011111_11111111_11111111u128;
        let (value, length) = variable_decode(buffer[offset..].as_ref());
        assert_eq!(value.unwrap(), solution, "Test 3 failed!");
        assert_eq!(length, 3, "Test 3 failed!");
        offset += length;

        let solution = 0b01000000u128;
        let (value, length) = variable_decode(buffer[offset..].as_ref());
        assert_eq!(value.unwrap(), solution, "Test 4 failed!");
        assert_eq!(length, 1, "Test 4 failed!");
    }
}
