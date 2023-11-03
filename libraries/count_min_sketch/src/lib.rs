mod count_min_sketch;

pub use count_min_sketch::CMSketch;

#[cfg(test)]
mod tests {
    use super::CMSketch;

    #[test]
    fn count1() {
        let mut cms1 = CMSketch::new(0.001, 0.99999);

        cms1.increase_count(b"video1");
        cms1.increase_count(b"video2");
        cms1.increase_count(b"video3");
        cms1.increase_count(b"video4");
        cms1.increase_count(b"video4");

        assert_eq!(cms1.get_count(b"video1"), 1);
        assert_eq!(cms1.get_count(b"video2"), 1);
        assert_eq!(cms1.get_count(b"video3"), 1);
        assert_eq!(cms1.get_count(b"video4"), 2);
    }

    #[test]
    fn serialize_deserialize() {
        let mut cms1 = CMSketch::new(0.0001, 0.9);

        cms1.increase_count(b"video1");
        cms1.increase_count(b"video2");
        cms1.increase_count(b"video3");
        cms1.increase_count(b"video3");

        let bytes1 = cms1.serialize();

        let cms2 = CMSketch::deserialize(&bytes1);

        let bytes2 = cms2.serialize();

        assert_eq!(bytes1, bytes2);

        assert_eq!(cms2.get_count(b"video1"), 1);
        assert_eq!(cms2.get_count(b"video2"), 1);
        assert_eq!(cms2.get_count(b"video3"), 2);
    }
}
