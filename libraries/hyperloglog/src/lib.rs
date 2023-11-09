mod hyperloglog;

pub use hyperloglog::HLL;

#[cfg(test)]
mod tests {
    use super::HLL;

    #[test]
    fn count() {
        let n = 10000;
        let mut hll = HLL::new(10);

        for i in 0..n {
            hll.add_to_count(format!("test{}", i).as_bytes());
        }

        assert!((hll.get_count().abs_diff(n) as f64 / n as f64) < 0.01);
    }

    #[test]
    fn serialize_deserialize() {
        let n = 10000;
        let mut hll1 = HLL::new(10);

        for i in 0..n {
            hll1.add_to_count(format!("test{}", i).as_bytes());
        }

        let bytes1 = hll1.serialize();
        let hll2 = HLL::deserialize(&bytes1);
        let bytes2 = hll2.serialize();

        assert_eq!(bytes1, bytes2);
        assert!((hll2.get_count().abs_diff(n) as f64 / n as f64) < 0.01);
    }
}
