mod sim_hash;

pub use sim_hash::{hamming_distance, sim_hash};

#[cfg(test)]
mod tests {
    use super::hamming_distance;
    use super::sim_hash;

    #[test]
    fn test_value() {
        let text1 = "ABCD";

        let hashed = sim_hash(text1);

        assert_eq!(hashed, 10927344704649967500);
    }

    #[test]
    fn test_similarity1() {
        let text1 = "Identical text";
        let text2 = "Identical text";

        let hamming_distance = hamming_distance(text1, text2);

        assert_eq!(hamming_distance, 0);
    }

    #[test]
    fn test_similarity2() {
        let text1 = "Identical text but not actually identical only with a slight difference";
        let text2 = "Identical text but not actually identical only with difference";

        let hamming_distance = hamming_distance(text1, text2);

        assert!(hamming_distance > 0);
    }

    #[test]
    fn test_similarity3() {
        let text1 = "Identical     ,,text but not actually ,,identical only with punctuation";
        let text2 = "Identical text but not actually identical only  ;;with punctuation,;;";

        let hamming_distance = hamming_distance(text1, text2);

        assert_eq!(hamming_distance, 0);
    }

    #[test]
    fn test_similarity4() {
        let text1 = "Completely different for testing purposes";
        let text2 = "cOMPLETELY DIFFERENT FOR TESTING PURPOSES";

        let hamming_distance = hamming_distance(text1, text2);

        assert_eq!(hamming_distance, 0);
    }

    #[test]
    fn test_difference1() {
        let text1 = "Completely different for testing purposes";
        let text2 = "Identical text but not actually identical only with some difference";

        let hamming_distance = hamming_distance(text1, text2);

        assert!(hamming_distance > 15);
    }
}
