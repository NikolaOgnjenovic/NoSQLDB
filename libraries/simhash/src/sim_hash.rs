use phf::{phf_set, Set};
use regex::Regex;
use std::collections::HashMap;
use twox_hash::xxh3::hash64;

static STOP_WORDS: Set<&'static str> = phf_set! {
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
    "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself",
    "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which",
    "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be",
    "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an",
    "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for",
    "with", "about", "against", "between", "into", "through", "during", "before", "after",
    "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under",
    "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all",
    "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not",
    "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don",
    "should", "now"
};

/// Cleans the text and hashes each word into binary from the cleaned text.
fn word_hash(text: &str) -> HashMap<u64, i32> {
    let re: Regex = Regex::new(r"[,;]").unwrap();
    let mut word_hashes: HashMap<u64, i32> = HashMap::new();

    let cleaned_text = re.replace_all(text, "");

    for word in cleaned_text.split_whitespace() {
        let lowercase_word = word.to_lowercase();
        if !STOP_WORDS.contains(&lowercase_word) {
            let hash = hash64(lowercase_word.as_bytes());
            *word_hashes.entry(hash).or_insert(0) += 1;
        }
    }

    word_hashes
}

/// Calculates the simhash of the whole text.
pub fn sim_hash(text: &str) -> u64 {
    let word_hashes = word_hash(text);

    let mut final_simhash = 0u64;

    for bit in 0..64 {
        let mut bit_sum = 0i32;

        for (hash, count) in word_hashes.iter() {
            let bit = (hash >> bit) & 1;
            if bit == 1 {
                bit_sum += *count;
            } else {
                bit_sum -= *count;
            }
        }

        if bit_sum > 0 {
            final_simhash |= 1 << bit;
        }
    }

    final_simhash
}

/// Difference between 2 simhashed texts.
pub fn hamming_distance(data1: &str, data2: &str) -> u8 {
    let simhash1 = sim_hash(data1);
    let simhash2 = sim_hash(data2);

    (simhash1 ^ simhash2).count_ones() as u8
}
