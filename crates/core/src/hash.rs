//! SHA-256, as the lowercase hex digests every key in a store is named by.

use sha2::{Digest, Sha256};

/// The SHA-256 of `data`, as 64 lowercase hex digits.
pub fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut out = String::with_capacity(64);
    for byte in digest.iter() {
        out.push(char::from_digit(u32::from(byte >> 4), 16).expect("a nibble"));
        out.push(char::from_digit(u32::from(byte & 0xf), 16).expect("a nibble"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_digests() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
