//! Hash byte-order helpers. lightwalletd uses internal (little-endian) byte order;
//! explorers display hashes reversed.

/// Internal-order bytes to display-order hex.
pub fn to_display(bytes: &[u8]) -> String {
    let mut b = bytes.to_vec();
    b.reverse();
    hex::encode(b)
}

/// Display-order hex to internal-order bytes. Returns `None` for malformed input.
pub fn from_display(s: &str) -> Option<Vec<u8>> {
    let mut b = hex::decode(s).ok()?;
    if b.len() != 32 {
        return None;
    }
    b.reverse();
    Some(b)
}

pub fn is_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let d = "39d1e0130e5f8381c6960cdf6e0930ea9351f3fd1b405243ebf3561d733b294c";
        assert_eq!(to_display(&from_display(d).unwrap()), d);
    }
}
