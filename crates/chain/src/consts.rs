use zebra_chain::parameters::NetworkUpgrade;

/// NU6.3 (Ironwood) activation height on mainnet.
pub const NU6_3_HEIGHT: u32 = 3_428_143;

/// NU6.3 (Ironwood) activation height on the configured network.
pub fn nu6_3_height() -> u32 {
    NetworkUpgrade::Nu6_3
        .activation_height(crate::network())
        .map(|h| h.0)
        .unwrap_or(NU6_3_HEIGHT)
}

/// Zatoshis per ZEC.
pub const COIN: i64 = 100_000_000;

/// Serialized size of a mainnet block header including the Equihash solution:
/// 140-byte header + compactsize(1344) + 1344-byte solution.
pub const BLOCK_HEADER_SIZE: usize = 140 + 3 + 1344;

/// Size of the compactsize encoding of `n`.
pub fn compact_size_len(n: usize) -> usize {
    match n {
        0..=0xfc => 1,
        0xfd..=0xffff => 3,
        0x1_0000..=0xffff_ffff => 5,
        _ => 9,
    }
}
