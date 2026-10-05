//! Stream codes on the wire (TA-27). moq-relay reads and writes RESET_STREAM and STOP_SENDING codes
//! on native QUIC in the HTTP/3 encoding draft-ietf-webtrans-http3 gives WebTransport, so a reset
//! or a stop goes out encoded, and a peer's code in the encoding is decoded. Close codes are not
//! mapped.

use quinn::VarInt;

/// Where the encoding puts code 0.
const FIRST: u64 = 0x52e4_a40f_a8db;
/// Where it puts 2³²−1, the last code.
const LAST: u64 = 0x52e5_ac98_3162;

/// The wire code for a reset or a stop with `code`: one GREASE slot is skipped per 30 codes.
pub fn to_wire(code: u32) -> VarInt {
    let code = u64::from(code);
    VarInt::from_u64(FIRST + code + code / 0x1e).expect("the encoding stays below 2⁶²")
}

/// The peer's code for a reset or a stop that carried `wire`: decoded when it is in the encoding,
/// nothing on a GREASE slot, and as it arrived otherwise.
pub fn from_wire(wire: VarInt) -> Option<u64> {
    let wire = wire.into_inner();
    if !(FIRST..=LAST).contains(&wire) {
        return Some(wire);
    }
    let shifted = wire - FIRST;
    if shifted % 0x1f == 0x1e {
        return None;
    }
    Some(shifted - shifted / 0x1f)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_code_goes_out_in_the_encoding() {
        assert_eq!(to_wire(0).into_inner(), 0x52e4_a40f_a8db);
        // What moq-relay resets a forwarded subgroup with (#248).
        assert_eq!(to_wire(1).into_inner(), 0x52e4_a40f_a8dc);
        assert_eq!(to_wire(0x1d).into_inner(), FIRST + 0x1d);
        assert_eq!(to_wire(0x1e).into_inner(), FIRST + 0x1f);
        assert_eq!(to_wire(u32::MAX).into_inner(), LAST);
    }

    #[test]
    fn an_encoded_code_decodes_back() {
        for code in [0, 1, 7, 0x12, 0x1d, 0x1e, 0x1f, 12345, u32::MAX] {
            assert_eq!(from_wire(to_wire(code)), Some(u64::from(code)));
        }
    }

    #[test]
    fn a_grease_slot_carries_no_code() {
        for slot in [FIRST + 0x1e, FIRST + 0x1e + 0x1f] {
            assert_eq!(from_wire(VarInt::from_u64(slot).unwrap()), None);
        }
    }

    #[test]
    fn a_code_outside_the_encoding_is_the_peers_as_it_is() {
        for code in [
            0,
            11,
            u64::from(u32::MAX),
            FIRST - 1,
            LAST + 1,
            VarInt::MAX.into_inner(),
        ] {
            assert_eq!(from_wire(VarInt::from_u64(code).unwrap()), Some(code));
        }
    }
}
