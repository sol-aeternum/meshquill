use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::error::{CoreError, ParseError};

/// Maximum payload allowed by outer serial/TCP frame wrappers.
pub const MAX_OUTER_PAYLOAD: usize = 300;
const APP_FRAME_PREFIX: u8 = 0x3c;
const DEVICE_FRAME_PREFIX: u8 = 0x3e;
const LENGTH_SIZE: usize = 2;

/// Inner-frame payload emitted by transport wrappers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OutboundFrame {
    /// Message payload without marker and length bytes.
    pub payload: Vec<u8>,
}

impl OutboundFrame {
    /// Builds a valid outbound inner payload.
    ///
    /// # Errors
    /// Returns `CoreError::Parse(ParseError::OversizedPacketPayload)` when the payload exceeds
    /// protocol limits.
    pub fn new(payload: Vec<u8>) -> Result<Self, CoreError> {
        if payload.len() > MAX_OUTER_PAYLOAD {
            return Err(CoreError::Parse(ParseError::OversizedPacketPayload {
                actual: payload.len(),
                maximum: MAX_OUTER_PAYLOAD,
            }));
        }
        Ok(Self { payload })
    }

    /// Extracts raw payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// Inner-frame payload received from the companion stream.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InboundFrame {
    /// Message payload without marker and length bytes.
    pub payload: Vec<u8>,
}

impl InboundFrame {
    /// Builds a validated inbound frame payload.
    #[must_use]
    pub fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    /// Extracts raw payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// Writes app->device outer envelopes with prefix `0x3c`.
#[derive(Default)]
pub struct OuterEncoder;

impl Encoder<OutboundFrame> for OuterEncoder {
    type Error = CoreError;

    fn encode(&mut self, item: OutboundFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload_len = u16::try_from(item.payload.len()).map_err(|_| {
            CoreError::Parse(ParseError::OversizedPacketPayload {
                actual: item.payload.len(),
                maximum: MAX_OUTER_PAYLOAD,
            })
        })?;
        if item.payload.len() > MAX_OUTER_PAYLOAD {
            return Err(CoreError::Parse(ParseError::OversizedPacketPayload {
                actual: item.payload.len(),
                maximum: MAX_OUTER_PAYLOAD,
            }));
        }

        dst.reserve(LENGTH_SIZE + item.payload.len() + 1);
        dst.put_u8(APP_FRAME_PREFIX);
        dst.put_u16_le(payload_len);
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}

/// Reads device->app outer envelopes with prefix `0x3e`.
#[derive(Default)]
pub struct OuterDecoder;

impl OuterDecoder {
    fn find_device_prefix(src: &BytesMut) -> Option<usize> {
        src.iter().position(|value| value == &DEVICE_FRAME_PREFIX)
    }
}

impl Decoder for OuterDecoder {
    type Item = InboundFrame;
    type Error = CoreError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let prefix_pos = if src[0] == DEVICE_FRAME_PREFIX {
            0
        } else {
            Self::find_device_prefix(src).unwrap_or(src.len())
        };

        if prefix_pos > 0 && prefix_pos < src.len() {
            src.advance(prefix_pos);
        }

        if prefix_pos >= src.len() {
            src.advance(src.len());
            return Ok(None);
        }

        if src.len() < 1 + LENGTH_SIZE {
            return Ok(None);
        }

        let declared_len = usize::from(u16::from_le_bytes([src[1], src[2]]));
        if declared_len > MAX_OUTER_PAYLOAD {
            let total = 1 + LENGTH_SIZE + declared_len;
            if src.len() >= total {
                src.advance(total);
            } else {
                src.advance(src.len());
            }

            return Err(CoreError::Parse(ParseError::OversizedPacketPayload {
                actual: declared_len,
                maximum: MAX_OUTER_PAYLOAD,
            }));
        }

        let frame_total = 1 + LENGTH_SIZE + declared_len;
        if src.len() < frame_total {
            return Ok(None);
        }

        let payload = src[3..frame_total].to_vec();
        src.advance(frame_total);
        Ok(Some(InboundFrame::new(payload)))
    }
}

/// Encodes a raw payload as a serial/TCP app->device frame.
///
/// # Errors
/// Returns `CoreError::Parse` if the payload is oversized.
pub fn encode_payload(payload: &[u8]) -> Result<Vec<u8>, CoreError> {
    let frame = OutboundFrame::new(payload.to_vec())?;
    let mut out = BytesMut::new();
    let mut encoder = OuterEncoder;
    encoder.encode(frame, &mut out)?;
    Ok(out.to_vec())
}

/// Decodes concatenated raw outer-frame bytes into one or more payloads.
///
/// # Errors
/// Returns `CoreError::Parse` when framing is invalid or payload limits are violated.
pub fn decode_frames(raw: &[u8]) -> Result<Vec<InboundFrame>, CoreError> {
    let mut src = BytesMut::from(raw);
    let mut decoder = OuterDecoder;
    let mut out = Vec::new();

    while !src.is_empty() {
        match decoder.decode(&mut src)? {
            Some(frame) => out.push(frame),
            None => break,
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_little_endian_app_frame() {
        let encoded = match encode_payload(&[1, 2, 3]) {
            Ok(encoded) => encoded,
            Err(error) => panic!("frame should encode: {error}"),
        };
        assert_eq!(encoded, vec![APP_FRAME_PREFIX, 3, 0, 1, 2, 3]);
    }

    #[test]
    fn decoder_resynchronizes_and_reads_concatenated_frames() {
        let raw = [
            0x99,
            DEVICE_FRAME_PREFIX,
            2,
            0,
            1,
            2,
            DEVICE_FRAME_PREFIX,
            1,
            0,
            3,
        ];
        let frames = match decode_frames(&raw) {
            Ok(frames) => frames,
            Err(error) => panic!("frames should decode: {error}"),
        };
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload, vec![1, 2]);
        assert_eq!(frames[1].payload, vec![3]);
    }

    #[test]
    fn decoder_preserves_partial_frame_until_completed() {
        let mut decoder = OuterDecoder;
        let mut bytes = BytesMut::from(&[DEVICE_FRAME_PREFIX, 2, 0, 7][..]);
        assert!(matches!(decoder.decode(&mut bytes), Ok(None)));
        assert_eq!(bytes.as_ref(), &[DEVICE_FRAME_PREFIX, 2, 0, 7]);
        bytes.extend_from_slice(&[8]);
        assert!(matches!(
            decoder.decode(&mut bytes),
            Ok(Some(InboundFrame { payload })) if payload == vec![7, 8]
        ));
    }

    #[test]
    fn encoder_and_decoder_reject_oversized_lengths() {
        assert!(matches!(
            encode_payload(&vec![0_u8; MAX_OUTER_PAYLOAD + 1]),
            Err(CoreError::Parse(ParseError::OversizedPacketPayload { .. }))
        ));

        let declared = u16::try_from(MAX_OUTER_PAYLOAD + 1).unwrap_or(u16::MAX);
        let mut bytes = BytesMut::from(
            &[
                DEVICE_FRAME_PREFIX,
                declared.to_le_bytes()[0],
                declared.to_le_bytes()[1],
            ][..],
        );
        let mut decoder = OuterDecoder;
        assert!(matches!(
            decoder.decode(&mut bytes),
            Err(CoreError::Parse(ParseError::OversizedPacketPayload { .. }))
        ));
        assert!(bytes.is_empty());
    }

    #[test]
    fn decoder_retains_the_defensive_300_byte_declared_frame_bound() {
        let accepted_length = u16::try_from(MAX_OUTER_PAYLOAD)
            .expect("test length")
            .to_le_bytes();
        let mut accepted = vec![DEVICE_FRAME_PREFIX, accepted_length[0], accepted_length[1]];
        accepted.extend_from_slice(&[0_u8; MAX_OUTER_PAYLOAD]);
        let frames = decode_frames(&accepted).expect("300-byte declared frame remains decodable");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload.len(), MAX_OUTER_PAYLOAD);

        let rejected_len = MAX_OUTER_PAYLOAD + 1;
        let length = u16::try_from(rejected_len)
            .expect("test length")
            .to_le_bytes();
        let mut rejected = vec![DEVICE_FRAME_PREFIX, length[0], length[1]];
        rejected.extend_from_slice(&[0_u8; MAX_OUTER_PAYLOAD + 1]);
        assert!(matches!(
            decode_frames(&rejected),
            Err(CoreError::Parse(ParseError::OversizedPacketPayload {
                actual,
                maximum: MAX_OUTER_PAYLOAD,
            })) if actual == rejected_len
        ));
    }
}
