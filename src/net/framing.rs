use std::io::{self, Read};

const HEADER_SIZE: usize = 4;
const MAX_FRAME_SIZE: usize = 1024 * 1024;

pub struct FrameDecoder {
    buffer: Vec<u8>,
}
impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }
    pub fn read_from(&mut self, stream: &mut impl Read) -> io::Result<usize> {
        let mut temp = [0u8; 4096];
        let bytes_read = stream.read(&mut temp)?;
        if bytes_read > 0 {
            self.buffer.extend_from_slice(&temp[..bytes_read]);
        }
        Ok(bytes_read)
    }
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }
    pub fn next_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        if self.buffer.len() < HEADER_SIZE {
            return Ok(None);
        }
        let length = u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;

        if length > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame too large",
            ));
        }
        let frame_size = HEADER_SIZE + length;
        if self.buffer.len() < frame_size {
            return Ok(None);
        }
        let payload = self.buffer[HEADER_SIZE..frame_size].to_vec();
        self.buffer.drain(..frame_size);
        Ok(Some(payload))
    }
}

pub fn encode_frame(payload: &[u8]) -> io::Result<Vec<u8>> {
    if payload.len() > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let length = payload.len() as u32;
    let mut frame = Vec::with_capacity(HEADER_SIZE + payload.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn encode_frame_creates_length_prefixed_frame() {
        let payload = b"GET name";

        let frame = encode_frame(payload).unwrap();

        assert_eq!(&frame[..4], &[0, 0, 0, 8]);
        assert_eq!(&frame[4..], payload);
    }

    #[test]
    fn decoder_extracts_complete_frame() {
        let payload = b"GET name";
        let frame = encode_frame(payload).unwrap();

        let mut decoder = FrameDecoder::new();
        let mut stream = Cursor::new(frame);

        decoder.read_from(&mut stream).unwrap();

        let result = decoder.next_frame().unwrap();

        assert_eq!(result, Some(payload.to_vec()));
    }

    #[test]
    fn decoder_waits_for_complete_header() {
        let mut decoder = FrameDecoder::new();

        let mut stream = Cursor::new(vec![0, 0]);

        decoder.read_from(&mut stream).unwrap();

        assert_eq!(decoder.next_frame().unwrap(), None);
    }

    #[test]
    fn decoder_waits_for_complete_payload() {
        let payload = b"GET name";
        let frame = encode_frame(payload).unwrap();

        let partial_frame = &frame[..frame.len() - 2];

        let mut decoder = FrameDecoder::new();
        let mut stream = Cursor::new(partial_frame.to_vec());

        decoder.read_from(&mut stream).unwrap();

        assert_eq!(decoder.next_frame().unwrap(), None);
    }

    #[test]
    fn decoder_handles_partial_frame_across_multiple_reads() {
        let payload = b"GET name";
        let frame = encode_frame(payload).unwrap();

        let first_part = frame[..6].to_vec();
        let second_part = frame[6..].to_vec();

        let mut decoder = FrameDecoder::new();

        let mut first_stream = Cursor::new(first_part);

        decoder.read_from(&mut first_stream).unwrap();

        assert_eq!(decoder.next_frame().unwrap(), None);

        let mut second_stream = Cursor::new(second_part);

        decoder.read_from(&mut second_stream).unwrap();

        assert_eq!(decoder.next_frame().unwrap(), Some(payload.to_vec()));
    }

    #[test]
    fn decoder_handles_multiple_frames_in_one_read() {
        let frame1 = encode_frame(b"SET name Baiju").unwrap();
        let frame2 = encode_frame(b"GET name").unwrap();
        let frame3 = encode_frame(b"DELETE name").unwrap();

        let mut data = Vec::new();

        data.extend_from_slice(&frame1);
        data.extend_from_slice(&frame2);
        data.extend_from_slice(&frame3);

        let mut decoder = FrameDecoder::new();
        let mut stream = Cursor::new(data);

        decoder.read_from(&mut stream).unwrap();

        assert_eq!(
            decoder.next_frame().unwrap(),
            Some(b"SET name Baiju".to_vec())
        );

        assert_eq!(decoder.next_frame().unwrap(), Some(b"GET name".to_vec()));

        assert_eq!(decoder.next_frame().unwrap(), Some(b"DELETE name".to_vec()));

        assert_eq!(decoder.next_frame().unwrap(), None);
    }

    #[test]
    fn decoder_handles_multiple_frames_with_last_one_incomplete() {
        let frame1 = encode_frame(b"SET name Baiju").unwrap();
        let frame2 = encode_frame(b"GET name").unwrap();

        let mut data = Vec::new();

        data.extend_from_slice(&frame1);
        data.extend_from_slice(&frame2[..6]);

        let mut decoder = FrameDecoder::new();
        let mut stream = Cursor::new(data);

        decoder.read_from(&mut stream).unwrap();

        assert_eq!(
            decoder.next_frame().unwrap(),
            Some(b"SET name Baiju".to_vec())
        );

        assert_eq!(decoder.next_frame().unwrap(), None);
    }

    #[test]
    fn decoder_rejects_frame_larger_than_maximum() {
        let length = (MAX_FRAME_SIZE + 1) as u32;

        let header = length.to_be_bytes();

        let mut decoder = FrameDecoder::new();
        let mut stream = Cursor::new(header.to_vec());

        decoder.read_from(&mut stream).unwrap();

        let result = decoder.next_frame();

        assert!(result.is_err());

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn encode_frame_rejects_payload_larger_than_maximum() {
        let payload = vec![b'a'; MAX_FRAME_SIZE + 1];

        let result = encode_frame(&payload);

        assert!(result.is_err());

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn encode_then_decode_round_trip() {
        let payload = b"SET username Baiju";

        let frame = encode_frame(payload).unwrap();

        let mut decoder = FrameDecoder::new();
        let mut stream = Cursor::new(frame);

        decoder.read_from(&mut stream).unwrap();

        let decoded = decoder.next_frame().unwrap().unwrap();

        assert_eq!(decoded, payload);
    }
}
