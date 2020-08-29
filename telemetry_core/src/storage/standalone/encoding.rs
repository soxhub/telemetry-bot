#![allow(dead_code)]

pub struct U32VarintEncoder {
    output: Vec<u8>,
}

impl U32VarintEncoder {
    pub fn sized_for(num_ints: usize) -> Self {
        Self {
            // Ballpark the memory we'll need; to avoid excess re-allocations
            output: Vec::with_capacity(num_ints * 2),
        }
    }

    pub fn encode(&mut self, value: u32) {
        let mut buffer = [0; 5];
        let encoded = unsigned_varint::encode::u32(value, &mut buffer);
        self.output.extend_from_slice(encoded);
    }

    pub fn finish(self) -> Vec<u8> {
        self.output
    }
}

pub struct U32VarintDecoder<'a> {
    input: &'a [u8],
}

impl<'a> U32VarintDecoder<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input }
    }

    pub fn decode_all(mut self) -> Option<Vec<u32>> {
        let min_results = 1 + (self.input.len() / 5); // results will contain _at least_ one integer per 5 bytes
        let mut results = Vec::with_capacity(min_results);
        while !self.input.is_empty() {
            results.push(self.decode()?);
        }
        Some(results)
    }

    #[inline]
    pub fn decode(&mut self) -> Option<u32> {
        let (head, tail) = unsigned_varint::decode::u32(self.input).ok()?;
        self.input = tail;
        Some(head)
    }
}

pub struct U32ZigZagEncoder {
    output: Vec<u8>,
    prev: u32,
}

impl U32ZigZagEncoder {
    pub fn sized_for(num_ints: usize) -> Self {
        Self {
            // Assumes upto 5-bytes for the first integer and assuming 1 byte for all subsequent integers.
            //
            // While this won't always be exact, it should be within a single power of 2 of the result size
            // which will avoid many unnecessary re-allocations without overzealously over-allocatiing.
            output: Vec::with_capacity(num_ints + 5),
            prev: 0,
        }
    }

    pub fn encode(&mut self, value: u32) {
        let mut buffer = [0; 5];
        let signed = value as i32 - self.prev as i32;
        let zigzag = (signed << 1) ^ (signed >> 31);
        let encoded = unsigned_varint::encode::u32(zigzag as u32, &mut buffer);
        self.output.extend_from_slice(encoded);
        self.prev = value;
    }

    pub fn finish(self) -> Vec<u8> {
        self.output
    }
}

pub struct U32ZigZagDecoder<'a> {
    input: &'a [u8],
    prev: u32,
}

impl<'a> U32ZigZagDecoder<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, prev: 0 }
    }

    pub fn decode_all(mut self) -> Option<Vec<u32>> {
        let min_results = 1 + (self.input.len() / 10); // results will contain _at least_ one integer per 10 bytes
        let mut results = Vec::with_capacity(min_results);
        while !self.input.is_empty() {
            results.push(self.decode()?);
        }
        Some(results)
    }

    #[inline]
    pub fn decode(&mut self) -> Option<u32> {
        let (head, tail) = unsigned_varint::decode::u32(self.input).ok()?;
        let delta = (head as u32 >> 1) as i32 ^ -((head & 1) as i32);
        let value = (self.prev as i32 + delta) as u32;
        self.input = tail;
        self.prev = value;
        Some(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn u32_varint_encoder_works() {
        let inputs: [&[u32]; 9] = [
            &[0].as_ref(),
            &[0, 0, 0, 0, 0, 0, 0, 0, 0].as_ref(),
            &[1].as_ref(),
            &[1, 1, 1, 1, 1, 1, 1, 1, 1].as_ref(),
            &[5].as_ref(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10].as_ref(),
            &[u32::MIN].as_ref(),
            &[u32::MAX].as_ref(),
            &[u32::MAX, u32::MAX, u32::MAX].as_ref(),
        ];
        for values in &inputs {
            let mut encoder = U32VarintEncoder::sized_for(inputs.len());
            for n in *values {
                encoder.encode(*n);
            }

            // Compress the inputs
            let encoded = encoder.finish();

            // NOTE: Encoding of MAX-range values will cause compressed values to be large than expected
            // if values.len() > 1 {
            //     assert!(encoded.len() < std::mem::size_of::<u32>() * values.len());
            // }

            // Ensure that the decoded value matches the input
            let decoded = U32VarintDecoder::new(&encoded).decode_all().unwrap();
            assert_eq!(&decoded, values);
        }
    }

    #[test]
    fn u32_zigzag_encoder_works() {
        let inputs: [&[u32]; 9] = [
            &[0].as_ref(),
            &[0, 0, 0, 0, 0, 0, 0, 0, 0].as_ref(),
            &[1].as_ref(),
            &[1, 1, 1, 1, 1, 1, 1, 1, 1].as_ref(),
            &[5].as_ref(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10].as_ref(),
            &[u32::MIN].as_ref(),
            &[u32::MAX].as_ref(),
            &[u32::MAX, u32::MAX, u32::MAX].as_ref(),
        ];
        for values in &inputs {
            let mut encoder = U32ZigZagEncoder::sized_for(inputs.len());
            for n in *values {
                encoder.encode(*n);
            }

            // Compress the inputs
            let encoded = encoder.finish();
            if values.len() > 1 {
                assert!(encoded.len() < std::mem::size_of::<u32>() * values.len());
            }

            // Ensure that the decoded value matches the input
            let decoded = U32ZigZagDecoder::new(&encoded).decode_all().unwrap();
            assert_eq!(&decoded, values);
        }
    }
}
