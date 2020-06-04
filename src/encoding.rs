#![allow(dead_code)]

pub struct I64DeltaEncoder {
    output: Vec<u8>,
    prev: i64,
}

impl I64DeltaEncoder {
    pub fn sized_for(num_ints: usize) -> Self {
        Self {
            // Assumes upto 10-bytes for the first integer and assuming 1 byte for all subsequent integers.
            //
            // While this won't always be exact, it should be within a single power of 2 of the result size
            // which will avoid many unnecessary re-allocations without overzealously over-allocatiing.
            output: Vec::with_capacity(num_ints + 10),
            prev: 0,
        }
    }

    pub fn encode(&mut self, value: i64) {
        let mut buffer = [0; 10];
        let signed = value - self.prev;
        let zigzag = (signed << 1) ^ (signed >> 63);
        let encoded = unsigned_varint::encode::u64(zigzag as u64, &mut buffer);
        self.output.extend_from_slice(encoded);
        self.prev = value;
    }

    pub fn finish(self) -> Vec<u8> {
        self.output
    }
}

pub struct I64DeltaDecoder<'a> {
    input: &'a [u8],
    prev: i64,
}

impl<'a> I64DeltaDecoder<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        Self { input, prev: 0 }
    }

    pub fn decode_all(mut self) -> Option<Vec<i64>> {
        let min_results = 1 + (self.input.len() / 10); // results will contain _at least_ one integer per 10 bytes
        let mut results = Vec::with_capacity(min_results);
        while !self.input.is_empty() {
            results.push(self.decode()?);
        }
        Some(results)
    }

    #[inline]
    pub fn decode(&mut self) -> Option<i64> {
        let (head, tail) = unsigned_varint::decode::u64(self.input).ok()?;
        let delta = (head as u64 >> 1) as i64 ^ -((head & 1) as i64);
        let value = self.prev + delta as i64;
        self.input = tail;
        self.prev = value;
        Some(value)
    }
}

pub struct F32Encoder {
    output: Vec<u8>,
    // prev: f32,
}

impl F32Encoder {
    pub fn sized_for(num_floats: usize) -> Self {
        Self {
            output: Vec::with_capacity(num_floats + std::mem::size_of::<f32>()),
            // prev: 0,
        }
    }

    pub fn encode(&mut self, value: f32) {
        self.output.extend_from_slice(&value.to_le_bytes());
        // let mut buffer = [0; 10];
        // let signed = value - self.prev;
        // let zigzag = (signed << 1) ^ (signed >> 63);
        // let encoded = unsigned_varint::encode::u64(zigzag as u64, &mut buffer);
        // encoded);
        // self.prev = value;
    }

    pub fn finish(self) -> Vec<u8> {
        self.output
    }
}

pub struct F32Decoder<'a> {
    input: &'a [u8],
    // prev: f32,
}

impl<'a> F32Decoder<'a> {
    pub fn new(input: &'a [u8]) -> Self {
        assert_eq!(input.len() % std::mem::size_of::<f32>(), 0);
        Self { input }
    }

    pub fn decode_all(self) -> Option<Vec<f32>> {
        let capacity = self.input.len() / std::mem::size_of::<f32>();
        let mut results = Vec::with_capacity(capacity);
        for chunk in self.input.chunks(std::mem::size_of::<f32>()) {
            use std::convert::TryInto;

            let bytes = chunk.try_into().unwrap();
            results.push(f32::from_le_bytes(bytes));
        }
        Some(results)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn i64_delta_encoder_works() {
        let inputs: [&[i64]; 12] = [
            &[0].as_ref(),
            &[0, 0, 0, 0, 0, 0, 0, 0, 0].as_ref(),
            &[1].as_ref(),
            &[1, 1, 1, 1, 1, 1, 1, 1, 1].as_ref(),
            &[-1].as_ref(),
            &[-1, -1, -1, -1, -1, -1, -1, -1, -1].as_ref(),
            &[5].as_ref(),
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10].as_ref(),
            &[0, 1, -2, 3, -4, 5, 0, 0, 0, -9, 1].as_ref(),
            &[i64::MIN].as_ref(),
            &[i64::MAX].as_ref(),
            &[i64::MAX, i64::MAX, i64::MAX].as_ref(),
        ];
        for values in &inputs {
            let mut encoder = I64DeltaEncoder::sized_for(inputs.len());
            for n in *values {
                encoder.encode(*n);
            }

            // Compress the inputs
            let encoded = encoder.finish();
            if values.len() > 1 {
                assert!(encoded.len() < std::mem::size_of::<i64>() * values.len());
            }

            // Ensure that the decoded value matches the input
            let decoded = I64DeltaDecoder::new(&encoded).decode_all().unwrap();
            assert_eq!(&decoded, values);
        }
    }

    #[test]
    fn f32_encoder_works() {
        let inputs: [&[f32]; 1] = [&[-1., 0., 0.5, 15.123].as_ref()];
        for values in &inputs {
            let mut encoder = F32Encoder::sized_for(inputs.len());
            for n in *values {
                encoder.encode(*n);
            }

            // Compress the inputs
            let encoded = encoder.finish();
            if values.len() > 1 {
                assert!(encoded.len() < std::mem::size_of::<i64>() * values.len());
            }

            // Ensure that the decoded value matches the input
            let decoded = F32Decoder::new(&encoded).decode_all().unwrap();
            assert_eq!(&decoded, values);
        }
    }
}
