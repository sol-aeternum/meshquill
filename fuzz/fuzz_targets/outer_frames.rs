#![no_main]

use libfuzzer_sys::fuzz_target;
use meshquill_core::framing::decode_frames;

fuzz_target!(|data: &[u8]| {
    let _ = decode_frames(data);
});
