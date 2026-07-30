#![no_main]

use libfuzzer_sys::fuzz_target;
use meshquill_core::protocol::Packet;

fuzz_target!(|data: &[u8]| {
    let _ = Packet::parse(data);
});
