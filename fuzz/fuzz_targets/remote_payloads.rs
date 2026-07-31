#![no_main]

use libfuzzer_sys::fuzz_target;
use meshquill_core::domain::ControlData;
use meshquill_core::remote::{
    parse_acl_payload, parse_basic_response, parse_neighbour_page, parse_owner_response,
    parse_regions_response, parse_summary_payload, parse_telemetry_payload,
};

fuzz_target!(|data: &[u8]| {
    let Some((&selector, remainder)) = data.split_first() else {
        return;
    };
    let Some((&prefix_seed, payload)) = remainder.split_first() else {
        return;
    };
    let prefix_length = (prefix_seed % 32) + 1;

    match selector % 8 {
        0 => {
            let _ = parse_neighbour_page(payload, prefix_length);
        }
        1 => {
            let _ = parse_regions_response(payload);
        }
        2 => {
            let _ = parse_owner_response(payload);
        }
        3 => {
            let _ = parse_basic_response(payload);
        }
        4 => {
            let _ = parse_acl_payload(payload);
        }
        5 => {
            let _ = parse_telemetry_payload(payload);
        }
        6 => {
            let _ = parse_summary_payload(payload);
        }
        7 => {
            let control = ControlData {
                snr_qdb: 0,
                rssi: 0,
                path_len: 0,
                payload: payload.to_vec(),
            };
            let _ = control.node_discovery_response();
        }
        _ => unreachable!(),
    }
});
