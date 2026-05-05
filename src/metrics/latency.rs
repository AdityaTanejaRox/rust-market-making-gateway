use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_micros() as i64
}

pub fn now_ms() -> i64 {
    now_us() / 1_000
}