use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::thread::yield_now;
use std::env;

const NODE_ID_BITS: u64 = 10;
const SEQUENCE_BITS: u64 = 12;

const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1;

const NODE_ID_SHIFT: u64 = SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u64 = SEQUENCE_BITS + NODE_ID_BITS;

pub struct Snowflake {
    node_id: u64,
    state: AtomicU64, // Packed state: (timestamp << 12) | sequence
}

impl Snowflake {
    pub fn new() -> Self {
        match env::var("Q_NODE_ID") {
            Ok(value) => Self::from(value.parse::<u64>().unwrap_or(0)),
            Err(_) => Self::from(0),
        }
    }

    pub fn generate(&self) -> u64 {
        loop {
            let now = current_timestamp();

            let last = self.state.load(Ordering::Relaxed);
            let last_ts = last >> SEQUENCE_BITS;
            let last_seq = last & MAX_SEQUENCE;

            if now > last_ts {
                let next = (now << SEQUENCE_BITS) | 0;
                if self
                    .state
                    .compare_exchange(last, next, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    return (now << TIMESTAMP_SHIFT) | (self.node_id << NODE_ID_SHIFT);
                }
            } else if now == last_ts {
                let seq = (last_seq + 1) & MAX_SEQUENCE;
                if seq == 0 {
                    // Sequence rollover; wait for next millisecond
                    while current_timestamp() <= now {
                        yield_now(); // avoid busy wait
                    }
                    continue;
                }

                let next = (last_ts << SEQUENCE_BITS) | seq;
                if self
                    .state
                    .compare_exchange(last, next, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    return (now << TIMESTAMP_SHIFT)
                        | (self.node_id << NODE_ID_SHIFT)
                        | seq;
                }
            } else {
                panic!("Clock moved backwards!");
            }
        }
    }
}

impl From<u64> for Snowflake {
    fn from(node_id: u64) -> Self {
        Self {
            node_id,
            state: AtomicU64::new(0),
        }
    }
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before UNIX_EPOCH")
        .as_millis() as u64
}
