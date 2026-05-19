use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::RngCore;

static ID_COUNTER: AtomicU32 = AtomicU32::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Id16([u8; 16]);

impl Id16 {
    pub fn new() -> Self {
        let mut bytes = [0u8; 16];
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        bytes[0] = (timestamp >> 40) as u8;
        bytes[1] = (timestamp >> 32) as u8;
        bytes[2] = (timestamp >> 24) as u8;
        bytes[3] = (timestamp >> 16) as u8;
        bytes[4] = (timestamp >> 8) as u8;
        bytes[5] = timestamp as u8;

        let rand_number = rand::thread_rng().next_u64();
        bytes[6] = (rand_number >> 40) as u8;
        bytes[7] = (rand_number >> 32) as u8;
        bytes[8] = (rand_number >> 24) as u8;
        bytes[9] = (rand_number >> 16) as u8;
        bytes[10] = (rand_number >> 8) as u8;
        bytes[11] = rand_number as u8;

        let counter = ID_COUNTER.fetch_add(1, Ordering::Relaxed).wrapping_add(1) & 0x7fff_ffff;
        bytes[12] = (counter >> 24) as u8;
        bytes[13] = (counter >> 16) as u8;
        bytes[14] = (counter >> 8) as u8;
        bytes[15] = counter as u8;
        Self(bytes)
    }

    pub const fn zero() -> Self {
        Self([0; 16])
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl Default for Id16 {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[u8]> for Id16 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
