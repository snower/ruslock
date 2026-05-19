use md5::{Digest, Md5};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Key16([u8; 16]);

impl Key16 {
    pub fn new<K: AsRef<[u8]>>(key: K) -> Self {
        let key = key.as_ref();
        if key.len() > 16 {
            let mut hasher = Md5::new();
            hasher.update(key);
            let digest = hasher.finalize();
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&digest);
            Self(bytes)
        } else {
            let mut bytes = [0u8; 16];
            bytes[16 - key.len()..].copy_from_slice(key);
            Self(bytes)
        }
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

impl AsRef<[u8]> for Key16 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
