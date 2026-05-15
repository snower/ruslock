#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PackedTime(u32);

impl PackedTime {
    pub const fn new(value: u16) -> Self {
        Self(value as u32)
    }

    pub const fn with_flags(value: u16, flags: u16) -> Self {
        Self(((flags as u32) << 16) | value as u32)
    }

    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    pub const fn value(self) -> u16 {
        (self.0 & 0xffff) as u16
    }

    pub const fn flags(self) -> u16 {
        (self.0 >> 16) as u16
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn merge_flags(self, flags: u16) -> Self {
        Self::with_flags(self.value(), self.flags() | flags)
    }
}
