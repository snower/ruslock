use std::collections::HashMap;

use crate::error::{Result, SlockError};
use crate::protocol::constants::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockResultData {
    raw: Vec<u8>,
}

impl LockResultData {
    pub fn new(raw: Vec<u8>) -> Self {
        Self { raw }
    }

    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    pub fn stage(&self) -> Option<u8> {
        self.raw.get(4).map(|v| v >> 6)
    }

    pub fn command_type(&self) -> Option<u8> {
        self.raw.get(4).map(|v| v & 0x3f)
    }

    pub fn flags(&self) -> Option<u8> {
        self.raw.get(5).copied()
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        let offset = self.value_offset().ok()?;
        if self.raw.len() <= offset {
            None
        } else {
            Some(&self.raw[offset..])
        }
    }

    pub fn as_string(&self) -> Result<String> {
        let offset = self.value_offset()?;
        if self.raw.len() <= offset {
            return Ok(String::new());
        }
        String::from_utf8(self.raw[offset..].to_vec())
            .map_err(|err| SlockError::LockData(format!("invalid utf-8 lock data: {err}")))
    }

    pub fn as_i64(&self) -> i64 {
        let Ok(offset) = self.value_offset() else {
            return 0;
        };
        let mut bytes = [0u8; 8];
        for (index, byte) in bytes.iter_mut().enumerate() {
            if let Some(value) = self.raw.get(offset + index) {
                *byte = *value;
            }
        }
        i64::from_le_bytes(bytes)
    }

    pub fn as_list(&self) -> Result<Vec<Vec<u8>>> {
        if !self.is_array_value() {
            return Ok(Vec::new());
        }
        let mut values = Vec::new();
        let mut index = self.value_offset()?;
        while index + 4 <= self.raw.len() {
            let value_len = self.read_u32(index)? as usize;
            index += 4;
            if value_len == 0 {
                continue;
            }
            if index + value_len > self.raw.len() {
                return Err(SlockError::LockData(
                    "list value length exceeds data".to_string(),
                ));
            }
            values.push(self.raw[index..index + value_len].to_vec());
            index += value_len;
        }
        Ok(values)
    }

    pub fn as_string_list(&self) -> Result<Vec<String>> {
        self.as_list()?
            .into_iter()
            .map(|value| {
                String::from_utf8(value)
                    .map_err(|err| SlockError::LockData(format!("invalid utf-8 list data: {err}")))
            })
            .collect()
    }

    pub fn as_map(&self) -> Result<HashMap<String, Vec<u8>>> {
        if !self.is_kv_value() {
            return Ok(HashMap::new());
        }
        let mut values = HashMap::new();
        let mut index = self.value_offset()?;
        while index + 4 <= self.raw.len() {
            let key_len = self.read_u32(index)? as usize;
            index += 4;
            if key_len == 0 {
                continue;
            }
            if index + key_len > self.raw.len() {
                return Err(SlockError::LockData(
                    "map key length exceeds data".to_string(),
                ));
            }
            let key = String::from_utf8(self.raw[index..index + key_len].to_vec())
                .map_err(|err| SlockError::LockData(format!("invalid utf-8 map key: {err}")))?;
            index += key_len;
            if index + 4 > self.raw.len() {
                return Err(SlockError::LockData("map value length missing".to_string()));
            }
            let value_len = self.read_u32(index)? as usize;
            index += 4;
            if value_len == 0 {
                continue;
            }
            if index + value_len > self.raw.len() {
                return Err(SlockError::LockData(
                    "map value length exceeds data".to_string(),
                ));
            }
            values.insert(key, self.raw[index..index + value_len].to_vec());
            index += value_len;
        }
        Ok(values)
    }

    pub fn as_string_map(&self) -> Result<HashMap<String, String>> {
        self.as_map()?
            .into_iter()
            .map(|(key, value)| {
                String::from_utf8(value)
                    .map(|value| (key, value))
                    .map_err(|err| SlockError::LockData(format!("invalid utf-8 map value: {err}")))
            })
            .collect()
    }

    fn value_offset(&self) -> Result<usize> {
        if self.raw.len() < 6 {
            return Err(SlockError::LockData(
                "lock result data shorter than header".to_string(),
            ));
        }
        if (self.raw[5] & LOCK_DATA_FLAG_CONTAINS_PROPERTY) != 0 {
            if self.raw.len() < 8 {
                return Err(SlockError::LockData(
                    "lock result property length missing".to_string(),
                ));
            }
            let property_len = u16::from_le_bytes([self.raw[6], self.raw[7]]) as usize;
            Ok(8 + property_len)
        } else {
            Ok(6)
        }
    }

    fn is_array_value(&self) -> bool {
        self.raw.len() >= 6
            && self.raw[4] != LOCK_DATA_COMMAND_TYPE_UNSET
            && (self.raw[5] & LOCK_DATA_FLAG_VALUE_TYPE_ARRAY) != 0
    }

    fn is_kv_value(&self) -> bool {
        self.raw.len() >= 6
            && self.raw[4] != LOCK_DATA_COMMAND_TYPE_UNSET
            && (self.raw[5] & LOCK_DATA_FLAG_VALUE_TYPE_KV) != 0
    }

    fn read_u32(&self, index: usize) -> Result<u32> {
        if index + 4 > self.raw.len() {
            return Err(SlockError::LockData("u32 value exceeds data".to_string()));
        }
        Ok(u32::from_le_bytes([
            self.raw[index],
            self.raw[index + 1],
            self.raw[index + 2],
            self.raw[index + 3],
        ]))
    }
}
