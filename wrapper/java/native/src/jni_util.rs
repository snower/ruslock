//! Small JNI value conversion helpers.

use jni::objects::{JByteArray, JObject, JObjectArray, JString};
use jni::sys::jbyteArray;
use jni::JNIEnv;

pub(crate) fn string(env: &mut JNIEnv, value: JString) -> Result<String, String> {
    env.get_string(&value)
        .map(String::from)
        .map_err(|err| format!("invalid Java string argument: {err}"))
}

/// Converts a Java `String[]` into owned node addresses.
pub(crate) fn string_array(
    env: &mut JNIEnv,
    value: JObjectArray,
) -> Result<Vec<String>, String> {
    if value.as_raw().is_null() {
        return Err("node array must not be null".to_string());
    }
    let len = env
        .get_array_length(&value)
        .map_err(|err| format!("invalid Java string array argument: {err}"))?;
    let mut nodes = Vec::with_capacity(len.max(0) as usize);
    for index in 0..len {
        let element: JObject = env
            .get_object_array_element(&value, index)
            .map_err(|err| format!("invalid Java node element at index {index}: {err}"))?;
        if element.as_raw().is_null() {
            return Err(format!("node array element {index} is null"));
        }
        let node = string(env, JString::from(element))?;
        let node = node.trim();
        if node.is_empty() {
            return Err(format!("node array element {index} is empty"));
        }
        nodes.push(node.to_string());
    }
    Ok(nodes)
}

pub(crate) fn bytes(env: &JNIEnv, value: JByteArray) -> Result<Vec<u8>, String> {
    env.convert_byte_array(value)
        .map_err(|err| format!("invalid Java byte array argument: {err}"))
}

pub(crate) fn optional_bytes(
    env: &JNIEnv,
    value: JByteArray,
) -> Result<Option<Vec<u8>>, String> {
    if value.as_raw().is_null() {
        Ok(None)
    } else {
        Ok(Some(bytes(env, value)?))
    }
}

pub(crate) fn to_byte_array(env: &JNIEnv, value: &[u8]) -> Result<jbyteArray, String> {
    env.byte_array_from_slice(value)
        .map(|array| array.into_raw())
        .map_err(|err| format!("failed to allocate a Java byte array: {err}"))
}
