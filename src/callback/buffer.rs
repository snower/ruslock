use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default)]
pub(crate) struct SharedBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reader(&self) -> ReaderBuffer {
        ReaderBuffer {
            inner: self.clone(),
        }
    }

    pub(crate) fn writer(&self) -> WriterBuffer {
        WriterBuffer {
            inner: self.clone(),
        }
    }

    pub(crate) fn push(&self, bytes: &[u8]) {
        self.inner
            .lock()
            .expect("callback buffer mutex poisoned")
            .extend_from_slice(bytes);
    }

    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("callback buffer mutex poisoned")
            .len()
    }

    pub(crate) fn peek(&self, len: usize) -> Option<Vec<u8>> {
        let buffer = self.inner.lock().expect("callback buffer mutex poisoned");
        (buffer.len() >= len).then(|| buffer[..len].to_vec())
    }

    pub(crate) fn peek_range(&self, start: usize, len: usize) -> Option<Vec<u8>> {
        let buffer = self.inner.lock().expect("callback buffer mutex poisoned");
        (buffer.len() >= start + len).then(|| buffer[start..start + len].to_vec())
    }

    pub(crate) fn consume(&self, len: usize) -> Option<Vec<u8>> {
        let mut buffer = self.inner.lock().expect("callback buffer mutex poisoned");
        if buffer.len() < len {
            return None;
        }
        Some(buffer.drain(..len).collect())
    }

    fn drain(&self) -> Vec<u8> {
        let mut buffer = self.inner.lock().expect("callback buffer mutex poisoned");
        buffer.drain(..).collect()
    }

    fn drain_into(&self, out: &mut Vec<u8>) {
        let mut buffer = self.inner.lock().expect("callback buffer mutex poisoned");
        out.extend(buffer.drain(..));
    }

    pub(crate) fn clear(&self) {
        self.inner
            .lock()
            .expect("callback buffer mutex poisoned")
            .clear();
    }
}

#[derive(Clone, Debug, Default)]
pub struct ReaderBuffer {
    inner: SharedBuffer,
}

impl ReaderBuffer {
    /// Creates a standalone reader buffer.
    ///
    /// Most users get this handle from [`crate::callback::Client::reader_buffer`].
    pub fn new() -> Self {
        Self {
            inner: SharedBuffer::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends bytes received by the caller-owned socket.
    pub fn push(&self, bytes: &[u8]) {
        self.inner.push(bytes);
    }

    pub fn clear(&self) {
        self.inner.clear();
    }
}

#[derive(Clone, Debug, Default)]
pub struct WriterBuffer {
    inner: SharedBuffer,
}

impl WriterBuffer {
    /// Creates a standalone writer buffer.
    ///
    /// Most users get this handle from [`crate::callback::Client::writer_buffer`].
    pub fn new() -> Self {
        Self {
            inner: SharedBuffer::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drains bytes that the caller should send through its own socket.
    pub fn drain(&self) -> Vec<u8> {
        self.inner.drain()
    }

    /// Drains outgoing bytes into an existing buffer.
    pub fn drain_into(&self, out: &mut Vec<u8>) {
        self.inner.drain_into(out);
    }

    pub fn clear(&self) {
        self.inner.clear();
    }
}
