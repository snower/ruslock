use ruslock::callback::{Client, ReaderBuffer};

#[test]
fn reader_buffer_pushes_and_clones_share_storage() {
    let reader = ReaderBuffer::new();
    let clone = reader.clone();

    reader.push(b"ab");
    clone.push(b"cd");

    assert_eq!(reader.len(), 4);
    assert_eq!(clone.len(), 4);
    reader.clear();
    assert!(clone.is_empty());
}

#[test]
fn writer_buffer_drains_fifo_and_into_existing_vec() {
    let client = Client::new();
    let writer = client.writer_buffer();
    let clone = writer.clone();

    client.handle_init().unwrap();
    assert_eq!(writer.len(), 64);

    let mut out = vec![b'x'];
    writer.drain_into(&mut out);
    assert_eq!(out.len(), 65);
    assert_eq!(out[0], b'x');
    assert!(clone.is_empty());

    client.handle_init().unwrap();
    assert_eq!(writer.drain().len(), 0);
    assert!(writer.is_empty());
}
