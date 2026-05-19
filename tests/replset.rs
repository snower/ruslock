#[cfg(feature = "blocking")]
#[test]
fn blocking_replset_parses_comma_strings_and_arrays() {
    let from_string = ruslock::blocking::ReplsetClient::new("127.0.0.1:5658,127.0.0.1:5659");
    assert_eq!(from_string.nodes(), &["127.0.0.1:5658", "127.0.0.1:5659"]);

    let from_array = ruslock::blocking::ReplsetClient::new(["127.0.0.1:5658"]);
    assert_eq!(from_array.nodes(), &["127.0.0.1:5658"]);
}

#[cfg(feature = "aio")]
#[test]
fn async_replset_parses_comma_strings_and_arrays() {
    let from_string = ruslock::aio::ReplsetClient::new("127.0.0.1:5658,127.0.0.1:5659");
    assert_eq!(from_string.nodes(), &["127.0.0.1:5658", "127.0.0.1:5659"]);

    let from_array = ruslock::aio::ReplsetClient::new(["127.0.0.1:5658"]);
    assert_eq!(from_array.nodes(), &["127.0.0.1:5658"]);
}
