use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ruslock::callback::{ClientHandle, ReplsetClient};
use ruslock::protocol::constants::*;
use ruslock::SlockError;

fn init_response(request: &[u8], init_type: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_INIT;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = COMMAND_RESULT_SUCCED;
    response[20] = init_type;
    response
}

fn lock_response(request: &[u8], result: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = request[2];
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    response[21] = request[20];
    response[22..38].copy_from_slice(&request[21..37]);
    response[38..54].copy_from_slice(&request[37..53]);
    response
}

fn ping_response(request: &[u8], result: u8) -> [u8; 64] {
    let mut response = [0u8; 64];
    response[0] = MAGIC;
    response[1] = VERSION;
    response[2] = COMMAND_TYPE_PING;
    response[3..19].copy_from_slice(&request[3..19]);
    response[19] = result;
    response
}

fn init_node(client: &ruslock::callback::Client, init_type: u8) -> Vec<u8> {
    assert!(!client.handle_init().unwrap());
    let request = client.writer_buffer().drain();
    client
        .reader_buffer()
        .push(&init_response(&request, init_type));
    assert!(client.handle_init().unwrap());
    request
}

#[test]
fn callback_replset_ping_routes_to_leader_and_handle_api_matches() {
    let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    let nodes = replset.node_clients();
    init_node(&nodes[0].client(), 0);
    init_node(&nodes[1].client(), INIT_TYPE_FLAG_IS_LEADER);

    let calls = Arc::new(Mutex::new(0));
    let calls_clone = calls.clone();
    let handle = replset
        .ping(move |result| {
            assert_eq!(result.unwrap().result, COMMAND_RESULT_SUCCED);
            *calls_clone.lock().unwrap() += 1;
        })
        .unwrap();

    assert!(nodes[0].writer_buffer().is_empty());
    let request = nodes[1].writer_buffer().drain();
    assert_eq!(request[2], COMMAND_TYPE_PING);
    assert_eq!(handle.transport().unwrap().node_index(), 1);
    nodes[1]
        .reader_buffer()
        .push(&ping_response(&request, COMMAND_RESULT_SUCCED));
    assert_eq!(nodes[1].client().handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 1);

    let client_handle = ClientHandle::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    assert!(matches!(
        client_handle.ping(|_| {}).unwrap_err(),
        SlockError::NotConnected
    ));
}

#[test]
fn callback_replset_exposes_child_clients_and_routes_to_leader() {
    let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    let nodes = replset.node_clients();
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].index(), 0);
    assert_eq!(nodes[0].address(), "127.0.0.1:5658");
    assert_eq!(nodes[1].index(), 1);
    assert_eq!(nodes[1].address(), "127.0.0.1:5659");

    init_node(&nodes[0].client(), 0);
    init_node(&nodes[1].client(), INIT_TYPE_FLAG_IS_LEADER);
    assert_eq!(replset.lived_nodes(), vec![0, 1]);
    assert_eq!(replset.leader(), Some(1));

    let lock = replset.lock("callback-replset-leader", 3, 4);
    let handle = lock
        .acquire(|result| {
            result.unwrap();
        })
        .unwrap();

    assert!(nodes[0].writer_buffer().is_empty());
    let request = nodes[1].writer_buffer().drain();
    assert_eq!(request[2], COMMAND_TYPE_LOCK);
    let transport = handle.transport().unwrap();
    assert_eq!(transport.node_index(), 1);
    assert_eq!(transport.address(), "127.0.0.1:5659");

    transport
        .reader_buffer()
        .push(&lock_response(&request, COMMAND_RESULT_SUCCED));
    assert_eq!(transport.client().handle_read().unwrap(), 1);
}

#[test]
fn callback_client_handle_selects_replset_and_reports_no_live_node() {
    let handle = ClientHandle::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    assert_eq!(handle.node_clients().len(), 2);

    let lock = handle.lock("callback-replset-no-live", 1, 1);
    let error = lock.acquire(|_| {}).unwrap_err();
    assert!(matches!(error, SlockError::NotConnected));
    assert!(handle
        .node_clients()
        .into_iter()
        .all(|node| node.writer_buffer().is_empty()));
}

#[test]
fn callback_replset_state_error_retries_on_next_live_child_and_updates_transport() {
    let replset = ReplsetClient::new("127.0.0.1:5658,127.0.0.1:5659").unwrap();
    let nodes = replset.node_clients();
    init_node(&nodes[0].client(), INIT_TYPE_FLAG_IS_LEADER);
    init_node(&nodes[1].client(), 0);

    let calls = Arc::new(Mutex::new(0));
    let calls_clone = calls.clone();
    let lock = replset.lock("callback-replset-state-error", 3, 4);
    let handle = lock
        .acquire(move |result| {
            result.unwrap();
            *calls_clone.lock().unwrap() += 1;
        })
        .unwrap();

    let first = nodes[0].writer_buffer().drain();
    assert_eq!(handle.transport().unwrap().node_index(), 0);
    nodes[0]
        .reader_buffer()
        .push(&lock_response(&first, COMMAND_RESULT_STATE_ERROR));
    assert_eq!(nodes[0].client().handle_read().unwrap(), 1);

    let second_transport = handle.transport().unwrap();
    assert_eq!(second_transport.node_index(), 1);
    let second = second_transport.writer_buffer().drain();
    assert_eq!(second[2], COMMAND_TYPE_LOCK);

    nodes[0]
        .reader_buffer()
        .push(&lock_response(&first, COMMAND_RESULT_SUCCED));
    assert_eq!(nodes[0].client().handle_read().unwrap(), 0);
    assert_eq!(*calls.lock().unwrap(), 0);

    second_transport
        .reader_buffer()
        .push(&lock_response(&second, COMMAND_RESULT_SUCCED));
    assert_eq!(second_transport.client().handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[test]
fn callback_replset_falls_back_to_first_live_child_without_leader() {
    let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    let nodes = replset.node_clients();
    init_node(&nodes[0].client(), 0);
    init_node(&nodes[1].client(), 0);
    assert_eq!(replset.leader(), None);

    let lock = replset.lock("callback-replset-fallback", 1, 1);
    let handle = lock
        .acquire(|result| {
            result.unwrap();
        })
        .unwrap();
    assert_eq!(handle.transport().unwrap().node_index(), 0);
    assert!(!nodes[0].writer_buffer().is_empty());
    assert!(nodes[1].writer_buffer().is_empty());
}

#[test]
fn callback_replset_disconnect_retries_on_another_live_child() {
    let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    let nodes = replset.node_clients();
    init_node(&nodes[0].client(), INIT_TYPE_FLAG_IS_LEADER);
    init_node(&nodes[1].client(), 0);

    let calls = Arc::new(Mutex::new(0));
    let calls_clone = calls.clone();
    let lock = replset.lock("callback-replset-disconnect", 1, 1);
    let handle = lock
        .acquire(move |result| {
            result.unwrap();
            *calls_clone.lock().unwrap() += 1;
        })
        .unwrap();

    assert_eq!(handle.transport().unwrap().node_index(), 0);
    assert!(nodes[0].client().handle_disconnect().unwrap());
    assert_eq!(replset.leader(), None);
    let transport = handle.transport().unwrap();
    assert_eq!(transport.node_index(), 1);
    let retry = transport.writer_buffer().drain();
    transport
        .reader_buffer()
        .push(&lock_response(&retry, COMMAND_RESULT_SUCCED));
    assert_eq!(transport.client().handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[test]
fn callback_replset_cancel_suppresses_late_response() {
    let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    let nodes = replset.node_clients();
    init_node(&nodes[0].client(), INIT_TYPE_FLAG_IS_LEADER);
    init_node(&nodes[1].client(), 0);

    let calls = Arc::new(Mutex::new(0));
    let calls_clone = calls.clone();
    let lock = replset.lock("callback-replset-cancel", 1, 1);
    let handle = lock
        .acquire(move |_| {
            *calls_clone.lock().unwrap() += 1;
        })
        .unwrap();
    let request = nodes[0].writer_buffer().drain();
    assert!(handle.cancel().unwrap());
    assert_eq!(nodes[0].client().pending_len(), 0);
    nodes[0]
        .reader_buffer()
        .push(&lock_response(&request, COMMAND_RESULT_SUCCED));
    assert_eq!(nodes[0].client().handle_read().unwrap(), 0);
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[test]
fn callback_replset_tree_leaf_continuation_keeps_one_callback_across_retry() {
    let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]).unwrap();
    let nodes = replset.node_clients();
    init_node(&nodes[0].client(), INIT_TYPE_FLAG_IS_LEADER);
    init_node(&nodes[1].client(), 0);

    let calls = Arc::new(Mutex::new(0));
    let calls_clone = calls.clone();
    let leaf = replset
        .tree_lock("callback-replset-tree-root", 1, 1)
        .load_child("callback-replset-tree-child")
        .new_leaf_lock();
    let handle = leaf
        .acquire(move |result| {
            result.unwrap();
            *calls_clone.lock().unwrap() += 1;
        })
        .unwrap();

    let first = nodes[0].writer_buffer().drain();
    nodes[0]
        .reader_buffer()
        .push(&lock_response(&first, COMMAND_RESULT_STATE_ERROR));
    assert_eq!(nodes[0].client().handle_read().unwrap(), 1);
    assert_eq!(handle.transport().unwrap().node_index(), 1);

    let child_retry = nodes[1].writer_buffer().drain();
    nodes[1]
        .reader_buffer()
        .push(&lock_response(&child_retry, COMMAND_RESULT_SUCCED));
    assert_eq!(nodes[1].client().handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 0);

    let parent_check_transport = handle.transport().unwrap();
    assert_eq!(parent_check_transport.node_index(), 0);
    let parent_check = parent_check_transport.writer_buffer().drain();
    parent_check_transport
        .reader_buffer()
        .push(&lock_response(&parent_check, COMMAND_RESULT_SUCCED));
    assert_eq!(parent_check_transport.client().handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 0);

    let leaf_acquire_transport = handle.transport().unwrap();
    let leaf_acquire = leaf_acquire_transport.writer_buffer().drain();
    leaf_acquire_transport
        .reader_buffer()
        .push(&lock_response(&leaf_acquire, COMMAND_RESULT_SUCCED));
    assert_eq!(leaf_acquire_transport.client().handle_read().unwrap(), 1);
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[test]
fn callback_replset_timeout_fails_operation_once_and_clears_child_pending() {
    let options = ruslock::ClientOptions {
        command_timeout_grace: Duration::from_secs(1),
        ..Default::default()
    };
    let replset =
        ReplsetClient::with_options(["127.0.0.1:5658", "127.0.0.1:5659"], options).unwrap();
    let nodes = replset.node_clients();
    init_node(&nodes[0].client(), INIT_TYPE_FLAG_IS_LEADER);
    init_node(&nodes[1].client(), 0);

    let error = Arc::new(Mutex::new(None));
    let error_clone = error.clone();
    let lock = replset.lock("callback-replset-timeout", 1, 1);
    lock.acquire(move |result| {
        *error_clone.lock().unwrap() = Some(result.unwrap_err().to_string());
    })
    .unwrap();
    assert_eq!(nodes[0].client().pending_len(), 1);
    let deadline = replset.next_deadline().unwrap();
    assert!(deadline > Instant::now());
    assert_eq!(replset.handle_timeout(deadline).unwrap(), 1);
    assert_eq!(nodes[0].client().pending_len(), 0);
    assert_eq!(error.lock().unwrap().as_deref(), Some("command timeout"));
}
