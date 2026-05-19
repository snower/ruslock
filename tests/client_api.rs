#[cfg(feature = "blocking")]
mod blocking_api {
    use ruslock::blocking::{
        Client, ClientApi, ClientHandle, Database, Lock, ReplsetClient, TreeLock,
    };
    use ruslock::Result;

    fn build_business_primitives<C: ClientApi>(client: &C) -> (Database, Lock, TreeLock) {
        let db = client.select_database(255);
        let lock = client.lock("api-lock", 1, 1);
        let tree = client.tree_lock("api-tree", 1, 1);
        (db, lock, tree)
    }

    #[test]
    fn blocking_client_replset_and_handle_share_business_api() {
        let client = Client::new("127.0.0.1:5658");
        let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]);
        let single_handle = ClientHandle::new("127.0.0.1:5658");
        let replset_handle = ClientHandle::new("127.0.0.1:5658,127.0.0.1:5659");

        assert_eq!(build_business_primitives(&client).0.db_id(), 255);
        assert_eq!(build_business_primitives(&replset).0.db_id(), 255);
        assert_eq!(build_business_primitives(&single_handle).0.db_id(), 255);
        assert_eq!(build_business_primitives(&replset_handle).0.db_id(), 255);

        let _: Result<Database> = Ok(replset.select_database(1));
        let _: Lock = replset.lock("same-lock-type", 1, 1);
    }

    #[test]
    fn blocking_client_handle_selects_backend_from_node_count() {
        assert!(matches!(
            ClientHandle::new(["127.0.0.1:5658"]),
            ClientHandle::Single(_)
        ));
        assert!(matches!(
            ClientHandle::new(["127.0.0.1:5658", "127.0.0.1:5659"]),
            ClientHandle::Replset(_)
        ));
    }

    #[test]
    fn blocking_tree_lock_exposes_java_leaf_api() {
        let client = Client::new("127.0.0.1:5658");
        let root = client.tree_lock("tree-root", 1, 1);
        let child = root.new_child();
        let loaded_child = root.load_child(child.lock_key().as_bytes());
        let leaf = root.new_leaf_lock();
        let loaded_leaf = root.load_leaf_lock(leaf.lock_id());

        assert_ne!(root.lock_key(), child.lock_key());
        assert_eq!(child.lock_key(), loaded_child.lock_key());
        assert_eq!(leaf.lock_id(), loaded_leaf.lock_id());
    }
}

#[cfg(feature = "aio")]
mod async_api {
    use ruslock::aio::{Client, ClientApi, ClientHandle, Database, Lock, ReplsetClient, TreeLock};

    fn build_business_primitives<C: ClientApi>(client: &C) -> (Database, Lock, TreeLock) {
        let db = client.select_database(255);
        let lock = client.lock("api-lock", 1, 1);
        let tree = client.tree_lock("api-tree", 1, 1);
        (db, lock, tree)
    }

    #[test]
    fn async_client_replset_and_handle_share_business_api() {
        let client = Client::new("127.0.0.1:5658");
        let replset = ReplsetClient::new(["127.0.0.1:5658", "127.0.0.1:5659"]);
        let single_handle = ClientHandle::new("127.0.0.1:5658");
        let replset_handle = ClientHandle::new("127.0.0.1:5658,127.0.0.1:5659");

        assert_eq!(build_business_primitives(&client).0.db_id(), 255);
        assert_eq!(build_business_primitives(&replset).0.db_id(), 255);
        assert_eq!(build_business_primitives(&single_handle).0.db_id(), 255);
        assert_eq!(build_business_primitives(&replset_handle).0.db_id(), 255);

        let _: Database = replset.select_database(1);
        let _: Lock = replset.lock("same-lock-type", 1, 1);
    }

    #[test]
    fn async_client_handle_selects_backend_from_node_count() {
        assert!(matches!(
            ClientHandle::new(["127.0.0.1:5658"]),
            ClientHandle::Single(_)
        ));
        assert!(matches!(
            ClientHandle::new(["127.0.0.1:5658", "127.0.0.1:5659"]),
            ClientHandle::Replset(_)
        ));
    }

    #[test]
    fn async_tree_lock_exposes_java_leaf_api() {
        let client = Client::new("127.0.0.1:5658");
        let root = client.tree_lock("tree-root", 1, 1);
        let child = root.new_child();
        let loaded_child = root.load_child(child.lock_key().as_bytes());
        let leaf = root.new_leaf_lock();
        let loaded_leaf = root.load_leaf_lock(leaf.lock_id());

        assert_ne!(root.lock_key(), child.lock_key());
        assert_eq!(child.lock_key(), loaded_child.lock_key());
        assert_eq!(leaf.lock_id(), loaded_leaf.lock_id());
    }
}
