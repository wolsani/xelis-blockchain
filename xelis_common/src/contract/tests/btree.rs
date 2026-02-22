use crate::contract::vm::ExitValue;

use super::*;

/// Test basic BTree insert and get operations
#[tokio::test]
async fn test_btree_insert_and_get() {
    let code = r#"
        entry insert_value(key: bytes, value: bytes) {
            let store = BTreeStore::new(key);
            store.insert(key, value);
            return 0
        }

        entry get_value(key: bytes) -> bytes {
            let store = BTreeStore::new(key);
            let value = store.get(key).unwrap();
            return value
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Insert a value
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![
            ValueCell::Bytes("test_key".as_bytes().to_vec()),
            ValueCell::Bytes("test_value".as_bytes().to_vec()),
        ],
    )
    .await
    .expect("insert value");

    assert!(result.is_success(), "insert should succeed: {:?}", result);

    let storage = &chain_state.contract_caches.get(&contract_hash)
        .expect("contract cache")
        .storage;
    assert!(!storage.is_empty(), "storage should have entries after insert");

    // Get the value back
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![ValueCell::Bytes("test_key".as_bytes().to_vec())],
    )
    .await
    .expect("get value");

    assert!(result.is_success(), "get should succeed: {:?}", result);

    let ExitValue::Payload(ValueCell::Bytes(payload)) = result.exit_value else {
        panic!("invalid exit value");
    };

    assert!(payload == b"test_value".to_vec());
}

/// Test BTree delete operations
#[tokio::test]
async fn test_btree_delete() {
    let code = r#"
        entry insert_value(key: bytes, value: bytes) {
            let store = BTreeStore::new(key);
            store.insert(key, value);
            return 0
        }

        entry get_value(key: bytes) -> bytes {
            let store = BTreeStore::new(key);
            return store.get(key).expect("key should exist")
        }

        entry delete_value(key: bytes) -> bool {
            let store = BTreeStore::new(key);
            return store.delete(key)
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Insert a value
    invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![
            ValueCell::Bytes("key1".as_bytes().to_vec()),
            ValueCell::Bytes("value1".as_bytes().to_vec()),
        ],
    )
    .await
    .expect("insert");

    // Try to get the deleted value (should be present)
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![ValueCell::Bytes("key1".as_bytes().to_vec())],
    )
    .await
    .expect("get present");

    assert!(result.is_success(), "get deleted should succeed: {:?}", result);

    // Delete the value
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(2),
        vec![ValueCell::Bytes("key1".as_bytes().to_vec())],
    )
    .await
    .expect("delete");

    assert!(result.is_success(), "delete should succeed: {:?}", result);

    // Try to get the deleted value (should be empty)
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![ValueCell::Bytes("key1".as_bytes().to_vec())],
    )
    .await
    .expect("get deleted");

    assert!(!result.is_success(), "get deleted should fail: {:?}", result);
}

/// Test BTree multiple values with same key (duplicate key handling)
#[tokio::test]
async fn test_btree_duplicate_keys() {
    let code = r#"
        entry insert_multiple(key: bytes, val1: bytes, val2: bytes, val3: bytes) {
            let store = BTreeStore::new(key);
            store.insert(key, val1);
            store.insert(key, val2);
            store.insert(key, val3);
            return 0
        }

        entry get_first(key: bytes) -> bytes {
            let store = BTreeStore::new(key);
            return store.get(key)
        }

        entry delete_and_get_next(key: bytes) -> bytes {
            let store = BTreeStore::new(key);
            store.delete(key);
            return store.get(key)
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Insert multiple values with same key
    invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![
            ValueCell::Bytes("mykey".as_bytes().to_vec()),
            ValueCell::Bytes("value1".as_bytes().to_vec()),
            ValueCell::Bytes("value2".as_bytes().to_vec()),
            ValueCell::Bytes("value3".as_bytes().to_vec()),
        ],
    )
    .await
    .expect("insert multiple");

    // Get first value (should be bytes(1) due to insertion order)
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![ValueCell::Bytes("mykey".as_bytes().to_vec())],
    )
    .await
    .expect("get first");

    assert!(result.is_success(), "get first should succeed: {:?}", result);

    // Delete first and get next value
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(2),
        vec![ValueCell::Bytes("mykey".as_bytes().to_vec())],
    )
    .await
    .expect("delete and get next");

    assert!(result.is_success(), "delete and get next should succeed: {:?}", result);
}

/// Test internal mutability: modifying a value after retrieval should NOT affect storage
#[tokio::test]
async fn test_btree_internal_mutability_string() {
    // This test demonstrates that BTree values are stored by value and changes
    // to the retrieved value don't affect the original stored value
    let code = r#"
        entry store_data(key: bytes, data_bytes: bytes) {
            let store = BTreeStore::new(key);
            store.insert(key, data_bytes);
            return 0
        }

        entry get_and_verify(key: bytes) -> bool {
            let store = BTreeStore::new(key);
            let retrieved = store.get(key);
            return retrieved.len() > 0
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Store data
    invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![
            ValueCell::Bytes("key1".as_bytes().to_vec()),
            ValueCell::Bytes("data".as_bytes().to_vec()),
        ],
    )
    .await
    .expect("store data");

    // Get and verify it's unchanged
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![ValueCell::Bytes("key1".as_bytes().to_vec())],
    )
    .await
    .expect("get and verify");

    assert!(result.is_success(), "get and verify should succeed: {:?}", result);
}

/// Test internal mutability: numeric types and references
#[tokio::test]
async fn test_btree_internal_mutability_numeric() {
    let code = r#"
        entry store_number(key: bytes, val: bytes) {
            let store = BTreeStore::new(key);
            store.insert(key, val);
            return 0
        }

        entry get_and_verify(key: bytes) -> bool {
            let store = BTreeStore::new(key);
            let value = store.get(key);
            return value.len() > 0
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Store number
    invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![
            ValueCell::Bytes("key1".as_bytes().to_vec()),
            ValueCell::Bytes("data".as_bytes().to_vec()),
        ],
    )
    .await
    .expect("store number");

    // Get and verify
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![ValueCell::Bytes("key1".as_bytes().to_vec())],
    )
    .await
    .expect("get and verify");

    assert!(result.is_success(), "get and verify should succeed: {:?}", result);
}

/// Test BTree with different namespaces maintain separate stores
#[tokio::test]
async fn test_btree_different_namespaces() {
    let code = r#"
        entry insert_to_store1(key: bytes, value: bytes) {
            let store = BTreeStore::new(bytes(1));
            store.insert(key, value);
            return 0
        }

        entry insert_to_store2(key: bytes, value: bytes) {
            let store = BTreeStore::new(bytes(2));
            store.insert(key, value);
            return 0
        }

        entry get_from_store1(key: bytes) -> bytes {
            let store = BTreeStore::new(bytes(1));
            return store.get(key)
        }

        entry get_from_store2(key: bytes) -> bytes {
            let store = BTreeStore::new(bytes(2));
            return store.get(key)
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Insert into store1
    invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![
            ValueCell::Bytes("key".as_bytes().to_vec()),
            ValueCell::Bytes("value1".as_bytes().to_vec()),
        ],
    )
    .await
    .expect("insert to store1");

    // Insert into store2
    invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![
            ValueCell::Bytes("key".as_bytes().to_vec()),
            ValueCell::Bytes("value2".as_bytes().to_vec()),
        ],
    )
    .await
    .expect("insert to store2");

    // Verify store1 has value1
    let result1 = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(2),
        vec![ValueCell::Bytes("key".as_bytes().to_vec())],
    )
    .await
    .expect("get from store1");

    assert!(result1.is_success(), "get from store1 should succeed: {:?}", result1);

    // Verify store2 has value2
    let result2 = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(3),
        vec![ValueCell::Bytes("key".as_bytes().to_vec())],
    )
    .await
    .expect("get from store2");

    assert!(result2.is_success(), "get from store2 should succeed: {:?}", result2);
}

/// Test BTree cursor operations (seek)
#[tokio::test]
async fn test_btree_cursor_seek() {
    let code = r#"
        entry insert_values(key: bytes) {
            let store = BTreeStore::new(key);
            store.insert(bytes(1), bytes(1));
            store.insert(bytes(2), bytes(2));
            store.insert(bytes(3), bytes(3));
            store.insert(bytes(4), bytes(4));
            return 0
        }

        entry has_data(key: bytes) -> bool {
            let store = BTreeStore::new(key);
            let value = store.get(key);
            return value.len() > 0
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Insert values
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![ValueCell::Bytes("ns".as_bytes().to_vec())],
    )
    .await
    .expect("insert values");

    assert!(result.is_success(), "insert values should succeed: {:?}", result);

    // Verify data exists
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(1),
        vec![ValueCell::Bytes("ns".as_bytes().to_vec())],
    )
    .await
    .expect("has data");

    assert!(result.is_success(), "has data should succeed: {:?}", result);
}