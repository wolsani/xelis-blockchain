use crate::{contract::vm::ExitValue, versioned_type::VersionedState};

use super::*;

#[tokio::test]
async fn test_insert_and_get() {
    let code = r#"
        struct Foo {
            value: bytes
        }

        entry insert_value(key: bytes, value: Foo) {
            let storage = Storage::new();
            require(storage.store(key, value).is_none(), "Key already exists");
            value.value = b"overwritten_value";
            println(value.value);
            return 0
        }

        entry get_value(key: bytes) -> bytes {
            let storage = Storage::new();
            let value: Foo = storage.load(key).unwrap();
            return value.value
        }
    "#;

    let mut chain_state = MockChainState::new();
    let contract_hash = deploy_contract(&mut chain_state, code, ContractVersion::V1)
        .await
        .expect("deploy contract")
        .0;

    // Insert a value
    let value = ValueCell::Object(vec![
        ValueCell::Bytes("test_value".as_bytes().to_vec()).into()
    ]);
    let result = invoke_contract(
        &mut chain_state,
        &contract_hash,
        InvokeContract::Entry(0),
        vec![
            ValueCell::Bytes("test_key".as_bytes().to_vec()),
            value.clone(),            
        ],
    )
    .await
    .expect("insert value");

    assert!(result.is_success(), "insert should succeed: {:?}", result);

    let storage = &chain_state.contract_caches.get(&contract_hash)
        .expect("contract cache")
        .storage;
    assert!(storage.len() == 1, "storage should have 1 entry after insert");
    assert!(storage.get(&ValueCell::Bytes("test_key".as_bytes().to_vec())) == Some(&Some((VersionedState::New, Some(value)))), "storage should contain the inserted key");

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