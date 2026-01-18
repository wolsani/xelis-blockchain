use super::*;

#[tokio::test]
async fn contract_event_flow() {
    // Create a contract that emits an event when called
    // Another event that register an event listener from previous one
    // Call the first contract and verify the event is captured

    let code = r#"
        entry call_event() {
            emit_event(42, ["hello", "world!"]);
            return 0
        }
    "#;

    let mut chain_state = MockChainState::new();
    let emitter_hash = create_contract(&mut chain_state, code).expect("create emit event contract");

    let code = r#"
        fn on_contract_event(a: string, b: string) -> u64 {
            println(a + " " + b + " !");
            return 0
        }

        hook constructor() -> u64 {
            let contract = Hash::from_hex("CONTRACT_HASH");
            listen_event(contract, 0, on_contract_event, 500);
            
            return 0
        }
    "#.replace("CONTRACT_HASH", &emitter_hash.to_string());

    // Deploy the listener hash
    deploy_contract(&mut chain_state, &code).await
        .expect("deploy listener contract");

    println!("Invoking emitter contract {}...", emitter_hash);
    // Invoke the emitter contract to trigger the event
    invoke_contract(
        &mut chain_state,
        &emitter_hash,
        InvokeContract::Entry(0),
        vec![],
    ).await.expect("invoke emitter contract");

    for (caller, logs) in chain_state.contract_logs {
        println!("Logs for contract caller {}:", caller);
        for log in logs {
            println!("  {:?}", log);
        }
    }
}