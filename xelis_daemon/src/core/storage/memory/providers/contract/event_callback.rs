use async_trait::async_trait;
use xelis_common::{
    block::TopoHeight,
    contract::EventCallbackRegistration,
    crypto::Hash,
};
use crate::core::storage::VersionedEventCallbackRegistration;
use crate::core::{
    error::BlockchainError,
    storage::ContractEventCallbackProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl ContractEventCallbackProvider for MemoryStorage {
    async fn set_last_contract_event_callback(
        &mut self,
        contract: &Hash,
        event_id: u64,
        listener_contract: &Hash,
        version: VersionedEventCallbackRegistration,
        topoheight: TopoHeight,
    ) -> Result<(), BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let listener_id = self.get_contract_id(listener_contract)?;
        self.event_callback_pointers.insert((contract_id, event_id, listener_id), topoheight);
        self.versioned_event_callbacks.insert((topoheight, contract_id, event_id, listener_id), version);
        Ok(())
    }

    async fn get_event_callback_for_contract_at_maximum_topoheight(
        &self,
        contract: &Hash,
        event_id: u64,
        listener_contract: &Hash,
        max_topoheight: TopoHeight,
    ) -> Result<Option<(TopoHeight, VersionedEventCallbackRegistration)>, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else { return Ok(None); };
        let Some(listener_id) = self.get_optional_contract_id(listener_contract) else { return Ok(None); };
        let Some(&pointer) = self.event_callback_pointers.get(&(contract_id, event_id, listener_id)) else {
            return Ok(None);
        };

        let mut topo = Some(pointer);
        while let Some(t) = topo {
            if t <= max_topoheight {
                if let Some(ver) = self.versioned_event_callbacks.get(&(t, contract_id, event_id, listener_id)) {
                    return Ok(Some((t, ver.clone())));
                }
            }
            topo = self.versioned_event_callbacks.get(&(t, contract_id, event_id, listener_id))
                .and_then(|v| v.get_previous_topoheight());
        }

        Ok(None)
    }

    async fn get_event_callbacks_for_event_at_maximum_topoheight<'a>(
        &'a self,
        contract: &'a Hash,
        event_id: u64,
        max_topoheight: TopoHeight,
    ) -> Result<impl Iterator<Item = Result<(Hash, TopoHeight, VersionedEventCallbackRegistration), BlockchainError>> + Send + 'a, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let listeners: Vec<_> = self.event_callback_pointers.iter()
            .filter(move |(&(cid, eid, _), _)| cid == contract_id && eid == event_id)
            .filter_map(move |(&(_, _, listener_id), &pointer)| {
                let mut topo = Some(pointer);
                while let Some(t) = topo {
                    if t <= max_topoheight {
                        if let Some(ver) = self.versioned_event_callbacks.get(&(t, contract_id, event_id, listener_id)) {
                            let hash = self.get_contract_hash_from_id(listener_id).ok()?;
                            return Some(Ok((hash, t, ver.clone())));
                        }
                    }
                    topo = self.versioned_event_callbacks.get(&(t, contract_id, event_id, listener_id))
                        .and_then(|v| v.get_previous_topoheight());
                }
                None
            })
            .collect();
        Ok(listeners.into_iter())
    }

    async fn get_event_callbacks_available_at_maximum_topoheight<'a>(
        &'a self,
        contract: &'a Hash,
        event_id: u64,
        max_topoheight: TopoHeight,
    ) -> Result<impl Iterator<Item = Result<(Hash, EventCallbackRegistration), BlockchainError>> + Send + 'a, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let listeners: Vec<_> = self.event_callback_pointers.iter()
            .filter(move |(&(cid, eid, _), _)| cid == contract_id && eid == event_id)
            .filter_map(move |(&(_, _, listener_id), &pointer)| {
                let mut topo = Some(pointer);
                while let Some(t) = topo {
                    if t <= max_topoheight {
                        if let Some(ver) = self.versioned_event_callbacks.get(&(t, contract_id, event_id, listener_id)) {
                            let callback = ver.get().as_ref()?.clone();
                            let hash = self.get_contract_hash_from_id(listener_id).ok()?;
                            return Some(Ok((hash, callback)));
                        }
                    }
                    topo = self.versioned_event_callbacks.get(&(t, contract_id, event_id, listener_id))
                        .and_then(|v| v.get_previous_topoheight());
                }
                None
            })
            .collect();
        Ok(listeners.into_iter())
    }
}
