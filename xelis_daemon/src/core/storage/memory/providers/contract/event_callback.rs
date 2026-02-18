use pooled_arc::PooledArc;
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
        let shared_contract = PooledArc::from_ref(contract);
        let shared_listener = PooledArc::from_ref(listener_contract);
        self.event_callback_pointers.insert((shared_contract.clone(), event_id, shared_listener.clone()), topoheight);
        self.versioned_event_callbacks.insert((topoheight, shared_contract, event_id, shared_listener), version);
        Ok(())
    }

    async fn get_event_callback_for_contract_at_maximum_topoheight(
        &self,
        contract: &Hash,
        event_id: u64,
        listener_contract: &Hash,
        max_topoheight: TopoHeight,
    ) -> Result<Option<(TopoHeight, VersionedEventCallbackRegistration)>, BlockchainError> {
        let shared_contract = PooledArc::from_ref(contract);
        let shared_listener = PooledArc::from_ref(listener_contract);
        let Some(&pointer) = self.event_callback_pointers.get(&(shared_contract.clone(), event_id, shared_listener.clone())) else {
            return Ok(None);
        };

        let mut topo = Some(pointer);
        while let Some(t) = topo {
            if t <= max_topoheight {
                if let Some(ver) = self.versioned_event_callbacks.get(&(t, shared_contract.clone(), event_id, shared_listener.clone())) {
                    return Ok(Some((t, ver.clone())));
                }
            }
            topo = self.versioned_event_callbacks.get(&(t, shared_contract.clone(), event_id, shared_listener.clone()))
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
        let shared_contract = PooledArc::from_ref(contract);
        let listeners: Vec<_> = self.event_callback_pointers.iter()
            .filter(move |(&(ref cid, eid, _), _)| cid.as_ref() == contract && eid == event_id)
            .filter_map(move |(&(_, _, ref listener), &pointer)| {
                let mut topo = Some(pointer);
                while let Some(t) = topo {
                    if t <= max_topoheight {
                        if let Some(ver) = self.versioned_event_callbacks.get(&(t, shared_contract.clone(), event_id, listener.clone())) {
                            return Some(Ok((listener.as_ref().clone(), t, ver.clone())));
                        }
                    }
                    topo = self.versioned_event_callbacks.get(&(t, shared_contract.clone(), event_id, listener.clone()))
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
        let shared_contract = PooledArc::from_ref(contract);
        let listeners: Vec<_> = self.event_callback_pointers.iter()
            .filter(move |(&(ref cid, eid, _), _)| cid.as_ref() == contract && eid == event_id)
            .filter_map(move |(&(_, _, ref listener), &pointer)| {
                let mut topo = Some(pointer);
                while let Some(t) = topo {
                    if t <= max_topoheight {
                        if let Some(ver) = self.versioned_event_callbacks.get(&(t, shared_contract.clone(), event_id, listener.clone())) {
                            let callback = ver.get().as_ref()?.clone();
                            return Some(Ok((listener.as_ref().clone(), callback)));
                        }
                    }
                    topo = self.versioned_event_callbacks.get(&(t, shared_contract.clone(), event_id, listener.clone()))
                        .and_then(|v| v.get_previous_topoheight());
                }
                None
            })
            .collect();
        Ok(listeners.into_iter())
    }
}
