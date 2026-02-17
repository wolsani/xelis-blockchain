use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedContractEventCallbackProvider,
};
use super::super::super::super::MemoryStorage;

#[async_trait]
impl VersionedContractEventCallbackProvider for MemoryStorage {
    async fn delete_versioned_contract_event_callbacks_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_event_callbacks.range((topoheight, 0, 0, 0)..=(topoheight, u64::MAX, u64::MAX, u64::MAX))
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((_, cid, eid, lid), prev_topo) in keys {
            self.versioned_event_callbacks.remove(&(topoheight, cid, eid, lid));
            let pointer = self.event_callback_pointers.get(&(cid, eid, lid)).copied();
            if pointer.is_some_and(|p| p >= topoheight) {
                if let Some(prev) = prev_topo {
                    self.event_callback_pointers.insert((cid, eid, lid), prev);
                } else {
                    self.event_callback_pointers.remove(&(cid, eid, lid));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contract_event_callbacks_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_event_callbacks.range((topoheight + 1, 0, 0, 0)..)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((t, cid, eid, lid), prev_topo) in keys {
            self.versioned_event_callbacks.remove(&(t, cid, eid, lid));
            let pointer = self.event_callback_pointers.get(&(cid, eid, lid)).copied();
            if pointer.is_none_or(|v| v > topoheight) {
                let filtered = prev_topo.filter(|&v| v <= topoheight);
                if let Some(p) = filtered {
                    self.event_callback_pointers.insert((cid, eid, lid), p);
                } else {
                    self.event_callback_pointers.remove(&(cid, eid, lid));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contract_event_callbacks_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_event_callbacks.range(..(topoheight, 0, 0, 0))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_event_callbacks.remove(&key);
        }
        Ok(())
    }
}
