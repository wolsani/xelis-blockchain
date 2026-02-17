use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedContractProvider,
};
use super::super::super::super::MemoryStorage;

#[async_trait]
impl VersionedContractProvider for MemoryStorage {
    async fn delete_versioned_contracts_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contracts.range((topoheight, 0)..=(topoheight, u64::MAX))
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((_, cid), prev_topo) in keys {
            self.versioned_contracts.remove(&(topoheight, cid));
            if let Some(hash) = self.contract_by_id.get(&cid).cloned() {
                if let Some(entry) = self.contracts.get_mut(&hash) {
                    if entry.module_pointer.is_some_and(|p| p >= topoheight) {
                        entry.module_pointer = prev_topo;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contracts_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contracts.range((topoheight + 1, 0)..)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((t, cid), prev_topo) in keys {
            self.versioned_contracts.remove(&(t, cid));
            if let Some(hash) = self.contract_by_id.get(&cid).cloned() {
                if let Some(entry) = self.contracts.get_mut(&hash) {
                    if entry.module_pointer.is_none_or(|v| v > topoheight) {
                        entry.module_pointer = prev_topo.filter(|&v| v <= topoheight);
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contracts_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contracts.range(..(topoheight, 0))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_contracts.remove(&key);
        }
        Ok(())
    }
}
