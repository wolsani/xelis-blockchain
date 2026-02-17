use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedContractDataProvider,
};
use super::super::super::super::MemoryStorage;

#[async_trait]
impl VersionedContractDataProvider for MemoryStorage {
    async fn delete_versioned_contract_data_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contract_data.range((topoheight, 0, 0)..=(topoheight, u64::MAX, u64::MAX))
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((_, cid, did), prev_topo) in keys {
            self.versioned_contract_data.remove(&(topoheight, cid, did));
            let pointer = self.contract_data_pointers.get(&(cid, did)).copied();
            if pointer.is_some_and(|p| p >= topoheight) {
                if let Some(prev) = prev_topo {
                    self.contract_data_pointers.insert((cid, did), prev);
                } else {
                    self.contract_data_pointers.remove(&(cid, did));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contract_data_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contract_data.range((topoheight + 1, 0, 0)..)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((t, cid, did), prev_topo) in keys {
            self.versioned_contract_data.remove(&(t, cid, did));
            let pointer = self.contract_data_pointers.get(&(cid, did)).copied();
            if pointer.is_none_or(|v| v > topoheight) {
                let filtered = prev_topo.filter(|&v| v <= topoheight);
                if let Some(p) = filtered {
                    self.contract_data_pointers.insert((cid, did), p);
                } else {
                    self.contract_data_pointers.remove(&(cid, did));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contract_data_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contract_data.range(..(topoheight, 0, 0))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_contract_data.remove(&key);
        }
        Ok(())
    }
}
