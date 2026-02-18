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
        self.versioned_event_callbacks.retain(|&(t, _, _, _), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_contract_event_callbacks_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_event_callbacks.retain(|&(t, _, _, _), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_contract_event_callbacks_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_event_callbacks.retain(|&(t, _, _, _), _| t >= topoheight);
        Ok(())
    }
}
