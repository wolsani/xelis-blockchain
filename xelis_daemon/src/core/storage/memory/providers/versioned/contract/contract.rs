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
        self.versioned_contracts.retain(|&(t, _), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_contracts_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_contracts.retain(|&(t, _), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_contracts_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_contracts.retain(|&(t, _), _| t >= topoheight);
        Ok(())
    }
}
