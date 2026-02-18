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
        self.versioned_contract_data.retain(|&(t, _, _), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_contract_data_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_contract_data.retain(|&(t, _, _), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_contract_data_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_contract_data.retain(|&(t, _, _), _| t >= topoheight);
        Ok(())
    }
}
