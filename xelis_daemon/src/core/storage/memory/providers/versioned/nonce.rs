use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedNonceProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedNonceProvider for MemoryStorage {
    async fn delete_versioned_nonces_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_nonces.retain(|&(_, t), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_nonces_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_nonces.retain(|&(_, t), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_nonces_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_nonces.retain(|&(_, t), _| t >= topoheight);
        Ok(())
    }
}
