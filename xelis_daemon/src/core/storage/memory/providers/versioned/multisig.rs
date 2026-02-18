use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedMultiSigProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedMultiSigProvider for MemoryStorage {
    async fn delete_versioned_multisigs_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_multisig.retain(|&(t, _), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_multisigs_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_multisig.retain(|&(t, _), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_multisigs_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_multisig.retain(|&(t, _), _| t >= topoheight);
        Ok(())
    }
}
