use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedRegistrationsProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedRegistrationsProvider for MemoryStorage {
    async fn delete_versioned_registrations_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.prefixed_registrations.retain(|&(t, _)| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_registrations_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.prefixed_registrations.retain(|&(t, _)| t <= topoheight);
        Ok(())
    }
}
