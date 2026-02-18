use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedAssetProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedAssetProvider for MemoryStorage {
    async fn delete_versioned_assets_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_assets.retain(|&(t, _), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_assets_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_assets.retain(|&(t, _), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_assets_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_assets.retain(|&(t, _), _| t >= topoheight);
        Ok(())
    }
}
