use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedAssetsCirculatingSupplyProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedAssetsCirculatingSupplyProvider for MemoryStorage {
    async fn delete_versioned_assets_supply_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_assets_supply.retain(|&(t, _), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_assets_supply_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_assets_supply.retain(|&(t, _), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_assets_supply_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_assets_supply.retain(|&(t, _), _| t >= topoheight);
        Ok(())
    }
}
