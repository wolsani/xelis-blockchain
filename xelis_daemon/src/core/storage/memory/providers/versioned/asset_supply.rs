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
        let keys: Vec<_> = self.versioned_assets_supply.range((topoheight, 0)..=(topoheight, u64::MAX))
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((_, aid), prev_topo) in keys {
            self.versioned_assets_supply.remove(&(topoheight, aid));
            if let Some(hash) = self.asset_by_id.get(&aid).cloned() {
                if let Some(asset) = self.assets.get_mut(&hash) {
                    if asset.supply_pointer.is_some_and(|p| p >= topoheight) {
                        asset.supply_pointer = prev_topo;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_assets_supply_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_assets_supply.range((topoheight + 1, 0)..)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((t, aid), prev_topo) in keys {
            self.versioned_assets_supply.remove(&(t, aid));
            if let Some(hash) = self.asset_by_id.get(&aid).cloned() {
                if let Some(asset) = self.assets.get_mut(&hash) {
                    if asset.supply_pointer.is_none_or(|v| v > topoheight) {
                        asset.supply_pointer = prev_topo.filter(|&v| v <= topoheight);
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_assets_supply_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_assets_supply.range(..(topoheight, 0))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_assets_supply.remove(&key);
        }
        Ok(())
    }
}
