use async_trait::async_trait;
use xelis_common::{
    block::TopoHeight,
    crypto::Hash,
};
use crate::core::{
    error::BlockchainError,
    storage::{AssetCirculatingSupplyProvider, VersionedSupply},
};
use super::super::MemoryStorage;

#[async_trait]
impl AssetCirculatingSupplyProvider for MemoryStorage {
    async fn has_circulating_supply_for_asset(&self, asset: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.assets.get(asset).map_or(false, |a| a.supply_pointer.is_some()))
    }

    async fn has_circulating_supply_for_asset_at_exact_topoheight(&self, asset: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let asset_id = self.get_asset_id(asset)?;
        Ok(self.versioned_assets_supply.contains_key(&(topoheight, asset_id)))
    }

    async fn get_circulating_supply_for_asset_at_exact_topoheight(&self, asset: &Hash, topoheight: TopoHeight) -> Result<VersionedSupply, BlockchainError> {
        let asset_id = self.get_asset_id(asset)?;
        self.versioned_assets_supply.get(&(topoheight, asset_id))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_circulating_supply_for_asset_at_maximum_topoheight(&self, asset: &Hash, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedSupply)>, BlockchainError> {
        let Some(asset_entry) = self.assets.get(asset) else {
            return Ok(None);
        };
        let Some(pointer) = asset_entry.supply_pointer else {
            return Ok(None);
        };

        let start = if pointer > maximum_topoheight
            && self.versioned_assets_supply.contains_key(&(maximum_topoheight, asset_entry.id))
        {
            maximum_topoheight
        } else {
            pointer
        };

        let mut topo = Some(start);
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(supply) = self.versioned_assets_supply.get(&(t, asset_entry.id)) {
                    return Ok(Some((t, supply.clone())));
                }
            }
            topo = self.versioned_assets_supply.get(&(t, asset_entry.id))
                .and_then(|s| s.get_previous_topoheight());
        }

        Ok(None)
    }

    async fn set_last_circulating_supply_for_asset(&mut self, hash: &Hash, topoheight: TopoHeight, supply: &VersionedSupply) -> Result<(), BlockchainError> {
        let asset = self.assets.get_mut(hash).ok_or(BlockchainError::AssetNotFound(hash.clone()))?;
        asset.supply_pointer = Some(topoheight);
        let id = asset.id;
        self.versioned_assets_supply.insert((topoheight, id), supply.clone());
        Ok(())
    }
}
