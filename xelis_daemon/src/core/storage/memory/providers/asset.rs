use async_trait::async_trait;
use xelis_common::{
    asset::{AssetData, VersionedAssetData},
    block::TopoHeight,
    crypto::{Hash, PublicKey},
};
use crate::core::{
    error::BlockchainError,
    storage::AssetProvider,
};
use super::super::{AssetEntry, MemoryStorage};

#[async_trait]
impl AssetProvider for MemoryStorage {
    async fn has_asset(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.assets.contains_key(hash))
    }

    async fn has_asset_at_exact_topoheight(&self, hash: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let asset = self.assets.get(hash).ok_or(BlockchainError::AssetNotFound(hash.clone()))?;
        Ok(self.versioned_assets.contains_key(&(topoheight, asset.id)))
    }

    async fn get_asset_topoheight(&self, hash: &Hash) -> Result<Option<TopoHeight>, BlockchainError> {
        Ok(self.assets.get(hash).and_then(|a| a.data_pointer))
    }

    async fn get_asset_at_topoheight(&self, hash: &Hash, topoheight: TopoHeight) -> Result<VersionedAssetData, BlockchainError> {
        let asset = self.assets.get(hash).ok_or(BlockchainError::AssetNotFound(hash.clone()))?;
        self.versioned_assets.get(&(topoheight, asset.id))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn is_asset_registered_at_maximum_topoheight(&self, hash: &Hash, maximum_topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        match self.get_asset_topoheight(hash).await? {
            Some(topo) if topo <= maximum_topoheight => Ok(true),
            _ => Ok(false),
        }
    }

    async fn get_asset_at_maximum_topoheight(&self, hash: &Hash, topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedAssetData)>, BlockchainError> {
        let Some(asset) = self.assets.get(hash) else {
            return Ok(None);
        };
        let mut topo = asset.data_pointer;
        while let Some(t) = topo {
            if t <= topoheight {
                if let Some(data) = self.versioned_assets.get(&(t, asset.id)) {
                    return Ok(Some((t, data.clone())));
                }
            }
            topo = self.versioned_assets.get(&(t, asset.id))
                .and_then(|d| d.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn get_asset(&self, hash: &Hash) -> Result<(TopoHeight, VersionedAssetData), BlockchainError> {
        let asset = self.assets.get(hash).ok_or(BlockchainError::AssetNotFound(hash.clone()))?;
        let topoheight = asset.data_pointer.ok_or(BlockchainError::AssetNotFound(hash.clone()))?;
        let data = self.versioned_assets.get(&(topoheight, asset.id))
            .cloned()
            .ok_or(BlockchainError::Unknown)?;
        Ok((topoheight, data))
    }

    async fn get_assets<'a>(&'a self) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        Ok(self.assets.keys().cloned().map(Ok))
    }

    async fn get_assets_with_data_in_range<'a>(&'a self, minimum_topoheight: Option<u64>, maximum_topoheight: Option<u64>) -> Result<impl Iterator<Item = Result<(Hash, TopoHeight, AssetData), BlockchainError>> + 'a, BlockchainError> {
        Ok(self.assets.iter()
            .filter_map(move |(hash, asset)| {
                let topoheight = asset.data_pointer?;
                if minimum_topoheight.is_some_and(|v| topoheight < v) || maximum_topoheight.is_some_and(|v| topoheight > v) {
                    return None;
                }
                let data = self.versioned_assets.get(&(topoheight, asset.id))?;
                Some(Ok((hash.clone(), topoheight, data.clone().take())))
            })
        )
    }

    async fn get_assets_for<'a>(&'a self, key: &'a PublicKey) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        let account_id = self.get_account_id(key)?;
        Ok(self.balance_pointers.keys()
            .filter(move |(aid, _)| *aid == account_id)
            .filter_map(move |(_, asset_id)| {
                self.asset_by_id.get(asset_id).cloned().map(Ok)
            })
        )
    }

    async fn count_assets(&self) -> Result<u64, BlockchainError> {
        Ok(self.next_asset_id)
    }

    async fn add_asset(&mut self, hash: &Hash, topoheight: TopoHeight, data: VersionedAssetData) -> Result<(), BlockchainError> {
        let asset = if let Some(asset) = self.assets.get_mut(hash) {
            asset.data_pointer = Some(topoheight);
            asset.clone()
        } else {
            let id = self.next_asset_id;
            self.next_asset_id += 1;
            let entry = AssetEntry {
                id,
                data_pointer: Some(topoheight),
                supply_pointer: None,
            };
            self.asset_by_id.insert(id, hash.clone());
            self.assets.insert(hash.clone(), entry.clone());
            entry
        };

        self.versioned_assets.insert((topoheight, asset.id), data);
        Ok(())
    }
}
