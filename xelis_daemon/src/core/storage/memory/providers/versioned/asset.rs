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
        self.assets.iter_mut().for_each(|(_, entry)| {
            entry.data.split_off(&topoheight);
        });
        Ok(())
    }

    async fn delete_versioned_assets_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.assets.iter_mut().for_each(|(_, entry)| {
            entry.data.split_off(&(topoheight + 1));
        });
        Ok(())
    }

    async fn delete_versioned_assets_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let iter = self.assets.iter_mut()
            .map(|(_, entry)| (entry.data.split_off(&topoheight), &mut entry.data));

        for (to_keep, data) in iter {
            *data = to_keep;
        }

        Ok(())
    }
}
