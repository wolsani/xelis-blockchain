use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedContractProvider,
};
use super::super::super::super::MemoryStorage;

#[async_trait]
impl VersionedContractProvider for MemoryStorage {
    async fn delete_versioned_contracts_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.contracts.iter_mut()
            .for_each(|(_, entry)| {
                entry.modules.split_off(&topoheight);
                entry.data.retain(|_, data_map| {
                    data_map.split_off(&topoheight);
                    !data_map.is_empty()
                });
                entry.balances.iter_mut().for_each(|(_, balance_map)| {
                    balance_map.split_off(&topoheight);
                });
            });
        Ok(())
    }

    async fn delete_versioned_contracts_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let topoheight = topoheight + 1;
        self.contracts.iter_mut()
            .for_each(|(_, entry)| {
                entry.modules.split_off(&topoheight);
                entry.data.retain(|_, data_map| {
                    data_map.split_off(&topoheight);
                    !data_map.is_empty()
                });
                entry.balances.iter_mut().for_each(|(_, balance_map)| {
                    balance_map.split_off(&topoheight);
                });
            });
        Ok(())
    }

    async fn delete_versioned_contracts_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.contracts.iter_mut()
            .for_each(|(_, entry)| {
                // TODO: if keep_last, we must check that the last value is not deleted, even if its below the topoheight.
                entry.modules.split_off(&topoheight);
                entry.data.retain(|_, data_map| {
                    let to_keep = data_map.split_off(&topoheight);
                    *data_map = to_keep;

                    !data_map.is_empty()
                });
                entry.balances.iter_mut().for_each(|(_, balance_map)| {
                    let to_keep = balance_map.split_off(&topoheight);
                    *balance_map = to_keep;
                });
            });

        Ok(())
    }
}
