use async_trait::async_trait;
use xelis_common::{
    block::TopoHeight,
    crypto::Hash,
};
use crate::core::storage::VersionedContractBalance;
use crate::core::{
    error::BlockchainError,
    storage::ContractBalanceProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl ContractBalanceProvider for MemoryStorage {
    async fn has_contract_balance_for(&self, contract: &Hash, asset: &Hash) -> Result<bool, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else { return Ok(false); };
        let Some(asset_id) = self.get_optional_asset_id(asset) else { return Ok(false); };
        Ok(self.contract_balance_pointers.contains_key(&(contract_id, asset_id)))
    }

    async fn has_contract_balance_at_exact_topoheight(&self, contract: &Hash, asset: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let asset_id = self.get_asset_id(asset)?;
        Ok(self.versioned_contract_balances.contains_key(&(topoheight, contract_id, asset_id)))
    }

    async fn get_contract_balance_at_exact_topoheight(&self, contract: &Hash, asset: &Hash, topoheight: TopoHeight) -> Result<VersionedContractBalance, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let asset_id = self.get_asset_id(asset)?;
        self.versioned_contract_balances.get(&(topoheight, contract_id, asset_id))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_contract_balance_at_maximum_topoheight(&self, contract: &Hash, asset: &Hash, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedContractBalance)>, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else { return Ok(None); };
        let Some(asset_id) = self.get_optional_asset_id(asset) else { return Ok(None); };
        let mut topo = self.contract_balance_pointers.get(&(contract_id, asset_id)).copied();
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(bal) = self.versioned_contract_balances.get(&(t, contract_id, asset_id)) {
                    return Ok(Some((t, bal.clone())));
                }
            }
            topo = self.versioned_contract_balances.get(&(t, contract_id, asset_id))
                .and_then(|b| b.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn get_last_topoheight_for_contract_balance(&self, contract: &Hash, asset: &Hash) -> Result<Option<TopoHeight>, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else { return Ok(None); };
        let Some(asset_id) = self.get_optional_asset_id(asset) else { return Ok(None); };
        Ok(self.contract_balance_pointers.get(&(contract_id, asset_id)).copied())
    }

    async fn get_last_contract_balance(&self, contract: &Hash, asset: &Hash) -> Result<(TopoHeight, VersionedContractBalance), BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let asset_id = self.get_asset_id(asset)?;
        let pointer = self.contract_balance_pointers.get(&(contract_id, asset_id))
            .copied()
            .ok_or(BlockchainError::Unknown)?;
        let balance = self.versioned_contract_balances.get(&(pointer, contract_id, asset_id))
            .cloned()
            .ok_or(BlockchainError::Unknown)?;
        Ok((pointer, balance))
    }

    async fn get_contract_assets_for<'a>(&'a self, contract: &'a Hash) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        Ok(self.contract_balance_pointers.keys()
            .filter(move |(cid, _)| *cid == contract_id)
            .filter_map(move |(_, asset_id)| {
                self.asset_by_id.get(asset_id).cloned().map(Ok)
            })
        )
    }

    async fn set_last_contract_balance_to(&mut self, contract: &Hash, asset: &Hash, topoheight: TopoHeight, balance: VersionedContractBalance) -> Result<(), BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let asset_id = self.get_asset_id(asset)?;
        self.contract_balance_pointers.insert((contract_id, asset_id), topoheight);
        self.versioned_contract_balances.insert((topoheight, contract_id, asset_id), balance);
        Ok(())
    }
}
