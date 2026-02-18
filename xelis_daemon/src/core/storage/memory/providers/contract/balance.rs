use pooled_arc::PooledArc;
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
        Ok(self.contract_balance_pointers.contains_key(&(PooledArc::from_ref(contract), PooledArc::from_ref(asset))))
    }

    async fn has_contract_balance_at_exact_topoheight(&self, contract: &Hash, asset: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        Ok(self.versioned_contract_balances.contains_key(&(topoheight, PooledArc::from_ref(contract), PooledArc::from_ref(asset))))
    }

    async fn get_contract_balance_at_exact_topoheight(&self, contract: &Hash, asset: &Hash, topoheight: TopoHeight) -> Result<VersionedContractBalance, BlockchainError> {
        self.versioned_contract_balances.get(&(topoheight, PooledArc::from_ref(contract), PooledArc::from_ref(asset)))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_contract_balance_at_maximum_topoheight(&self, contract: &Hash, asset: &Hash, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedContractBalance)>, BlockchainError> {
        let shared_contract = PooledArc::from_ref(contract);
        let shared_asset = PooledArc::from_ref(asset);
        let mut topo = self.contract_balance_pointers.get(&(shared_contract.clone(), shared_asset.clone())).copied();
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(bal) = self.versioned_contract_balances.get(&(t, shared_contract.clone(), shared_asset.clone())) {
                    return Ok(Some((t, bal.clone())));
                }
            }
            topo = self.versioned_contract_balances.get(&(t, shared_contract.clone(), shared_asset.clone()))
                .and_then(|b| b.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn get_last_topoheight_for_contract_balance(&self, contract: &Hash, asset: &Hash) -> Result<Option<TopoHeight>, BlockchainError> {
        Ok(self.contract_balance_pointers.get(&(PooledArc::from_ref(contract), PooledArc::from_ref(asset))).copied())
    }

    async fn get_last_contract_balance(&self, contract: &Hash, asset: &Hash) -> Result<(TopoHeight, VersionedContractBalance), BlockchainError> {
        let shared_contract = PooledArc::from_ref(contract);
        let shared_asset = PooledArc::from_ref(asset);
        let pointer = self.contract_balance_pointers.get(&(shared_contract.clone(), shared_asset.clone()))
            .copied()
            .ok_or(BlockchainError::Unknown)?;
        let balance = self.versioned_contract_balances.get(&(pointer, shared_contract, shared_asset))
            .cloned()
            .ok_or(BlockchainError::Unknown)?;
        Ok((pointer, balance))
    }

    async fn get_contract_assets_for<'a>(&'a self, contract: &'a Hash) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        Ok(self.contract_balance_pointers.keys()
            .filter(move |(c, _)| c.as_ref() == contract)
            .map(move |(_, asset)| Ok(asset.as_ref().clone()))
        )
    }

    async fn set_last_contract_balance_to(&mut self, contract: &Hash, asset: &Hash, topoheight: TopoHeight, balance: VersionedContractBalance) -> Result<(), BlockchainError> {
        let shared_contract = PooledArc::from_ref(contract);
        let shared_asset = PooledArc::from_ref(asset);
        self.contract_balance_pointers.insert((shared_contract.clone(), shared_asset.clone()), topoheight);
        self.versioned_contract_balances.insert((topoheight, shared_contract, shared_asset), balance);
        Ok(())
    }
}
