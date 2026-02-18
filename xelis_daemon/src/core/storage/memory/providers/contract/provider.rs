use pooled_arc::PooledArc;
use std::borrow::Cow;
use async_trait::async_trait;
use anyhow::Context;
use xelis_common::{
    asset::AssetData,
    block::TopoHeight,
    contract::ContractModule,
    crypto::{Hash, PublicKey},
    versioned_type::Versioned,
};
use xelis_common::contract::{
    ContractStorage,
    ContractProvider as ContractInfoProvider,
};
use crate::core::{
    error::BlockchainError,
    storage::{
        AssetCirculatingSupplyProvider,
        AssetProvider,
        AccountProvider,
        BalanceProvider,
        ContractBalanceProvider,
        ContractDataProvider,
        ContractEventCallbackProvider,
        ContractProvider,
        ContractScheduledExecutionProvider,
        VersionedContractModule,
    },
};
use super::super::super::MemoryStorage;

#[async_trait]
impl ContractProvider for MemoryStorage {
    async fn set_last_contract_to<'a>(&mut self, hash: &Hash, topoheight: TopoHeight, contract: &VersionedContractModule<'a>) -> Result<(), BlockchainError> {
        let entry = self.get_or_create_contract(hash);
        entry.module_pointer = Some(topoheight);
        let owned = Versioned::new(
            contract.get().as_ref().map(|v| v.clone().into_owned()),
            contract.get_previous_topoheight(),
        );
        self.versioned_contracts.insert((topoheight, PooledArc::from_ref(hash)), owned);
        Ok(())
    }

    async fn get_last_topoheight_for_contract(&self, hash: &Hash) -> Result<Option<TopoHeight>, BlockchainError> {
        Ok(self.contracts.get(hash).and_then(|c| c.module_pointer))
    }

    async fn get_contract_at_topoheight_for<'a>(&self, hash: &Hash, topoheight: TopoHeight) -> Result<VersionedContractModule<'a>, BlockchainError> {
        let shared = PooledArc::from_ref(hash);
        let stored = self.versioned_contracts.get(&(topoheight, shared))
            .ok_or(BlockchainError::ContractNotFound(hash.clone()))?;
        Ok(Versioned::new(
            stored.get().as_ref().map(|v| Cow::Owned(v.clone())),
            stored.get_previous_topoheight(),
        ))
    }

    async fn get_contract_at_maximum_topoheight_for<'a>(&self, hash: &Hash, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedContractModule<'a>)>, BlockchainError> {
        let Some(entry) = self.contracts.get(hash) else { return Ok(None); };
        let Some(pointer) = entry.module_pointer else { return Ok(None); };
        let shared = PooledArc::from_ref(hash);
        let mut topo = Some(pointer);
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(stored) = self.versioned_contracts.get(&(t, shared.clone())) {
                    let module = Versioned::new(
                        stored.get().as_ref().map(|v| Cow::Owned(v.clone())),
                        stored.get_previous_topoheight(),
                    );
                    return Ok(Some((t, module)));
                }
            }
            topo = self.versioned_contracts.get(&(t, shared.clone()))
                .and_then(|m| m.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn get_contracts<'a>(&'a self, minimum_topoheight: Option<TopoHeight>, maximum_topoheight: Option<TopoHeight>) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        Ok(self.contracts.iter()
            .filter(move |(_, entry)| {
                if let Some(pointer) = entry.module_pointer {
                    if minimum_topoheight.is_some_and(|min| pointer < min) {
                        return false;
                    }
                    if maximum_topoheight.is_some_and(|max| pointer > max) {
                        return false;
                    }
                    true
                } else {
                    false
                }
            })
            .map(|(hash, _)| Ok(hash.as_ref().clone()))
        )
    }

    async fn delete_last_topoheight_for_contract(&mut self, hash: &Hash) -> Result<(), BlockchainError> {
        if let Some(entry) = self.contracts.get_mut(hash) {
            entry.module_pointer = None;
        }
        Ok(())
    }

    async fn has_contract(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        let Some(entry) = self.contracts.get(hash) else { return Ok(false); };
        let Some(pointer) = entry.module_pointer else { return Ok(false); };
        let shared = PooledArc::from_ref(hash);
        Ok(self.versioned_contracts.get(&(pointer, shared))
            .map_or(false, |v| v.get().is_some()))
    }

    async fn has_contract_pointer(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.contracts.get(hash).map_or(false, |c| c.module_pointer.is_some()))
    }

    async fn has_contract_module_at_topoheight(&self, hash: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let shared = PooledArc::from_ref(hash);
        Ok(self.versioned_contracts.get(&(topoheight, shared))
            .map_or(false, |v| v.get().is_some()))
    }

    async fn has_contract_at_exact_topoheight(&self, hash: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let shared = PooledArc::from_ref(hash);
        Ok(self.versioned_contracts.contains_key(&(topoheight, shared)))
    }

    async fn has_contract_at_maximum_topoheight(&self, hash: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let Some(entry) = self.contracts.get(hash) else { return Ok(false); };
        let Some(pointer) = entry.module_pointer else { return Ok(false); };
        let shared = PooledArc::from_ref(hash);
        let mut t = Some(pointer);
        while let Some(current) = t {
            if current <= topoheight {
                return Ok(self.versioned_contracts.get(&(current, shared.clone()))
                    .map_or(false, |v| v.get().is_some()));
            }
            t = self.versioned_contracts.get(&(current, shared.clone()))
                .and_then(|m| m.get_previous_topoheight());
        }
        Ok(false)
    }

    async fn count_contracts(&self) -> Result<u64, BlockchainError> {
        Ok(self.contracts.len() as u64)
    }

    async fn add_tx_for_contract(&mut self, contract: &Hash, tx: &Hash) -> Result<(), BlockchainError> {
        self.contract_transactions.insert((PooledArc::from_ref(contract), PooledArc::from_ref(tx)));
        Ok(())
    }

    async fn get_contract_transactions<'a>(&'a self, contract: &Hash) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        let shared = PooledArc::from_ref(contract);
        Ok(self.contract_transactions.range((shared.clone(), PooledArc::from_ref(&Hash::zero()))..=(shared, PooledArc::from_ref(&Hash::max())))
            .map(|(_, hash)| Ok(hash.as_ref().clone()))
        )
    }
}

// ---- ContractStorage (xelis_common) ----

#[async_trait]
impl ContractStorage for MemoryStorage {
    async fn load_data(&self, contract: &Hash, key: &xelis_vm::ValueCell, topoheight: TopoHeight) -> Result<Option<(TopoHeight, Option<xelis_vm::ValueCell>)>, anyhow::Error> {
        let res = self.get_contract_data_at_maximum_topoheight_for(contract, key, topoheight).await?;
        Ok(res.map(|(topo, data)| (topo, data.take())))
    }

    async fn load_data_latest_topoheight(&self, contract: &Hash, key: &xelis_vm::ValueCell, topoheight: TopoHeight) -> Result<Option<TopoHeight>, anyhow::Error> {
        let res = self.get_contract_data_topoheight_at_maximum_topoheight_for(contract, key, topoheight).await?;
        Ok(res)
    }

    async fn has_contract(&self, contract: &Hash, topoheight: TopoHeight) -> Result<bool, anyhow::Error> {
        let res = ContractProvider::has_contract_at_maximum_topoheight(self, contract, topoheight).await?;
        Ok(res)
    }
}

// ---- ContractInfoProvider (xelis_common ContractProvider) ----

#[async_trait]
impl ContractInfoProvider for MemoryStorage {
    async fn get_contract_balance_for_asset(&self, contract: &Hash, asset: &Hash, topoheight: TopoHeight) -> Result<Option<(TopoHeight, u64)>, anyhow::Error> {
        let res = self.get_contract_balance_at_maximum_topoheight(contract, asset, topoheight).await?;
        Ok(res.map(|(topo, balance)| (topo, balance.take())))
    }

    async fn get_account_balance_for_asset(&self, key: &PublicKey, asset: &Hash, topoheight: TopoHeight) -> Result<Option<(TopoHeight, xelis_common::account::CiphertextCache)>, anyhow::Error> {
        let res = self.get_balance_at_maximum_topoheight(key, asset, topoheight).await?;
        Ok(res.map(|(topo, balance)| (topo, balance.take_balance())))
    }

    async fn has_scheduled_execution_at_topoheight(&self, contract: &Hash, topoheight: TopoHeight) -> Result<bool, anyhow::Error> {
        let res = ContractScheduledExecutionProvider::has_contract_scheduled_execution_at_topoheight(self, contract, topoheight).await?;
        Ok(res)
    }

    async fn asset_exists(&self, asset: &Hash, topoheight: TopoHeight) -> Result<bool, anyhow::Error> {
        let res = self.is_asset_registered_at_maximum_topoheight(asset, topoheight).await?;
        Ok(res)
    }

    async fn load_asset_data(&self, asset: &Hash, topoheight: TopoHeight) -> Result<Option<(TopoHeight, AssetData)>, anyhow::Error> {
        let res = self.get_asset_at_maximum_topoheight(asset, topoheight).await?;
        Ok(res.map(|(topo, v)| (topo, v.take())))
    }

    async fn load_asset_circulating_supply(&self, asset: &Hash, topoheight: TopoHeight) -> Result<(TopoHeight, u64), anyhow::Error> {
        self.get_circulating_supply_for_asset_at_maximum_topoheight(asset, topoheight).await?
            .map(|(topo, v)| (topo, v.take()))
            .context("Asset circulating supply not found")
    }

    async fn account_exists(&self, key: &PublicKey, topoheight: TopoHeight) -> Result<bool, anyhow::Error> {
        let res = self.is_account_registered_for_topoheight(key, topoheight).await?;
        Ok(res)
    }

    async fn load_contract_module(&self, contract: &Hash, topoheight: TopoHeight) -> Result<Option<(TopoHeight, Option<ContractModule>)>, anyhow::Error> {
        let res = self.get_contract_at_maximum_topoheight_for(contract, topoheight).await?;
        Ok(res.map(|(topo, module)| (topo, module.take().map(|v| v.into_owned()))))
    }

    async fn has_contract_callback_for_event(&self, contract: &Hash, event_id: u64, listener: &Hash, topoheight: TopoHeight) -> Result<bool, anyhow::Error> {
        let res = self.get_event_callback_for_contract_at_maximum_topoheight(contract, event_id, listener, topoheight).await?;
        Ok(res.is_some_and(|(_, v)| v.get().is_some()))
    }
}
