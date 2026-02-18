use pooled_arc::PooledArc;
use async_trait::async_trait;
use futures::stream;
use xelis_common::{
    block::TopoHeight,
    crypto::Hash,
};
use crate::core::storage::VersionedContractData;
use xelis_vm::ValueCell;
use futures::Stream;
use crate::core::{
    error::BlockchainError,
    storage::ContractDataProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl ContractDataProvider for MemoryStorage {
    async fn set_last_contract_data_to(&mut self, contract: &Hash, key: &ValueCell, topoheight: TopoHeight, version: &VersionedContractData) -> Result<(), BlockchainError> {
        let data_key = self.register_data_key(key);
        let shared = PooledArc::from_ref(contract);
        self.versioned_contract_data.insert((topoheight, shared.clone(), data_key.clone()), version.clone());
        self.contract_data_pointers.insert((shared, data_key), topoheight);
        Ok(())
    }

    async fn get_last_topoheight_for_contract_data(&self, contract: &Hash, key: &ValueCell) -> Result<Option<TopoHeight>, BlockchainError> {
        let data_key = Self::get_contract_data_key_bytes(key);
        Ok(self.contract_data_pointers.get(&(PooledArc::from_ref(contract), data_key)).copied())
    }

    async fn get_contract_data_at_exact_topoheight_for<'a>(&self, contract: &Hash, key: &ValueCell, topoheight: TopoHeight) -> Result<VersionedContractData, BlockchainError> {
        let data_key = Self::get_contract_data_key_bytes(key);
        self.versioned_contract_data.get(&(topoheight, PooledArc::from_ref(contract), data_key))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_contract_data_at_maximum_topoheight_for<'a>(&self, contract: &Hash, key: &ValueCell, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedContractData)>, BlockchainError> {
        let data_key = Self::get_contract_data_key_bytes(key);
        let shared = PooledArc::from_ref(contract);
        let mut topo = self.contract_data_pointers.get(&(shared.clone(), data_key.clone())).copied();
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(data) = self.versioned_contract_data.get(&(t, shared.clone(), data_key.clone())) {
                    return Ok(Some((t, data.clone())));
                }
            }
            topo = self.versioned_contract_data.get(&(t, shared.clone(), data_key.clone()))
                .and_then(|d| d.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn get_contract_data_topoheight_at_maximum_topoheight_for<'a>(&self, contract: &Hash, key: &ValueCell, maximum_topoheight: TopoHeight) -> Result<Option<TopoHeight>, BlockchainError> {
        let data_key = Self::get_contract_data_key_bytes(key);
        let shared = PooledArc::from_ref(contract);
        let mut topo = self.contract_data_pointers.get(&(shared.clone(), data_key.clone())).copied();
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                return Ok(Some(t));
            }
            topo = self.versioned_contract_data.get(&(t, shared.clone(), data_key.clone()))
                .and_then(|d| d.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn has_contract_data_at_maximum_topoheight(&self, contract: &Hash, key: &ValueCell, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let data_key = Self::get_contract_data_key_bytes(key);
        let shared = PooledArc::from_ref(contract);
        let Some(topo) = self.get_contract_data_topo_internal(contract, &data_key, topoheight) else {
            return Ok(false);
        };
        Ok(self.versioned_contract_data.get(&(topo, shared, data_key))
            .map_or(false, |d| d.get().is_some()))
    }

    async fn has_contract_data_at_exact_topoheight(&self, contract: &Hash, key: &ValueCell, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let data_key = Self::get_contract_data_key_bytes(key);
        Ok(self.versioned_contract_data.contains_key(&(topoheight, PooledArc::from_ref(contract), data_key)))
    }

    async fn get_contract_data_entries_at_maximum_topoheight<'a>(&'a self, contract: &'a Hash, topoheight: TopoHeight) -> Result<impl Stream<Item = Result<(ValueCell, ValueCell), BlockchainError>> + Send + 'a, BlockchainError> {
        let shared = PooledArc::from_ref(contract);
        let data_keys: Vec<Vec<u8>> = self.contract_data_pointers.keys()
            .filter(|(c, _)| c.as_ref() == contract)
            .map(|(_, dk)| dk.clone())
            .collect();

        let iter = data_keys.into_iter()
            .filter_map(move |data_key| {
                let topo = self.get_contract_data_topo_internal(contract, &data_key, topoheight)?;
                let data = self.versioned_contract_data.get(&(topo, shared.clone(), data_key.clone()))?;
                let value = data.get().as_ref()?.clone();
                let key = self.contract_data_keys.get(&data_key)?.clone();
                Some(Ok((key, value)))
            });

        Ok(stream::iter(iter))
    }
}
