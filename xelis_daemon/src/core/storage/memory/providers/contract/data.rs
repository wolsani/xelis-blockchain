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
        let contract_id = self.get_contract_id(contract)?;
        let data_id = self.get_or_create_contract_data_id(key);
        self.versioned_contract_data.insert((topoheight, contract_id, data_id), version.clone());
        self.contract_data_pointers.insert((contract_id, data_id), topoheight);
        Ok(())
    }

    async fn get_last_topoheight_for_contract_data(&self, contract: &Hash, key: &ValueCell) -> Result<Option<TopoHeight>, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else {
            return Ok(None);
        };
        let Some(data_id) = self.get_optional_contract_data_id(key) else {
            return Ok(None);
        };
        Ok(self.contract_data_pointers.get(&(contract_id, data_id)).copied())
    }

    async fn get_contract_data_at_exact_topoheight_for<'a>(&self, contract: &Hash, key: &ValueCell, topoheight: TopoHeight) -> Result<VersionedContractData, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let data_id = self.get_contract_data_id(key)?;
        self.versioned_contract_data.get(&(topoheight, contract_id, data_id))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_contract_data_at_maximum_topoheight_for<'a>(&self, contract: &Hash, key: &ValueCell, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedContractData)>, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else {
            return Ok(None);
        };
        let Some(data_id) = self.get_optional_contract_data_id(key) else {
            return Ok(None);
        };
        let mut topo = self.contract_data_pointers.get(&(contract_id, data_id)).copied();
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(data) = self.versioned_contract_data.get(&(t, contract_id, data_id)) {
                    return Ok(Some((t, data.clone())));
                }
            }
            topo = self.versioned_contract_data.get(&(t, contract_id, data_id))
                .and_then(|d| d.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn get_contract_data_topoheight_at_maximum_topoheight_for<'a>(&self, contract: &Hash, key: &ValueCell, maximum_topoheight: TopoHeight) -> Result<Option<TopoHeight>, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else {
            return Ok(None);
        };
        let Some(data_id) = self.get_optional_contract_data_id(key) else {
            return Ok(None);
        };
        let mut topo = self.contract_data_pointers.get(&(contract_id, data_id)).copied();
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                return Ok(Some(t));
            }
            topo = self.versioned_contract_data.get(&(t, contract_id, data_id))
                .and_then(|d| d.get_previous_topoheight());
        }
        Ok(None)
    }

    async fn has_contract_data_at_maximum_topoheight(&self, contract: &Hash, key: &ValueCell, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else {
            return Ok(false);
        };
        let Some(data_id) = self.get_optional_contract_data_id(key) else {
            return Ok(false);
        };
        let Some(topo) = self.get_contract_data_topo_internal(contract_id, data_id, topoheight) else {
            return Ok(false);
        };
        Ok(self.versioned_contract_data.get(&(topo, contract_id, data_id))
            .map_or(false, |d| d.get().is_some()))
    }

    async fn has_contract_data_at_exact_topoheight(&self, contract: &Hash, key: &ValueCell, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let data_id = self.get_contract_data_id(key)?;
        Ok(self.versioned_contract_data.contains_key(&(topoheight, contract_id, data_id)))
    }

    async fn get_contract_data_entries_at_maximum_topoheight<'a>(&'a self, contract: &'a Hash, topoheight: TopoHeight) -> Result<impl Stream<Item = Result<(ValueCell, ValueCell), BlockchainError>> + Send + 'a, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        let data_ids: Vec<u64> = self.contract_data_pointers.keys()
            .filter(|(cid, _)| *cid == contract_id)
            .map(|(_, did)| *did)
            .collect();

        let iter = data_ids.into_iter()
            .filter_map(move |data_id| {
                let topo = self.get_contract_data_topo_internal(contract_id, data_id, topoheight)?;
                let data = self.versioned_contract_data.get(&(topo, contract_id, data_id))?;
                let value = data.get().as_ref()?.clone();
                let key = self.contract_data_table_by_id.get(&data_id)?.clone();
                Some(Ok((key, value)))
            });

        Ok(stream::iter(iter))
    }
}
