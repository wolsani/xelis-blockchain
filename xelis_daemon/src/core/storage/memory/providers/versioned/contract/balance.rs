use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedContractBalanceProvider,
};
use super::super::super::super::MemoryStorage;

#[async_trait]
impl VersionedContractBalanceProvider for MemoryStorage {
    async fn delete_versioned_contract_balances_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contract_balances.range((topoheight, 0, 0)..=(topoheight, u64::MAX, u64::MAX))
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((_, cid, asid), prev_topo) in keys {
            self.versioned_contract_balances.remove(&(topoheight, cid, asid));
            let pointer = self.contract_balance_pointers.get(&(cid, asid)).copied();
            if pointer.is_some_and(|p| p >= topoheight) {
                if let Some(prev) = prev_topo {
                    self.contract_balance_pointers.insert((cid, asid), prev);
                } else {
                    self.contract_balance_pointers.remove(&(cid, asid));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contract_balances_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contract_balances.range((topoheight + 1, 0, 0)..)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((t, cid, asid), prev_topo) in keys {
            self.versioned_contract_balances.remove(&(t, cid, asid));
            let pointer = self.contract_balance_pointers.get(&(cid, asid)).copied();
            if pointer.is_none_or(|v| v > topoheight) {
                let filtered = prev_topo.filter(|&v| v <= topoheight);
                if let Some(p) = filtered {
                    self.contract_balance_pointers.insert((cid, asid), p);
                } else {
                    self.contract_balance_pointers.remove(&(cid, asid));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_contract_balances_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_contract_balances.range(..(topoheight, 0, 0))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_contract_balances.remove(&key);
        }
        Ok(())
    }
}
