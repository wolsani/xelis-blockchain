use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedBalanceProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedBalanceProvider for MemoryStorage {
    async fn delete_versioned_balances_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_balances.range((topoheight, 0, 0)..=(topoheight, u64::MAX, u64::MAX))
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((t, aid, asid), prev_topo) in keys {
            self.versioned_balances.remove(&(t, aid, asid));
            let pointer = self.balance_pointers.get(&(aid, asid)).copied();
            if pointer.is_some_and(|p| p >= topoheight) {
                if let Some(prev) = prev_topo {
                    self.balance_pointers.insert((aid, asid), prev);
                } else {
                    self.balance_pointers.remove(&(aid, asid));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_balances_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_balances.range((topoheight + 1, 0, 0)..)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((_, aid, asid), prev_topo) in keys {
            self.versioned_balances.remove(&(topoheight + 1, aid, asid));
            let pointer = self.balance_pointers.get(&(aid, asid)).copied();
            if pointer.is_none_or(|v| v > topoheight) {
                let filtered = prev_topo.filter(|&v| v <= topoheight);
                if let Some(p) = filtered {
                    self.balance_pointers.insert((aid, asid), p);
                } else {
                    self.balance_pointers.remove(&(aid, asid));
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_balances_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_balances.range(..(topoheight, 0, 0))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_balances.remove(&key);
        }
        Ok(())
    }
}
