use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedMultiSigProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedMultiSigProvider for MemoryStorage {
    async fn delete_versioned_multisigs_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_multisig.range((topoheight, 0)..=(topoheight, u64::MAX))
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((_, aid), prev_topo) in keys {
            self.versioned_multisig.remove(&(topoheight, aid));
            if let Some(account_key) = self.account_by_id.get(&aid).cloned() {
                if let Some(account) = self.accounts.get_mut(&account_key) {
                    if account.multisig_pointer.is_some_and(|p| p >= topoheight) {
                        account.multisig_pointer = prev_topo;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_multisigs_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_multisig.range((topoheight + 1, 0)..)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((t, aid), prev_topo) in keys {
            self.versioned_multisig.remove(&(t, aid));
            if let Some(account_key) = self.account_by_id.get(&aid).cloned() {
                if let Some(account) = self.accounts.get_mut(&account_key) {
                    if account.multisig_pointer.is_none_or(|v| v > topoheight) {
                        account.multisig_pointer = prev_topo.filter(|&v| v <= topoheight);
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_multisigs_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_multisig.range(..(topoheight, 0))
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_multisig.remove(&key);
        }
        Ok(())
    }
}
