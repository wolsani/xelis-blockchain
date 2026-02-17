use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedNonceProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedNonceProvider for MemoryStorage {
    async fn delete_versioned_nonces_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_nonces.iter()
            .filter(|(&(_, t), _)| t == topoheight)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((aid, _), prev_topo) in keys {
            self.versioned_nonces.remove(&(aid, topoheight));
            if let Some(account_key) = self.account_by_id.get(&aid).cloned() {
                if let Some(account) = self.accounts.get_mut(&account_key) {
                    if account.nonce_pointer.is_some_and(|p| p >= topoheight) {
                        account.nonce_pointer = prev_topo;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_nonces_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_nonces.iter()
            .filter(|(&(_, t), _)| t > topoheight)
            .map(|(k, v)| (*k, v.get_previous_topoheight()))
            .collect();
        for ((aid, t), prev_topo) in keys {
            self.versioned_nonces.remove(&(aid, t));
            if let Some(account_key) = self.account_by_id.get(&aid).cloned() {
                if let Some(account) = self.accounts.get_mut(&account_key) {
                    if account.nonce_pointer.is_none_or(|v| v > topoheight) {
                        account.nonce_pointer = prev_topo.filter(|&v| v <= topoheight);
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_versioned_nonces_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.versioned_nonces.iter()
            .filter(|(&(_, t), _)| t < topoheight)
            .map(|(k, _)| *k)
            .collect();
        for key in keys {
            self.versioned_nonces.remove(&key);
        }
        Ok(())
    }
}
