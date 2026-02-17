use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedRegistrationsProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedRegistrationsProvider for MemoryStorage {
    async fn delete_versioned_registrations_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.prefixed_registrations.range((topoheight, 0)..=(topoheight, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for (_, aid) in &keys {
            if let Some(account_key) = self.account_by_id.get(aid).cloned() {
                if let Some(account) = self.accounts.get_mut(&account_key) {
                    if account.registered_at.is_some_and(|p| p == topoheight) {
                        account.registered_at = None;
                    }
                }
            }
        }
        for key in keys {
            self.prefixed_registrations.remove(&key);
        }
        Ok(())
    }

    async fn delete_versioned_registrations_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.prefixed_registrations.range((topoheight + 1, 0)..)
            .map(|(k, _)| *k)
            .collect();
        for (_, aid) in &keys {
            if let Some(account_key) = self.account_by_id.get(aid).cloned() {
                if let Some(account) = self.accounts.get_mut(&account_key) {
                    if account.registered_at.is_some_and(|p| p > topoheight) {
                        account.registered_at = None;
                    }
                }
            }
        }
        for key in keys {
            self.prefixed_registrations.remove(&key);
        }
        Ok(())
    }
}
