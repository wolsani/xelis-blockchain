use async_trait::async_trait;
use xelis_common::{
    block::TopoHeight,
    crypto::PublicKey,
};
use crate::core::{
    error::BlockchainError,
    storage::AccountProvider,
};
use super::super::MemoryStorage;

#[async_trait]
impl AccountProvider for MemoryStorage {
    async fn count_accounts(&self) -> Result<u64, BlockchainError> {
        Ok(self.next_account_id)
    }

    async fn get_account_registration_topoheight(&self, key: &PublicKey) -> Result<TopoHeight, BlockchainError> {
        self.accounts.get(key)
            .and_then(|a| a.registered_at)
            .ok_or(BlockchainError::UnknownAccount)
    }

    async fn set_account_registration_topoheight(&mut self, key: &PublicKey, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let account = self.get_or_create_account(key);
        account.registered_at = Some(topoheight);
        let id = self.accounts.get(key).unwrap().id;
        self.prefixed_registrations.insert((topoheight, id), ());
        Ok(())
    }

    async fn delete_account_for(&mut self, key: &PublicKey) -> Result<(), BlockchainError> {
        if let Some(account) = self.accounts.get_mut(key) {
            account.registered_at = None;
            account.nonce_pointer = None;
            account.multisig_pointer = None;
        }
        Ok(())
    }

    async fn is_account_registered(&self, key: &PublicKey) -> Result<bool, BlockchainError> {
        Ok(self.accounts.contains_key(key))
    }

    async fn is_account_registered_for_topoheight(&self, key: &PublicKey, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        Ok(self.accounts.get(key)
            .map_or(false, |a| a.registered_at.map_or(false, |t| t <= topoheight)))
    }

    async fn get_registered_keys<'a>(&'a self, minimum_topoheight: Option<TopoHeight>, maximum_topoheight: Option<TopoHeight>) -> Result<impl Iterator<Item = Result<PublicKey, BlockchainError>> + 'a, BlockchainError> {
        Ok(self.accounts.iter()
            .filter(move |(_, account)| {
                let Some(registered_at) = account.registered_at else {
                    return false;
                };
                if minimum_topoheight.is_some_and(|v| registered_at < v) {
                    return false;
                }
                if maximum_topoheight.is_some_and(|v| registered_at > v) {
                    return false;
                }
                true
            })
            .map(|(key, _)| Ok(key.clone()))
        )
    }

    async fn has_key_updated_in_range(&self, key: &PublicKey, minimum_topoheight: TopoHeight, maximum_topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let Some(account) = self.accounts.get(key) else {
            return Ok(false);
        };

        // Check nonce pointer
        if let Some(nonce_pointer) = account.nonce_pointer {
            if nonce_pointer >= minimum_topoheight && nonce_pointer <= maximum_topoheight {
                return Ok(true);
            }
        }

        // Check balance pointers for this account
        for (&(aid, asset_id), &pointer_topo) in &self.balance_pointers {
            if aid != account.id {
                continue;
            }
            let mut topo = Some(pointer_topo);
            while let Some(t) = topo {
                if t < minimum_topoheight {
                    break;
                }
                if t <= maximum_topoheight {
                    return Ok(true);
                }
                topo = self.versioned_balances.get(&(t, account.id, asset_id))
                    .and_then(|b| b.get_previous_topoheight());
            }
        }

        Ok(false)
    }
}
