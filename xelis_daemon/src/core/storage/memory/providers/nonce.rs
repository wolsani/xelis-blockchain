use async_trait::async_trait;
use xelis_common::{
    account::VersionedNonce,
    block::TopoHeight,
    crypto::PublicKey,
};
use crate::core::{
    error::BlockchainError,
    storage::{NonceProvider, NetworkProvider},
};
use super::super::MemoryStorage;

#[async_trait]
impl NonceProvider for MemoryStorage {
    async fn has_nonce(&self, key: &PublicKey) -> Result<bool, BlockchainError> {
        Ok(self.accounts.get(key).map_or(false, |a| a.nonce_pointer.is_some()))
    }

    async fn has_nonce_at_exact_topoheight(&self, key: &PublicKey, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let id = self.get_account_id(key)?;
        Ok(self.versioned_nonces.contains_key(&(id, topoheight)))
    }

    async fn get_last_topoheight_for_nonce(&self, key: &PublicKey) -> Result<TopoHeight, BlockchainError> {
        self.accounts.get(key)
            .and_then(|a| a.nonce_pointer)
            .ok_or(BlockchainError::UnknownAccount)
    }

    async fn get_last_nonce(&self, key: &PublicKey) -> Result<(TopoHeight, VersionedNonce), BlockchainError> {
        let account = self.accounts.get(key).ok_or(BlockchainError::UnknownAccount)?;
        let topoheight = account.nonce_pointer
            .ok_or_else(|| BlockchainError::NoNonce(key.as_address(self.is_mainnet())))?;
        let nonce = self.versioned_nonces.get(&(account.id, topoheight))
            .cloned()
            .ok_or(BlockchainError::Unknown)?;
        Ok((topoheight, nonce))
    }

    async fn get_nonce_at_exact_topoheight(&self, key: &PublicKey, topoheight: TopoHeight) -> Result<VersionedNonce, BlockchainError> {
        let id = self.get_account_id(key)?;
        self.versioned_nonces.get(&(id, topoheight))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_nonce_at_maximum_topoheight(&self, key: &PublicKey, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedNonce)>, BlockchainError> {
        let Some(account) = self.accounts.get(key) else {
            return Ok(None);
        };
        let Some(pointer) = account.nonce_pointer else {
            return Ok(None);
        };

        let start = if pointer > maximum_topoheight
            && self.versioned_nonces.contains_key(&(account.id, maximum_topoheight))
        {
            maximum_topoheight
        } else {
            pointer
        };

        let mut topo = Some(start);
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(nonce) = self.versioned_nonces.get(&(account.id, t)) {
                    return Ok(Some((t, nonce.clone())));
                }
            }
            topo = self.versioned_nonces.get(&(account.id, t))
                .and_then(|n| n.get_previous_topoheight());
        }

        Ok(None)
    }

    async fn set_last_nonce_to(&mut self, key: &PublicKey, topoheight: TopoHeight, nonce: &VersionedNonce) -> Result<(), BlockchainError> {
        let account = self.get_or_create_account(key);
        let id = account.id;
        account.nonce_pointer = Some(topoheight);
        self.versioned_nonces.insert((id, topoheight), nonce.clone());
        Ok(())
    }
}
