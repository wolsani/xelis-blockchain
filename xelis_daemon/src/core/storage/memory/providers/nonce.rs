use pooled_arc::PooledArc;
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
        let shared_key = PooledArc::from_ref(key);
        Ok(self.versioned_nonces.contains_key(&(shared_key, topoheight)))
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
        let shared_key = PooledArc::from_ref(key);
        let nonce = self.versioned_nonces.get(&(shared_key, topoheight))
            .cloned()
            .ok_or(BlockchainError::Unknown)?;
        Ok((topoheight, nonce))
    }

    async fn get_nonce_at_exact_topoheight(&self, key: &PublicKey, topoheight: TopoHeight) -> Result<VersionedNonce, BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        self.versioned_nonces.get(&(shared_key, topoheight))
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

        let shared_key = PooledArc::from_ref(key);
        let start = if pointer > maximum_topoheight
            && self.versioned_nonces.contains_key(&(shared_key.clone(), maximum_topoheight))
        {
            maximum_topoheight
        } else {
            pointer
        };

        let mut topo = Some(start);
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(nonce) = self.versioned_nonces.get(&(shared_key.clone(), t)) {
                    return Ok(Some((t, nonce.clone())));
                }
            }
            topo = self.versioned_nonces.get(&(shared_key.clone(), t))
                .and_then(|n| n.get_previous_topoheight());
        }

        Ok(None)
    }

    async fn set_last_nonce_to(&mut self, key: &PublicKey, topoheight: TopoHeight, nonce: &VersionedNonce) -> Result<(), BlockchainError> {
        let account = self.get_or_create_account(key);
        account.nonce_pointer = Some(topoheight);
        let shared_key = PooledArc::from_ref(key);
        self.versioned_nonces.insert((shared_key, topoheight), nonce.clone());
        Ok(())
    }
}
