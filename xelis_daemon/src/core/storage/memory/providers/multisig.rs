use pooled_arc::PooledArc;
use std::borrow::Cow;
use async_trait::async_trait;
use xelis_common::{
    block::TopoHeight,
    crypto::PublicKey,
    versioned_type::Versioned,
};
use crate::core::{
    error::BlockchainError,
    storage::{MultiSigProvider, VersionedMultiSig},
};
use super::super::MemoryStorage;

#[async_trait]
impl MultiSigProvider for MemoryStorage {
    async fn get_last_topoheight_for_multisig(&self, key: &PublicKey) -> Result<Option<TopoHeight>, BlockchainError> {
        Ok(self.accounts.get(key)
            .ok_or(BlockchainError::UnknownAccount)?
            .multisig_pointer)
    }

    async fn get_multisig_at_topoheight_for<'a>(&'a self, key: &PublicKey, topoheight: TopoHeight) -> Result<VersionedMultiSig<'a>, BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let stored = self.versioned_multisig.get(&(topoheight, shared_key))
            .ok_or(BlockchainError::MultisigNotFound)?;
        Ok(Versioned::new(
            stored.get().as_ref().map(|v| Cow::Owned(v.clone())),
            stored.get_previous_topoheight(),
        ))
    }

    async fn delete_last_topoheight_for_multisig(&mut self, key: &PublicKey) -> Result<(), BlockchainError> {
        if let Some(account) = self.accounts.get_mut(key) {
            account.multisig_pointer = None;
        }
        Ok(())
    }

    async fn get_multisig_at_maximum_topoheight_for<'a>(&'a self, account: &PublicKey, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedMultiSig<'a>)>, BlockchainError> {
        let acc = self.accounts.get(account).ok_or(BlockchainError::UnknownAccount)?;
        let Some(pointer) = acc.multisig_pointer else {
            return Ok(None);
        };

        let start = if pointer > maximum_topoheight
            && self.versioned_multisig.contains_key(&(maximum_topoheight, PooledArc::from_ref(account)))
        {
            maximum_topoheight
        } else {
            pointer
        };

        let shared_account = PooledArc::from_ref(account);
        let mut topo = Some(start);
        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(stored) = self.versioned_multisig.get(&(t, shared_account.clone())) {
                    let ms = Versioned::new(
                        stored.get().as_ref().map(|v| Cow::Owned(v.clone())),
                        stored.get_previous_topoheight(),
                    );
                    return Ok(Some((t, ms)));
                }
            }
            topo = self.versioned_multisig.get(&(t, shared_account.clone()))
                .and_then(|m| m.get_previous_topoheight());
        }

        Ok(None)
    }

    async fn has_multisig(&self, account: &PublicKey) -> Result<bool, BlockchainError> {
        Ok(self.accounts.get(account)
            .ok_or(BlockchainError::UnknownAccount)?
            .multisig_pointer.is_some())
    }

    async fn has_multisig_at_exact_topoheight(&self, account: &PublicKey, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let shared = PooledArc::from_ref(account);
        Ok(self.versioned_multisig.contains_key(&(topoheight, shared)))
    }

    async fn set_last_multisig_to<'a>(&mut self, key: &PublicKey, topoheight: TopoHeight, multisig: VersionedMultiSig<'a>) -> Result<(), BlockchainError> {
        let account = self.accounts.get_mut(key).ok_or(BlockchainError::UnknownAccount)?;
        account.multisig_pointer = Some(topoheight);
        let owned = Versioned::new(
            multisig.get().as_ref().map(|v| v.clone().into_owned()),
            multisig.get_previous_topoheight(),
        );
        self.versioned_multisig.insert((topoheight, PooledArc::from_ref(key)), owned);
        Ok(())
    }
}
