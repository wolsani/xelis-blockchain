use pooled_arc::PooledArc;
use async_trait::async_trait;
use xelis_common::{
    account::{AccountSummary, Balance, VersionedBalance},
    block::TopoHeight,
    crypto::{Hash, PublicKey},
};
use crate::core::{
    error::BlockchainError,
    storage::BalanceProvider,
};
use super::super::MemoryStorage;

#[async_trait]
impl BalanceProvider for MemoryStorage {
    async fn has_balance_for(&self, key: &PublicKey, asset: &Hash) -> Result<bool, BlockchainError> {
        if !self.accounts.contains_key(key) {
            return Ok(false)
        }

        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        Ok(self.balance_pointers.get(&shared_key).and_then(|m| m.get(&shared_asset)).is_some())
    }

    async fn has_balance_at_exact_topoheight(&self, key: &PublicKey, asset: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        Ok(self.versioned_balances.contains_key(&(topoheight, shared_key, shared_asset)))
    }

    async fn get_balance_at_exact_topoheight(&self, key: &PublicKey, asset: &Hash, topoheight: TopoHeight) -> Result<VersionedBalance, BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        self.versioned_balances.get(&(topoheight, shared_key, shared_asset))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_balance_at_maximum_topoheight(&self, key: &PublicKey, asset: &Hash, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedBalance)>, BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        let mut topo = if self.versioned_balances.contains_key(&(maximum_topoheight, shared_key.clone(), shared_asset.clone())) {
            Some(maximum_topoheight)
        } else {
            self.balance_pointers.get(&shared_key).and_then(|m| m.get(&shared_asset)).copied()
        };

        while let Some(t) = topo {
            if t <= maximum_topoheight {
                if let Some(balance) = self.versioned_balances.get(&(t, shared_key.clone(), shared_asset.clone())) {
                    return Ok(Some((t, balance.clone())));
                }
            }
            topo = self.versioned_balances.get(&(t, shared_key.clone(), shared_asset.clone()))
                .and_then(|b| b.get_previous_topoheight());
        }

        Ok(None)
    }

    async fn get_last_topoheight_for_balance(&self, key: &PublicKey, asset: &Hash) -> Result<TopoHeight, BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        self.balance_pointers.get(&shared_key)
            .and_then(|m| m.get(&shared_asset))
            .copied()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_new_versioned_balance(&self, key: &PublicKey, asset: &Hash, topoheight: TopoHeight) -> Result<(VersionedBalance, bool), BlockchainError> {
        match self.get_balance_at_maximum_topoheight(key, asset, topoheight).await? {
            Some((topo, mut version)) => {
                version.prepare_new(Some(topo));
                Ok((version, false))
            }
            None => Ok((VersionedBalance::zero(), true)),
        }
    }

    async fn get_output_balance_at_maximum_topoheight(&self, key: &PublicKey, asset: &Hash, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedBalance)>, BlockchainError> {
        self.get_output_balance_in_range(key, asset, 0, maximum_topoheight).await
    }

    async fn get_output_balance_in_range(&self, key: &PublicKey, asset: &Hash, minimum_topoheight: TopoHeight, maximum_topoheight: TopoHeight) -> Result<Option<(TopoHeight, VersionedBalance)>, BlockchainError> {
        if !self.accounts.contains_key(key) {
            return Ok(None)
        }

        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        let Some(&pointer) = self.balance_pointers.get(&shared_key).and_then(|m| m.get(&shared_asset)) else {
            return Ok(None);
        };

        let start = if pointer > maximum_topoheight
            && self.versioned_balances.contains_key(&(maximum_topoheight, shared_key.clone(), shared_asset.clone()))
        {
            maximum_topoheight
        } else {
            pointer
        };

        let mut topo = Some(start);
        while let Some(t) = topo {
            if t < minimum_topoheight {
                break;
            }
            if let Some(balance) = self.versioned_balances.get(&(t, shared_key.clone(), shared_asset.clone())) {
                if t <= maximum_topoheight && balance.contains_output() {
                    return Ok(Some((t, balance.clone())));
                }
                topo = balance.get_previous_topoheight();
            } else {
                break;
            }
        }

        Ok(None)
    }

    async fn get_last_balance(&self, key: &PublicKey, asset: &Hash) -> Result<(TopoHeight, VersionedBalance), BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        let topoheight = self.balance_pointers.get(&shared_key)
            .and_then(|m| m.get(&shared_asset))
            .copied()
            .ok_or(BlockchainError::Unknown)?;
        let balance = self.versioned_balances.get(&(topoheight, shared_key, shared_asset))
            .cloned()
            .ok_or(BlockchainError::Unknown)?;
        Ok((topoheight, balance))
    }

    fn set_last_topoheight_for_balance(&mut self, key: &PublicKey, asset: &Hash, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        self.balance_pointers.entry(shared_key).or_default().insert(shared_asset, topoheight);
        Ok(())
    }

    async fn set_last_balance_to(&mut self, key: &PublicKey, asset: &Hash, topoheight: TopoHeight, version: &VersionedBalance) -> Result<(), BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        self.balance_pointers.entry(shared_key.clone()).or_default().insert(shared_asset.clone(), topoheight);
        self.versioned_balances.insert((topoheight, shared_key, shared_asset), version.clone());
        Ok(())
    }

    async fn set_balance_at_topoheight(&mut self, asset: &Hash, topoheight: TopoHeight, key: &PublicKey, balance: &VersionedBalance) -> Result<(), BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        self.versioned_balances.insert((topoheight, shared_key, shared_asset), balance.clone());
        Ok(())
    }

    async fn get_account_summary_for(&self, key: &PublicKey, asset: &Hash, min_topoheight: TopoHeight, max_topoheight: TopoHeight) -> Result<Option<AccountSummary>, BlockchainError> {
        if let Some((topo, version)) = self.get_balance_at_maximum_topoheight(key, asset, max_topoheight).await? {
            if topo < min_topoheight {
                return Ok(None);
            }

            let mut account = AccountSummary {
                output_topoheight: None,
                stable_topoheight: topo,
            };

            if version.contains_output() || version.get_previous_topoheight().is_none() {
                return Ok(Some(account));
            }

            let mut previous = version.get_previous_topoheight();
            let shared_key = PooledArc::from_ref(key);
            let shared_asset = PooledArc::from_ref(asset);
            while let Some(prev_topo) = previous {
                if let Some(balance) = self.versioned_balances.get(&(prev_topo, shared_key.clone(), shared_asset.clone())) {
                    if balance.contains_output() {
                        account.output_topoheight = Some(prev_topo);
                        break;
                    }
                    previous = balance.get_previous_topoheight();
                } else {
                    break;
                }
            }

            return Ok(Some(account));
        }

        Ok(None)
    }

    async fn get_spendable_balances_for(&self, key: &PublicKey, asset: &Hash, min_topoheight: TopoHeight, max_topoheight: TopoHeight, maximum: usize) -> Result<(Vec<Balance>, Option<TopoHeight>), BlockchainError> {
        let shared_key = PooledArc::from_ref(key);
        let shared_asset = PooledArc::from_ref(asset);
        let mut balances = Vec::new();
        let mut next_topo = self.balance_pointers.get(&shared_key).and_then(|m| m.get(&shared_asset)).copied();

        while let Some(topo) = next_topo.take().filter(|&t| t >= min_topoheight && balances.len() < maximum) {
            if topo > max_topoheight {
                if let Some(version) = self.versioned_balances.get(&(topo, shared_key.clone(), shared_asset.clone())) {
                    next_topo = version.get_previous_topoheight();
                }
                continue;
            }
            if let Some(version) = self.versioned_balances.get(&(topo, shared_key.clone(), shared_asset.clone())) {
                let has_output = version.contains_output();
                let previous = version.get_previous_topoheight();
                balances.push(version.clone().as_balance(topo));
                if has_output {
                    break;
                }
                next_topo = previous;
            }
        }

        Ok((balances, next_topo))
    }
}
