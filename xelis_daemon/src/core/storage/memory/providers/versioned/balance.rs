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
        self.versioned_balances.retain(|&(t, _, _), _| t != topoheight);
        Ok(())
    }

    async fn delete_versioned_balances_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.versioned_balances.retain(|&(t, _, _), _| t <= topoheight);
        Ok(())
    }

    async fn delete_versioned_balances_below_topoheight(&mut self, topoheight: TopoHeight, _keep_last: bool) -> Result<(), BlockchainError> {
        self.versioned_balances.retain(|&(t, _, _), _| t >= topoheight);
        Ok(())
    }
}
