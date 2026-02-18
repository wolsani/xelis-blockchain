use pooled_arc::PooledArc;
use async_trait::async_trait;
use xelis_common::crypto::Hash;
use crate::core::{
    error::BlockchainError,
    storage::BlockExecutionOrderProvider,
};
use super::super::MemoryStorage;

#[async_trait]
impl BlockExecutionOrderProvider for MemoryStorage {
    async fn get_blocks_execution_order<'a>(&'a self) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        Ok(self.block_execution_order.keys().map(|k| Ok(k.as_ref().clone())))
    }

    async fn get_block_position_in_order(&self, hash: &Hash) -> Result<u64, BlockchainError> {
        self.block_execution_order.get(hash)
            .copied()
            .ok_or(BlockchainError::Unknown)
    }

    async fn has_block_position_in_order(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.block_execution_order.contains_key(hash))
    }

    async fn add_block_execution_to_order(&mut self, hash: &Hash) -> Result<(), BlockchainError> {
        let position = self.blocks_execution_count;
        self.blocks_execution_count += 1;
        let shared = PooledArc::from_ref(hash);
        self.block_execution_order.insert(shared, position);
        Ok(())
    }

    async fn get_blocks_execution_count(&self) -> Result<u64, BlockchainError> {
        Ok(self.blocks_execution_count)
    }

    async fn swap_blocks_executions_positions(&mut self, left: &Hash, right: &Hash) -> Result<(), BlockchainError> {
        let left_pos = self.get_block_position_in_order(left).await?;
        let right_pos = self.get_block_position_in_order(right).await?;
        let shared_left = PooledArc::from_ref(left);
        let shared_right = PooledArc::from_ref(right);
        self.block_execution_order.insert(shared_left, right_pos);
        self.block_execution_order.insert(shared_right, left_pos);
        Ok(())
    }
}
