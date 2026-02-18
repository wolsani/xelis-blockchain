use pooled_arc::PooledArc;
use async_trait::async_trait;
use xelis_common::crypto::Hash;
use crate::core::{
    error::BlockchainError,
    storage::{ClientProtocolProvider, Tips},
};
use super::super::MemoryStorage;

#[async_trait]
impl ClientProtocolProvider for MemoryStorage {
    async fn get_block_executor_for_tx(&self, tx: &Hash) -> Result<Hash, BlockchainError> {
        self.tx_executed_in_block.get(tx)
            .map(|h| h.as_ref().clone())
            .ok_or(BlockchainError::Unknown)
    }

    async fn is_tx_executed_in_a_block(&self, tx: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.tx_executed_in_block.contains_key(tx))
    }

    async fn is_tx_executed_in_block(&self, tx: &Hash, block: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.tx_executed_in_block.get(tx).map_or(false, |h| h.as_ref() == block))
    }

    async fn is_tx_linked_to_blocks(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.tx_in_blocks.contains_key(hash))
    }

    async fn has_block_linked_to_tx(&self, tx: &Hash, block: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.tx_in_blocks.get(tx).map_or(false, |set| set.contains(block)))
    }

    async fn add_block_linked_to_tx_if_not_present(&mut self, tx: &Hash, block: &Hash) -> Result<bool, BlockchainError> {
        let shared_tx = PooledArc::from_ref(tx);
        let set = self.tx_in_blocks.entry(shared_tx).or_default();
        Ok(set.insert(block.clone()))
    }

    async fn unlink_transaction_from_block(&mut self, tx: &Hash, block: &Hash) -> Result<bool, BlockchainError> {
        if let Some(set) = self.tx_in_blocks.get_mut(tx) {
            return Ok(set.remove(block));
        }
        Ok(false)
    }

    async fn get_blocks_for_tx(&self, hash: &Hash) -> Result<Tips, BlockchainError> {
        self.tx_in_blocks.get(hash)
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn mark_tx_as_executed_in_block(&mut self, tx: &Hash, block: &Hash) -> Result<(), BlockchainError> {
        self.tx_executed_in_block.insert(PooledArc::from_ref(tx), PooledArc::from_ref(block));
        Ok(())
    }

    async fn unmark_tx_from_executed(&mut self, tx: &Hash) -> Result<(), BlockchainError> {
        self.tx_executed_in_block.remove(tx);
        Ok(())
    }

    async fn set_blocks_for_tx(&mut self, tx: &Hash, blocks: &Tips) -> Result<(), BlockchainError> {
        self.tx_in_blocks.insert(PooledArc::from_ref(tx), blocks.clone());
        Ok(())
    }
}
