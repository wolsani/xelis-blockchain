use pooled_arc::PooledArc;
use std::sync::Arc;
use async_trait::async_trait;
use xelis_common::{
    block::{Block, BlockHeader},
    crypto::Hash,
    difficulty::{CumulativeDifficulty, Difficulty},
    immutable::Immutable,
    serializer::Serializer,
    transaction::Transaction,
    varuint::VarUint,
};
use crate::core::{
    error::BlockchainError,
    storage::{
        BlockProvider,
        BlocksAtHeightProvider,
        ClientProtocolProvider,
        DifficultyProvider,
        TransactionProvider,
    },
};
use super::super::{BlockMetadata, MemoryStorage};

#[async_trait]
impl BlockProvider for MemoryStorage {
    async fn has_blocks(&self) -> Result<bool, BlockchainError> {
        Ok(!self.blocks.is_empty())
    }

    async fn count_blocks(&self) -> Result<u64, BlockchainError> {
        Ok(self.blocks_count)
    }

    async fn decrease_blocks_count(&mut self, amount: u64) -> Result<(), BlockchainError> {
        self.blocks_count = self.blocks_count.saturating_sub(amount);
        Ok(())
    }

    async fn has_block_with_hash(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.blocks.contains_key(hash))
    }

    async fn get_block_by_hash(&self, hash: &Hash) -> Result<Block, BlockchainError> {
        let header = self.blocks.get(hash)
            .ok_or(BlockchainError::Unknown)?;

        let mut transactions = Vec::with_capacity(header.get_txs_count());
        for tx_hash in header.get_txs_hashes() {
            let tx = self.get_transaction(tx_hash).await?;
            transactions.push(tx.into_arc());
        }

        Ok(Block::new(header.clone(), transactions))
    }

    async fn get_block_size(&self, hash: &Hash) -> Result<usize, BlockchainError> {
        let header = self.get_block_header_by_hash(hash).await?;
        let mut size = header.size();
        for tx_hash in header.get_txs_hashes() {
            size += self.get_transaction_size(tx_hash).await?;
        }
        Ok(size)
    }

    async fn get_block_size_ema(&self, hash: &Hash) -> Result<u32, BlockchainError> {
        self.block_metadata.get(hash)
            .map(|m| m.size_ema)
            .ok_or(BlockchainError::Unknown)
    }

    async fn save_block(
        &mut self,
        block: Arc<BlockHeader>,
        txs: &[Arc<Transaction>],
        difficulty: Difficulty,
        cumulative_difficulty: CumulativeDifficulty,
        covariance: VarUint,
        size_ema: u32,
        hash: Immutable<Hash>,
    ) -> Result<(), BlockchainError> {
        let mut count_txs = 0u64;
        for (tx_hash, transaction) in block.get_transactions().iter().zip(txs.iter()) {
            if !self.has_transaction(tx_hash).await? {
                self.add_transaction(tx_hash, transaction).await?;
                count_txs += 1;
            }
        }

        self.blocks.insert(PooledArc::from_ref(hash.as_ref()), block.clone());
        self.block_metadata.insert(PooledArc::from_ref(hash.as_ref()), BlockMetadata {
            difficulty,
            cumulative_difficulty,
            covariance,
            size_ema,
        });

        self.add_block_hash_at_height(&hash, block.get_height()).await?;

        if count_txs > 0 {
            self.txs_count += count_txs;
        }

        self.blocks_count += 1;
        Ok(())
    }

    async fn delete_block_by_hash(&mut self, hash: &Hash) -> Result<Immutable<BlockHeader>, BlockchainError> {
        let shared = PooledArc::from_ref(hash);
        let header = self.blocks.remove(&shared)
            .ok_or(BlockchainError::Unknown)?;

        self.remove_block_hash_at_height(hash, header.get_height()).await?;

        for tx in header.get_transactions() {
            self.unlink_transaction_from_block(tx, hash).await?;
        }

        self.block_metadata.remove(&shared);

        Ok(Immutable::Arc(header))
    }
}
