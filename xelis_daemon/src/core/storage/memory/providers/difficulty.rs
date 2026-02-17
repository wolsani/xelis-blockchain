use async_trait::async_trait;
use indexmap::IndexSet;
use xelis_common::{
    block::{BlockHeader, BlockVersion},
    crypto::Hash,
    difficulty::{CumulativeDifficulty, Difficulty},
    immutable::Immutable,
    time::TimestampMillis,
    varuint::VarUint,
};
use crate::core::{
    error::BlockchainError,
    storage::DifficultyProvider,
};
use super::super::MemoryStorage;

#[async_trait]
impl DifficultyProvider for MemoryStorage {
    async fn get_height_for_block_hash(&self, hash: &Hash) -> Result<u64, BlockchainError> {
        let header = self.get_block_header_by_hash(hash).await?;
        Ok(header.get_height())
    }

    async fn get_version_for_block_hash(&self, hash: &Hash) -> Result<BlockVersion, BlockchainError> {
        let header = self.get_block_header_by_hash(hash).await?;
        Ok(header.get_version())
    }

    async fn get_timestamp_for_block_hash(&self, hash: &Hash) -> Result<TimestampMillis, BlockchainError> {
        let header = self.get_block_header_by_hash(hash).await?;
        Ok(header.get_timestamp())
    }

    async fn get_difficulty_for_block_hash(&self, hash: &Hash) -> Result<Difficulty, BlockchainError> {
        self.block_metadata.get(hash)
            .map(|m| m.difficulty.clone())
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_cumulative_difficulty_for_block_hash(&self, hash: &Hash) -> Result<CumulativeDifficulty, BlockchainError> {
        self.block_metadata.get(hash)
            .map(|m| m.cumulative_difficulty.clone())
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_past_blocks_for_block_hash(&self, hash: &Hash) -> Result<Immutable<IndexSet<Hash>>, BlockchainError> {
        let header = self.get_block_header_by_hash(hash).await?;
        Ok(header.get_immutable_tips().clone())
    }

    async fn get_block_header_by_hash(&self, hash: &Hash) -> Result<Immutable<BlockHeader>, BlockchainError> {
        self.blocks.get(hash)
            .map(|h| Immutable::Arc(h.clone()))
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_estimated_covariance_for_block_hash(&self, hash: &Hash) -> Result<VarUint, BlockchainError> {
        self.block_metadata.get(hash)
            .map(|m| m.covariance.clone())
            .ok_or(BlockchainError::Unknown)
    }
}
