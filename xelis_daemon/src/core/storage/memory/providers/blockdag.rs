use async_trait::async_trait;
use xelis_common::{
    block::{BlockHeader, TopoHeight},
    crypto::Hash,
    immutable::Immutable,
};
use crate::core::{
    error::BlockchainError,
    storage::{
        BlockDagProvider,
        DagOrderProvider,
        DifficultyProvider,
        types::TopoHeightMetadata,
    },
};
use super::super::MemoryStorage;

#[async_trait]
impl BlockDagProvider for MemoryStorage {
    async fn get_block_header_at_topoheight(&self, topoheight: TopoHeight) -> Result<(Hash, Immutable<BlockHeader>), BlockchainError> {
        let hash = self.get_hash_at_topo_height(topoheight).await?;
        let header = self.get_block_header_by_hash(&hash).await?;
        Ok((hash, header))
    }

    async fn get_block_reward_at_topo_height(&self, topoheight: TopoHeight) -> Result<u64, BlockchainError> {
        self.topoheight_metadata.get(&topoheight)
            .map(|m| m.block_reward)
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_emitted_supply_at_topo_height(&self, topoheight: TopoHeight) -> Result<u64, BlockchainError> {
        self.topoheight_metadata.get(&topoheight)
            .map(|m| m.emitted_supply)
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_metadata_at_topoheight(&self, topoheight: TopoHeight) -> Result<TopoHeightMetadata, BlockchainError> {
        self.topoheight_metadata.get(&topoheight)
            .copied()
            .ok_or(BlockchainError::Unknown)
    }

    async fn set_metadata_at_topoheight(&mut self, topoheight: TopoHeight, metadata: TopoHeightMetadata) -> Result<(), BlockchainError> {
        self.topoheight_metadata.insert(topoheight, metadata);
        Ok(())
    }
}
