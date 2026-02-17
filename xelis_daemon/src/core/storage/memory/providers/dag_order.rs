use async_trait::async_trait;
use xelis_common::{
    block::TopoHeight,
    crypto::Hash,
};
use crate::core::{
    error::BlockchainError,
    storage::DagOrderProvider,
};
use super::super::MemoryStorage;

#[async_trait]
impl DagOrderProvider for MemoryStorage {
    async fn get_topo_height_for_hash(&self, hash: &Hash) -> Result<TopoHeight, BlockchainError> {
        self.topo_by_hash.get(hash)
            .copied()
            .ok_or(BlockchainError::Unknown)
    }

    async fn set_topo_height_for_block(&mut self, hash: &Hash, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.topo_by_hash.insert(hash.clone(), topoheight);
        self.hash_at_topo.insert(topoheight, hash.clone());
        Ok(())
    }

    async fn is_block_topological_ordered(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        let Some(&topo) = self.topo_by_hash.get(hash) else {
            return Ok(false);
        };
        let Some(stored_hash) = self.hash_at_topo.get(&topo) else {
            return Ok(false);
        };
        Ok(*stored_hash == *hash)
    }

    async fn get_hash_at_topo_height(&self, topoheight: TopoHeight) -> Result<Hash, BlockchainError> {
        self.hash_at_topo.get(&topoheight)
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn has_hash_at_topoheight(&self, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        Ok(self.hash_at_topo.contains_key(&topoheight))
    }

    async fn get_orphaned_blocks<'a>(&'a self) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        Ok(self.blocks.keys()
            .filter(|hash| !self.topo_by_hash.contains_key(hash))
            .cloned()
            .map(Ok))
    }
}
