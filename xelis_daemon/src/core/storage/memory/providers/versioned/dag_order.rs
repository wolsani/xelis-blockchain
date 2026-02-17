use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedDagOrderProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl VersionedDagOrderProvider for MemoryStorage {
    async fn delete_dag_order_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        if let Some(hash) = self.hash_at_topo.remove(&topoheight) {
            self.topo_by_hash.remove(&hash);
        }
        Ok(())
    }

    async fn delete_dag_order_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let keys: Vec<_> = self.hash_at_topo.range((topoheight + 1)..)
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        for (topo, hash) in keys {
            self.hash_at_topo.remove(&topo);
            self.topo_by_hash.remove(&hash);
        }
        Ok(())
    }
}
