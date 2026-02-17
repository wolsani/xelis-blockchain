use async_trait::async_trait;
use xelis_common::block::TopoHeight;
use crate::core::{
    error::BlockchainError,
    storage::VersionedScheduledExecutionsProvider,
};
use super::super::super::super::MemoryStorage;

#[async_trait]
impl VersionedScheduledExecutionsProvider for MemoryStorage {
    async fn delete_scheduled_executions_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let reg_keys: Vec<_> = self.delayed_execution_registrations.range((topoheight, 0, 0)..=(topoheight, u64::MAX, u64::MAX))
            .map(|(k, _)| *k)
            .collect();
        for (_, cid, exec_topo) in reg_keys {
            self.delayed_execution_registrations.remove(&(topoheight, cid, exec_topo));
            self.delayed_executions.remove(&(exec_topo, cid));
        }
        Ok(())
    }

    async fn delete_scheduled_executions_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let reg_keys: Vec<_> = self.delayed_execution_registrations.range((topoheight + 1, 0, 0)..)
            .map(|(k, _)| *k)
            .collect();
        for (t, cid, exec_topo) in reg_keys {
            self.delayed_execution_registrations.remove(&(t, cid, exec_topo));
            self.delayed_executions.remove(&(exec_topo, cid));
        }
        Ok(())
    }

    async fn delete_scheduled_executions_below_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let reg_keys: Vec<_> = self.delayed_execution_registrations.range(..(topoheight, 0, 0))
            .map(|(k, _)| *k)
            .collect();
        for (t, cid, exec_topo) in reg_keys {
            self.delayed_execution_registrations.remove(&(t, cid, exec_topo));
            self.delayed_executions.remove(&(exec_topo, cid));
        }
        Ok(())
    }
}
