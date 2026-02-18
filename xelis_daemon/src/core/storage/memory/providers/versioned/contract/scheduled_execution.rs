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
        self.delayed_executions.retain(|&(t, _), _| t != topoheight);
        self.delayed_execution_registrations.retain(|&(t, _, _)| t != topoheight);
        Ok(())
    }

    async fn delete_scheduled_executions_above_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.delayed_executions.retain(|&(t, _), _| t <= topoheight);
        self.delayed_execution_registrations.retain(|&(t, _, _)| t <= topoheight);
        Ok(())
    }

    async fn delete_scheduled_executions_below_topoheight(&mut self, topoheight: TopoHeight) -> Result<(), BlockchainError> {
        self.delayed_executions.retain(|&(t, _), _| t >= topoheight);
        self.delayed_execution_registrations.retain(|&(t, _, _)| t >= topoheight);
        Ok(())
    }
}
