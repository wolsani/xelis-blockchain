use async_trait::async_trait;
use futures::stream;
use xelis_common::{
    block::TopoHeight,
    contract::ScheduledExecution,
    crypto::Hash,
};
use futures::Stream;
use crate::core::{
    error::BlockchainError,
    storage::ContractScheduledExecutionProvider,
};
use super::super::super::MemoryStorage;

#[async_trait]
impl ContractScheduledExecutionProvider for MemoryStorage {
    async fn set_contract_scheduled_execution_at_topoheight(&mut self, contract: &Hash, topoheight: TopoHeight, execution: &ScheduledExecution, execution_topoheight: TopoHeight) -> Result<(), BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        self.delayed_executions.insert((execution_topoheight, contract_id), execution.clone());
        self.delayed_execution_registrations.insert((topoheight, contract_id, execution_topoheight), ());
        Ok(())
    }

    async fn has_contract_scheduled_execution_at_topoheight(&self, contract: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        let Some(contract_id) = self.get_optional_contract_id(contract) else { return Ok(false); };
        Ok(self.delayed_executions.contains_key(&(topoheight, contract_id)))
    }

    async fn get_contract_scheduled_execution_at_topoheight(&self, contract: &Hash, topoheight: TopoHeight) -> Result<ScheduledExecution, BlockchainError> {
        let contract_id = self.get_contract_id(contract)?;
        self.delayed_executions.get(&(topoheight, contract_id))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_contract_scheduled_executions_for_execution_topoheight<'a>(&'a self, topoheight: TopoHeight) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_executions.range((topoheight, 0)..=(topoheight, u64::MAX))
            .map(|(&(_, cid), _)| self.get_contract_hash_from_id(cid))
            .collect();
        Ok(entries.into_iter())
    }

    async fn get_registered_contract_scheduled_executions_at_topoheight<'a>(&'a self, topoheight: TopoHeight) -> Result<impl Iterator<Item = Result<(TopoHeight, Hash), BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_execution_registrations.range((topoheight, 0, 0)..=(topoheight, u64::MAX, u64::MAX))
            .map(|(&(_, cid, exec_topo), _)| {
                self.get_contract_hash_from_id(cid).map(|h| (exec_topo, h))
            })
            .collect();
        Ok(entries.into_iter())
    }

    async fn get_contract_scheduled_executions_at_topoheight<'a>(&'a self, topoheight: TopoHeight) -> Result<impl Iterator<Item = Result<ScheduledExecution, BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_executions.range((topoheight, 0)..=(topoheight, u64::MAX))
            .map(|(_, exec)| Ok(exec.clone()))
            .collect();
        Ok(entries.into_iter())
    }

    async fn get_registered_contract_scheduled_executions_in_range<'a>(&'a self, minimum_topoheight: TopoHeight, maximum_topoheight: TopoHeight, min_execution_topoheight: Option<TopoHeight>) -> Result<impl Stream<Item = Result<(TopoHeight, TopoHeight, ScheduledExecution), BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_execution_registrations.range((minimum_topoheight, 0, 0)..=(maximum_topoheight, u64::MAX, u64::MAX))
            .rev()
            .filter_map(move |(&(reg_topo, cid, exec_topo), _)| {
                if min_execution_topoheight.is_some_and(|min| exec_topo < min) {
                    return None;
                }
                let exec = self.delayed_executions.get(&(exec_topo, cid))?.clone();
                Some(Ok((exec_topo, reg_topo, exec)))
            })
            .collect();
        Ok(stream::iter(entries))
    }
}
