use pooled_arc::PooledArc;
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
        let shared = PooledArc::from_ref(contract);
        self.delayed_executions.insert((execution_topoheight, shared.clone()), execution.clone());
        self.delayed_execution_registrations.insert((topoheight, shared, execution_topoheight));
        Ok(())
    }

    async fn has_contract_scheduled_execution_at_topoheight(&self, contract: &Hash, topoheight: TopoHeight) -> Result<bool, BlockchainError> {
        Ok(self.delayed_executions.contains_key(&(topoheight, PooledArc::from_ref(contract))))
    }

    async fn get_contract_scheduled_execution_at_topoheight(&self, contract: &Hash, topoheight: TopoHeight) -> Result<ScheduledExecution, BlockchainError> {
        self.delayed_executions.get(&(topoheight, PooledArc::from_ref(contract)))
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_contract_scheduled_executions_for_execution_topoheight<'a>(&'a self, topoheight: TopoHeight) -> Result<impl Iterator<Item = Result<Hash, BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_executions.iter()
            .filter(move |(&(t, _), _)| t == topoheight)
            .map(|(&(_, ref contract), _)| Ok(contract.as_ref().clone()))
            .collect();
        Ok(entries.into_iter())
    }

    async fn get_registered_contract_scheduled_executions_at_topoheight<'a>(&'a self, topoheight: TopoHeight) -> Result<impl Iterator<Item = Result<(TopoHeight, Hash), BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_execution_registrations.iter()
            .filter(move |&(t, _, _)| *t == topoheight)
            .map(|(_, contract, exec_topo)| Ok((*exec_topo, contract.as_ref().clone())))
            .collect();
        Ok(entries.into_iter())
    }

    async fn get_contract_scheduled_executions_at_topoheight<'a>(&'a self, topoheight: TopoHeight) -> Result<impl Iterator<Item = Result<ScheduledExecution, BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_executions.iter()
            .filter(move |(&(t, _), _)| t == topoheight)
            .map(|(_, exec)| Ok(exec.clone()))
            .collect();
        Ok(entries.into_iter())
    }

    async fn get_registered_contract_scheduled_executions_in_range<'a>(&'a self, minimum_topoheight: TopoHeight, maximum_topoheight: TopoHeight, min_execution_topoheight: Option<TopoHeight>) -> Result<impl Stream<Item = Result<(TopoHeight, TopoHeight, ScheduledExecution), BlockchainError>> + Send + 'a, BlockchainError> {
        let entries: Vec<_> = self.delayed_execution_registrations.iter()
            .filter(move |&(reg_topo, _, exec_topo)| {
                *reg_topo >= minimum_topoheight && *reg_topo <= maximum_topoheight
                    && !min_execution_topoheight.is_some_and(|min| *exec_topo < min)
            })
            .filter_map(move |(_, contract, exec_topo)| {
                let exec = self.delayed_executions.get(&(*exec_topo, contract.clone()))?.clone();
                Some(Ok((*exec_topo, *exec_topo, exec)))
            })
            .collect();
        Ok(stream::iter(entries))
    }
}
