use async_trait::async_trait;
use crate::core::{
    error::BlockchainError,
    storage::{SnapshotProvider, snapshot::Snapshot},
};
use super::super::{MemoryColumn, MemoryStorage};

#[async_trait]
impl SnapshotProvider for MemoryStorage {
    type Column = MemoryColumn;

    async fn has_snapshot(&self) -> Result<bool, BlockchainError> {
        Ok(self.snapshot.is_some())
    }

    async fn start_snapshot(&mut self) -> Result<(), BlockchainError> {
        if self.snapshot.is_some() {
            return Err(BlockchainError::CommitPointAlreadyStarted);
        }
        self.snapshot = Some(Snapshot::new(self.cache.clone_mut()));
        Ok(())
    }

    fn end_snapshot(&mut self, apply: bool) -> Result<(), BlockchainError> {
        let snapshot = self.snapshot.take()
            .ok_or(BlockchainError::CommitPointNotStarted)?;

        if apply {
            self.cache = snapshot.cache;
        }

        Ok(())
    }

    fn swap_snapshot(&mut self, other: Option<Snapshot<MemoryColumn>>) -> Result<Option<Snapshot<MemoryColumn>>, BlockchainError> {
        Ok(std::mem::replace(&mut self.snapshot, other))
    }
}
