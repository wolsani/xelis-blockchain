use std::sync::Arc;
use async_trait::async_trait;
use futures::stream;
use xelis_common::{
    crypto::Hash,
    immutable::Immutable,
    serializer::Serializer,
    transaction::Transaction,
};
use futures::Stream;
use crate::core::{
    error::BlockchainError,
    storage::TransactionProvider,
};
use super::super::MemoryStorage;

#[async_trait]
impl TransactionProvider for MemoryStorage {
    async fn get_transaction(&self, hash: &Hash) -> Result<Immutable<Transaction>, BlockchainError> {
        self.transactions.get(hash)
            .map(|tx| Immutable::Arc(tx.clone()))
            .ok_or(BlockchainError::Unknown)
    }

    async fn get_transaction_size(&self, hash: &Hash) -> Result<usize, BlockchainError> {
        self.transactions.get(hash)
            .map(|tx| tx.size())
            .ok_or(BlockchainError::Unknown)
    }

    async fn count_transactions(&self) -> Result<u64, BlockchainError> {
        Ok(self.txs_count)
    }

    async fn get_unexecuted_transactions<'a>(&'a self) -> Result<impl Stream<Item = Result<Hash, BlockchainError>> + 'a, BlockchainError> {
        let iter = self.transactions.keys()
            .filter(|hash| !self.tx_executed_in_block.contains_key(hash))
            .cloned()
            .map(Ok);
        Ok(stream::iter(iter))
    }

    async fn has_transaction(&self, hash: &Hash) -> Result<bool, BlockchainError> {
        Ok(self.transactions.contains_key(hash))
    }

    async fn add_transaction(&mut self, hash: &Hash, transaction: &Transaction) -> Result<(), BlockchainError> {
        self.transactions.insert(hash.clone(), Arc::new(transaction.clone()));
        Ok(())
    }

    async fn delete_transaction(&mut self, hash: &Hash) -> Result<Immutable<Transaction>, BlockchainError> {
        let tx = self.transactions.remove(hash)
            .ok_or(BlockchainError::Unknown)?;

        if let Some(contract) = tx.invoked_contract() {
            if let Some(contract_id) = self.get_optional_contract_id(contract) {
                self.contract_transactions.remove(&(contract_id, hash.clone()));
            }
        }

        Ok(Immutable::Arc(tx))
    }
}
