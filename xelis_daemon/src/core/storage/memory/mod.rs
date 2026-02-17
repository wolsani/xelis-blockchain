mod providers;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use indexmap::IndexSet;
use xelis_common::{
    account::{VersionedBalance, VersionedNonce},
    asset::VersionedAssetData,
    block::{BlockHeader, TopoHeight},
    contract::{ContractLog, ContractModule, ScheduledExecution},
    crypto::{Hash, PublicKey},
    difficulty::{CumulativeDifficulty, Difficulty},
    immutable::Immutable,
    network::Network,
    serializer::Serializer,
    transaction::{MultiSigPayload, Transaction},
    varuint::VarUint,
    versioned_type::Versioned,
};
use xelis_vm::ValueCell;

use crate::core::{
    error::BlockchainError,
    storage::{
        cache::StorageCache,
        types::TopoHeightMetadata,
        ClientProtocolProvider,
        DagOrderProvider,
        DifficultyProvider,
        Storage,
        Tips,
        TransactionProvider,
        VersionedContractBalance,
        VersionedContractData,
        VersionedEventCallbackRegistration,
        VersionedSupply,
    },
};

// Internal account structure
#[derive(Debug, Clone)]
struct AccountEntry {
    id: u64,
    registered_at: Option<TopoHeight>,
    nonce_pointer: Option<TopoHeight>,
    multisig_pointer: Option<TopoHeight>,
}

// Internal asset structure
#[derive(Debug, Clone)]
struct AssetEntry {
    id: u64,
    data_pointer: Option<TopoHeight>,
    supply_pointer: Option<TopoHeight>,
}

// Internal contract structure
#[derive(Debug, Clone)]
struct ContractEntry {
    id: u64,
    module_pointer: Option<TopoHeight>,
}

// Block metadata
#[derive(Debug, Clone)]
struct BlockMetadata {
    difficulty: Difficulty,
    cumulative_difficulty: CumulativeDifficulty,
    covariance: VarUint,
    size_ema: u32,
}

pub struct MemoryStorage {
    network: Network,
    cache: StorageCache,
    concurrency: usize,

    // Top state
    top_topoheight: TopoHeight,
    top_height: u64,
    pruned_topoheight: Option<TopoHeight>,

    // Tips
    tips: Tips,

    // Block data
    blocks: HashMap<Hash, Arc<BlockHeader>>,
    block_metadata: HashMap<Hash, BlockMetadata>,
    blocks_at_height: BTreeMap<u64, IndexSet<Hash>>,
    blocks_count: u64,

    // Transactions
    transactions: HashMap<Hash, Arc<Transaction>>,
    txs_count: u64,

    // DAG order
    topo_by_hash: HashMap<Hash, TopoHeight>,
    hash_at_topo: BTreeMap<TopoHeight, Hash>,

    // TopoHeight metadata
    topoheight_metadata: BTreeMap<TopoHeight, TopoHeightMetadata>,

    // Client protocol
    tx_executed_in_block: HashMap<Hash, Hash>,
    tx_in_blocks: HashMap<Hash, Tips>,

    // Block execution order
    block_execution_order: HashMap<Hash, u64>,
    blocks_execution_count: u64,

    // Accounts
    accounts: HashMap<PublicKey, AccountEntry>,
    account_by_id: HashMap<u64, PublicKey>,
    next_account_id: u64,

    // Versioned nonces: (account_id, topoheight) -> VersionedNonce
    versioned_nonces: BTreeMap<(u64, TopoHeight), VersionedNonce>,

    // Assets
    assets: HashMap<Hash, AssetEntry>,
    asset_by_id: HashMap<u64, Hash>,
    next_asset_id: u64,

    // Versioned assets: (topoheight, asset_id) -> VersionedAssetData
    versioned_assets: BTreeMap<(TopoHeight, u64), VersionedAssetData>,

    // Versioned asset supply: (topoheight, asset_id) -> VersionedSupply
    versioned_assets_supply: BTreeMap<(TopoHeight, u64), VersionedSupply>,

    // Balances: (account_id, asset_id) -> last topoheight
    balance_pointers: HashMap<(u64, u64), TopoHeight>,
    // (topoheight, account_id, asset_id) -> VersionedBalance
    versioned_balances: BTreeMap<(TopoHeight, u64, u64), VersionedBalance>,

    // Prefixed registrations: (topoheight, account_id)
    prefixed_registrations: BTreeMap<(TopoHeight, u64), ()>,

    // Multisig: (topoheight, account_id)
    versioned_multisig: BTreeMap<(TopoHeight, u64), Versioned<Option<MultiSigPayload>>>,

    // Contracts
    contracts: HashMap<Hash, ContractEntry>,
    contract_by_id: HashMap<u64, Hash>,
    next_contract_id: u64,

    // Versioned contracts (modules): (topoheight, contract_id)
    versioned_contracts: BTreeMap<(TopoHeight, u64), Versioned<Option<ContractModule>>>,

    // Contract data tables
    contract_data_table: HashMap<Vec<u8>, u64>,
    contract_data_table_by_id: HashMap<u64, ValueCell>,
    next_contract_data_id: u64,

    // Contract data: (contract_id, data_id) -> last topoheight
    contract_data_pointers: HashMap<(u64, u64), TopoHeight>,
    // (topoheight, contract_id, data_id) -> VersionedContractData
    versioned_contract_data: BTreeMap<(TopoHeight, u64, u64), VersionedContractData>,

    // Contract logs
    contract_logs: HashMap<Hash, Vec<ContractLog>>,

    // Contract balances: (contract_id, asset_id) -> last topoheight
    contract_balance_pointers: HashMap<(u64, u64), TopoHeight>,
    // (topoheight, contract_id, asset_id) -> VersionedContractBalance
    versioned_contract_balances: BTreeMap<(TopoHeight, u64, u64), VersionedContractBalance>,

    // Contract scheduled executions
    delayed_executions: BTreeMap<(TopoHeight, u64), ScheduledExecution>,
    delayed_execution_registrations: BTreeMap<(TopoHeight, u64, TopoHeight), ()>,

    // Contract event callbacks: (contract_id, event_id, listener_id) -> last topoheight
    event_callback_pointers: HashMap<(u64, u64, u64), TopoHeight>,
    // (topoheight, contract_id, event_id, listener_id)
    versioned_event_callbacks: BTreeMap<(TopoHeight, u64, u64, u64), VersionedEventCallbackRegistration>,

    // Contract transactions: (contract_id, tx_hash)
    contract_transactions: BTreeMap<(u64, Hash), ()>,
}

impl MemoryStorage {
    pub fn new(network: Network, concurrency: usize) -> Self {
        Self {
            concurrency,
            network,
            cache: StorageCache::default(),
            top_topoheight: 0,
            top_height: 0,
            pruned_topoheight: None,
            tips: Tips::default(),
            blocks: HashMap::new(),
            block_metadata: HashMap::new(),
            blocks_at_height: BTreeMap::new(),
            blocks_count: 0,
            transactions: HashMap::new(),
            txs_count: 0,
            topo_by_hash: HashMap::new(),
            hash_at_topo: BTreeMap::new(),
            topoheight_metadata: BTreeMap::new(),
            tx_executed_in_block: HashMap::new(),
            tx_in_blocks: HashMap::new(),
            block_execution_order: HashMap::new(),
            blocks_execution_count: 0,
            accounts: HashMap::new(),
            account_by_id: HashMap::new(),
            next_account_id: 0,
            versioned_nonces: BTreeMap::new(),
            assets: HashMap::new(),
            asset_by_id: HashMap::new(),
            next_asset_id: 0,
            versioned_assets: BTreeMap::new(),
            versioned_assets_supply: BTreeMap::new(),
            balance_pointers: HashMap::new(),
            versioned_balances: BTreeMap::new(),
            prefixed_registrations: BTreeMap::new(),
            versioned_multisig: BTreeMap::new(),
            contracts: HashMap::new(),
            contract_by_id: HashMap::new(),
            next_contract_id: 0,
            versioned_contracts: BTreeMap::new(),
            contract_data_table: HashMap::new(),
            contract_data_table_by_id: HashMap::new(),
            next_contract_data_id: 0,
            contract_data_pointers: HashMap::new(),
            versioned_contract_data: BTreeMap::new(),
            contract_logs: HashMap::new(),
            contract_balance_pointers: HashMap::new(),
            versioned_contract_balances: BTreeMap::new(),
            delayed_executions: BTreeMap::new(),
            delayed_execution_registrations: BTreeMap::new(),
            event_callback_pointers: HashMap::new(),
            versioned_event_callbacks: BTreeMap::new(),
            contract_transactions: BTreeMap::new(),
        }
    }

    fn get_account_id(&self, key: &PublicKey) -> Result<u64, BlockchainError> {
        self.accounts.get(key)
            .map(|a| a.id)
            .ok_or_else(|| BlockchainError::AccountNotFound(key.as_address(self.network.is_mainnet())))
    }

    fn get_optional_account_id(&self, key: &PublicKey) -> Option<u64> {
        self.accounts.get(key).map(|a| a.id)
    }

    fn get_or_create_account(&mut self, key: &PublicKey) -> &mut AccountEntry {
        if !self.accounts.contains_key(key) {
            let id = self.next_account_id;
            self.next_account_id += 1;
            let entry = AccountEntry {
                id,
                registered_at: None,
                nonce_pointer: None,
                multisig_pointer: None,
            };
            self.accounts.insert(key.clone(), entry);
            self.account_by_id.insert(id, key.clone());
        }
        self.accounts.get_mut(key).unwrap()
    }

    fn get_asset_id(&self, hash: &Hash) -> Result<u64, BlockchainError> {
        self.assets.get(hash)
            .map(|a| a.id)
            .ok_or_else(|| BlockchainError::AssetNotFound(hash.clone()))
    }

    fn get_optional_asset_id(&self, hash: &Hash) -> Option<u64> {
        self.assets.get(hash).map(|a| a.id)
    }

    fn get_contract_id(&self, hash: &Hash) -> Result<u64, BlockchainError> {
        self.contracts.get(hash)
            .map(|c| c.id)
            .ok_or_else(|| BlockchainError::ContractNotFound(hash.clone()))
    }

    fn get_optional_contract_id(&self, hash: &Hash) -> Option<u64> {
        self.contracts.get(hash).map(|c| c.id)
    }

    fn get_contract_hash_from_id(&self, id: u64) -> Result<Hash, BlockchainError> {
        self.contract_by_id.get(&id)
            .cloned()
            .ok_or(BlockchainError::Unknown)
    }

    fn get_or_create_contract(&mut self, hash: &Hash) -> &mut ContractEntry {
        if !self.contracts.contains_key(hash) {
            let id = self.next_contract_id;
            self.next_contract_id += 1;
            let entry = ContractEntry {
                id,
                module_pointer: None,
            };
            self.contracts.insert(hash.clone(), entry);
            self.contract_by_id.insert(id, hash.clone());
        }
        self.contracts.get_mut(hash).unwrap()
    }

    fn get_contract_data_id(&self, key: &ValueCell) -> Result<u64, BlockchainError> {
        let bytes = key.to_bytes();
        self.contract_data_table.get(&bytes)
            .copied()
            .ok_or(BlockchainError::Unknown)
    }

    fn get_optional_contract_data_id(&self, key: &ValueCell) -> Option<u64> {
        let bytes = key.to_bytes();
        self.contract_data_table.get(&bytes).copied()
    }

    fn get_or_create_contract_data_id(&mut self, key: &ValueCell) -> u64 {
        let bytes = key.to_bytes();
        if let Some(&id) = self.contract_data_table.get(&bytes) {
            id
        } else {
            let id = self.next_contract_data_id;
            self.next_contract_data_id += 1;
            self.contract_data_table.insert(bytes, id);
            self.contract_data_table_by_id.insert(id, key.clone());
            id
        }
    }

    fn get_contract_data_topo_internal(&self, contract_id: u64, data_id: u64, max_topo: TopoHeight) -> Option<TopoHeight> {
        let mut topo = self.contract_data_pointers.get(&(contract_id, data_id)).copied();
        while let Some(t) = topo {
            if t <= max_topo {
                return Some(t);
            }
            topo = self.versioned_contract_data.get(&(t, contract_id, data_id))
                .and_then(|d| d.get_previous_topoheight());
        }
        None
    }
}

// ---- Storage trait ----

#[async_trait]
impl Storage for MemoryStorage {
    async fn delete_block_at_topoheight(&mut self, topoheight: TopoHeight) -> Result<(Hash, Immutable<BlockHeader>, Vec<(Hash, Immutable<Transaction>)>), BlockchainError> {
        let hash = self.get_hash_at_topo_height(topoheight).await?;

        self.hash_at_topo.remove(&topoheight);
        self.topo_by_hash.remove(&hash);
        self.topoheight_metadata.remove(&topoheight);

        let block = self.get_block_header_by_hash(&hash).await?;
        let mut txs = Vec::new();

        for tx_hash in block.get_txs_hashes() {
            if let Ok(tx) = self.get_transaction(tx_hash).await {
                self.unmark_tx_from_executed(tx_hash).await?;
                txs.push((tx_hash.clone(), tx));
            }
        }

        Ok((hash, block, txs))
    }

    async fn get_size_on_disk(&self) -> Result<u64, BlockchainError> {
        Ok(0)
    }

    async fn estimate_size(&self) -> Result<u64, BlockchainError> {
        Ok(0)
    }

    async fn stop(&mut self) -> Result<(), BlockchainError> {
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), BlockchainError> {
        Ok(())
    }
}

