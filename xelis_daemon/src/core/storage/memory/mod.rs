mod providers;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use indexmap::IndexSet;
use pooled_arc::*;
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
    transaction::Transaction,
    varuint::VarUint,
    versioned_type::Versioned,
};
use xelis_vm::ValueCell;

use crate::core::storage::VersionedMultiSig;
use crate::core::{
    error::BlockchainError,
    storage::{
        cache::ChainCache,
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

#[derive(Clone, Default)]
pub(crate) struct AccountEntry {
    pub balances: HashMap<PooledArc<Hash>, BTreeMap<TopoHeight, VersionedBalance>>,
    pub nonces: BTreeMap<TopoHeight, VersionedNonce>,
    pub multisig: BTreeMap<TopoHeight, VersionedMultiSig<'static>>,
    pub registered_at: Option<TopoHeight>,
}

// Internal asset structure
#[derive(Debug, Clone)]
pub(crate) struct AssetEntry {
    data_pointer: Option<TopoHeight>,
    supply_pointer: Option<TopoHeight>,
}

// Internal contract structure
#[derive(Debug, Clone)]
pub(crate) struct ContractEntry {
    module_pointer: Option<TopoHeight>,
}

// Block metadata
#[derive(Debug, Clone)]
pub(crate) struct BlockMetadata {
    difficulty: Difficulty,
    cumulative_difficulty: CumulativeDifficulty,
    covariance: VarUint,
    size_ema: u32,
}

pub struct MemoryStorage {
    network: Network,
    cache: ChainCache,
    concurrency: usize,

    // Top state
    top_topoheight: TopoHeight,
    top_height: u64,
    pruned_topoheight: Option<TopoHeight>,

    // Tips
    tips: Tips,

    accounts: HashMap<PooledArc<PublicKey>, AccountEntry>,

    // Block data
    blocks: HashMap<PooledArc<Hash>, Arc<BlockHeader>>,
    block_metadata: HashMap<PooledArc<Hash>, BlockMetadata>,
    blocks_at_height: BTreeMap<u64, IndexSet<PooledArc<Hash>>>,
    blocks_count: u64,

    // Transactions
    transactions: HashMap<PooledArc<Hash>, Arc<Transaction>>,
    txs_count: u64,

    // DAG order
    topo_by_hash: HashMap<PooledArc<Hash>, TopoHeight>,
    hash_at_topo: BTreeMap<TopoHeight, PooledArc<Hash>>,

    // TopoHeight metadata
    topoheight_metadata: BTreeMap<TopoHeight, TopoHeightMetadata>,

    // Client protocol
    tx_executed_in_block: HashMap<PooledArc<Hash>, PooledArc<Hash>>,
    tx_in_blocks: HashMap<PooledArc<Hash>, Tips>,

    // Block execution order
    block_execution_order: HashMap<PooledArc<Hash>, u64>,
    blocks_execution_count: u64,

    // Assets: hash -> entry with pointers
    assets: HashMap<PooledArc<Hash>, AssetEntry>,

    // Versioned assets: (topoheight, asset) -> VersionedAssetData
    versioned_assets: HashMap<(TopoHeight, PooledArc<Hash>), VersionedAssetData>,

    // Versioned asset supply: (topoheight, asset) -> VersionedSupply
    versioned_assets_supply: HashMap<(TopoHeight, PooledArc<Hash>), VersionedSupply>,

    // Contracts: hash -> entry with pointers
    contracts: HashMap<PooledArc<Hash>, ContractEntry>,

    // Versioned contracts (modules): (topoheight, contract)
    versioned_contracts: HashMap<(TopoHeight, PooledArc<Hash>), Versioned<Option<ContractModule>>>,

    // Contract data: (contract, data_key_bytes) -> last topoheight
    contract_data_pointers: HashMap<(PooledArc<Hash>, Vec<u8>), TopoHeight>,
    // (topoheight, contract, data_key_bytes) -> VersionedContractData
    versioned_contract_data: HashMap<(TopoHeight, PooledArc<Hash>, Vec<u8>), VersionedContractData>,
    // data_key_bytes -> ValueCell (for reverse lookup when iterating)
    contract_data_keys: HashMap<Vec<u8>, ValueCell>,

    // Contract logs
    contract_logs: HashMap<PooledArc<Hash>, Vec<ContractLog>>,

    // Contract balances: (contract, asset) -> last topoheight
    contract_balance_pointers: HashMap<(PooledArc<Hash>, PooledArc<Hash>), TopoHeight>,
    // (topoheight, contract, asset) -> VersionedContractBalance
    versioned_contract_balances: HashMap<(TopoHeight, PooledArc<Hash>, PooledArc<Hash>), VersionedContractBalance>,

    // Contract scheduled executions: (execution_topoheight, contract)
    delayed_executions: HashMap<(TopoHeight, PooledArc<Hash>), ScheduledExecution>,
    // (registration_topoheight, contract, execution_topoheight)
    delayed_execution_registrations: HashSet<(TopoHeight, PooledArc<Hash>, TopoHeight)>,

    // Contract event callbacks: (contract, event_id, listener_contract) -> last topoheight
    event_callback_pointers: HashMap<(PooledArc<Hash>, u64, PooledArc<Hash>), TopoHeight>,
    // (topoheight, contract, event_id, listener_contract)
    versioned_event_callbacks: HashMap<(TopoHeight, PooledArc<Hash>, u64, PooledArc<Hash>), VersionedEventCallbackRegistration>,

    // Contract transactions: (contract, tx_hash)
    contract_transactions: BTreeSet<(PooledArc<Hash>, PooledArc<Hash>)>,
}

impl MemoryStorage {
    pub fn new(network: Network, concurrency: usize) -> Self {
        Self {
            concurrency,
            network,
            cache: ChainCache::default(),
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
            assets: HashMap::new(),
            versioned_assets: HashMap::new(),
            versioned_assets_supply: HashMap::new(),
            accounts: HashMap::new(),
            contracts: HashMap::new(),
            versioned_contracts: HashMap::new(),
            contract_data_pointers: HashMap::new(),
            versioned_contract_data: HashMap::new(),
            contract_data_keys: HashMap::new(),
            contract_logs: HashMap::new(),
            contract_balance_pointers: HashMap::new(),
            versioned_contract_balances: HashMap::new(),
            delayed_executions: HashMap::new(),
            delayed_execution_registrations: HashSet::new(),
            event_callback_pointers: HashMap::new(),
            versioned_event_callbacks: HashMap::new(),
            contract_transactions: BTreeSet::new(),
        }
    }

    fn get_or_create_contract(&mut self, hash: &Hash) -> &mut ContractEntry {
        let shared_hash = PooledArc::from_ref(hash);
        self.contracts.entry(shared_hash).or_insert_with(|| ContractEntry {
            module_pointer: None,
        })
    }

    fn get_contract_data_key_bytes(key: &ValueCell) -> Vec<u8> {
        Serializer::to_bytes(key)
    }

    fn register_data_key(&mut self, key: &ValueCell) -> Vec<u8> {
        let bytes = Self::get_contract_data_key_bytes(key);
        self.contract_data_keys.entry(bytes.clone()).or_insert_with(|| key.clone());
        bytes
    }

    fn get_contract_data_topo_internal(&self, contract: &Hash, data_key: &[u8], max_topo: TopoHeight) -> Option<TopoHeight> {
        let shared_contract = PooledArc::from_ref(contract);
        let mut topo = self.contract_data_pointers.get(&(shared_contract.clone(), data_key.to_vec())).copied();
        while let Some(t) = topo {
            if t <= max_topo {
                return Some(t);
            }
            topo = self.versioned_contract_data.get(&(t, shared_contract.clone(), data_key.to_vec()))
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
        let shared_hash = PooledArc::from_ref(&hash);

        self.hash_at_topo.remove(&topoheight);
        self.topo_by_hash.remove(&shared_hash);
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

