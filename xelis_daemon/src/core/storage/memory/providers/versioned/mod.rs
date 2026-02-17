mod balance;
mod nonce;
mod multisig;
mod registrations;
mod asset;
mod asset_supply;
mod dag_order;
mod cache;
mod contract;

use crate::core::storage::VersionedProvider;
use super::super::MemoryStorage;

impl VersionedProvider for MemoryStorage {}
