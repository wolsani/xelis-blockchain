use async_trait::async_trait;
use xelis_common::{block::TopoHeight, crypto::Hash, serializer::*, versioned_type::Versioned};

use crate::core::error::BlockchainError;

// Represents an event callback registration
// chunk_id identifies which function chunk to call on the listener contract
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventCallback {
    // Chunk ID to invoke on the listener contract
    pub chunk_id: u64,
    // max_gas is the maximum gas that can be used for this callback
    // it is already paid/reserved at the time of registration
    pub max_gas: u64,
}

impl Serializer for EventCallback {
    fn write(&self, writer: &mut Writer) {
        writer.write_u64(self.chunk_id);
        writer.write_u64(self.max_gas);
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let chunk_id = reader.read_u64()?;
        let max_gas = reader.read_u64()?;
        Ok(EventCallback { chunk_id, max_gas })
    }

    fn size(&self) -> usize {
        16
    }
}

pub type VersionedEventCallback = Versioned<Option<EventCallback>>;

#[async_trait]
pub trait ContractEventCallbackProvider {
    // Register a listener for an event
    // contract: the contract that emits the event
    // event_id: the event identifier to listen to
    // listener_contract: the contract that will receive the callback
    // version: the event callback registration data (chunk_id, max_gas)
    // topoheight: the topoheight at which this registration is made
    async fn set_last_contract_event_callback(
        &mut self,
        contract: &Hash,
        event_id: u64,
        listener_contract: &Hash,
        version: VersionedEventCallback,
        topoheight: TopoHeight
    ) -> Result<(), BlockchainError>;

    async fn get_last_contract_event_callback_topoheight(
        &self,
        contract: &Hash,
        event_id: u64,
        listener_contract: &Hash,
    ) -> Result<Option<TopoHeight>, BlockchainError>;

    // Get all latest versions for a specific contract event 
    // Returns (listener_contract, version) for each latest version
    async fn get_event_callbacks_for_event_at_maximum_topoheight<'a>(
        &'a self,
        contract: &'a Hash,
        event_id: u64,
        max_topoheight: TopoHeight,
    ) -> Result<impl Iterator<Item = Result<(Hash, VersionedEventCallback), BlockchainError>> + Send + 'a, BlockchainError>;
}