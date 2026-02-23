/// Cap'n Proto serialization layer for the RPC client.
///
/// `RpcSerializer` is the single place in the client that knows about Cap'n Proto.
/// `HelloServiceClient` delegates every serialize/deserialize operation here so that
/// swapping the wire format only requires changing this file.
extern crate alloc;

use alloc::vec::Vec;
use capnp::message::Builder;
use capnp::serialize;
use log::error;

use crate::error::RpcError;
use crate::schema_capnp;

pub struct RpcSerializer;

impl RpcSerializer {
    pub const fn new() -> Self {
        RpcSerializer
    }

    /// Serialize an RPC request to bytes.
    ///
    /// `build_fn` receives a fresh Cap'n Proto `Builder` and the `reply_path`
    /// string so the caller can fill in the schema fields without knowing
    /// anything about the wire format.
    pub fn serialize_request<F>(&self, reply_path: &str, build_fn: F) -> Result<Vec<u8>, RpcError>
    where
        F: FnOnce(&mut Builder<capnp::message::HeapAllocator>, &str),
    {
        let mut message = Builder::new_default();
        build_fn(&mut message, reply_path);

        let mut bytes: Vec<u8> = Vec::new();
        serialize::write_message(&mut bytes, &message).map_err(|_| RpcError::DeserializationFailed)?;
        Ok(bytes)
    }

    /// Deserialize a raw response byte slice and extract a result using `extractor`.
    ///
    /// The lifetime of the Cap'n Proto reader is bound to this call, so the
    /// `extractor` closure must produce an owned value.
    pub fn deserialize_response<R, F>(&self, response_bytes: &[u8], method_name: &str, extractor: F) -> Result<R, RpcError>
    where
        F: FnOnce(schema_capnp::rpc_response::result::Reader) -> Result<R, RpcError>,
    {
        let mut slice = response_bytes;

        let reader = serialize::read_message_from_flat_slice(&mut slice, capnp::message::ReaderOptions::new()).map_err(|e| {
            error!("{}: failed to deserialize response: {:?}", method_name, e);
            RpcError::DeserializationFailed
        })?;

        let response = reader.get_root::<schema_capnp::rpc_response::Reader>().map_err(|e| {
            error!("{}: failed to get root RpcResponse: {:?}", method_name, e);
            RpcError::CapnpGetRootFailed
        })?;

        let result = response.get_result();
        match result.which() {
            Ok(schema_capnp::rpc_response::result::Error(err_text)) => {
                let err = err_text.unwrap_or("unknown error");
                error!("{}: server returned error: {}", method_name, err);
                Err(RpcError::DeserializationFailed)
            }
            Ok(_) => extractor(result),
            Err(e) => {
                error!("{}: unexpected response variant: {:?}", method_name, e);
                Err(RpcError::InvalidMessageFormat)
            }
        }
    }
}
