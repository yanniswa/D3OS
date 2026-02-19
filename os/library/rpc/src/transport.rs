use crate::error::RpcError;

/// Minimal trait for an RPC transport. Implementations should ensure
/// correct synchronization and zero-copy semantics where possible.
pub trait Transport {
    /// Send a message buffer to the given pipe path
    ///
    /// # Arguments
    /// * `path` - Path to the FIFO pipe to write to (e.g. "/myrpcpiperequest")
    /// * `msg`  - Serialized message bytes to send
    fn send(&self, path: &str, msg: &[u8]) -> Result<(), RpcError>;

    /// Receive a message into the provided buffer view; returns the length
    /// of the received message or an error code.
    ///
    /// # Arguments
    /// * `out` - Buffer to receive the message into
    /// * `reply_path` - Path to the reply FIFO pipe to read from
    fn receive<'a>(&self, out: &'a mut [u8], reply_path: &str) -> Result<usize, RpcError>;
}
