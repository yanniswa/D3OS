use crate::error::RpcError;
extern crate alloc;
use alloc::vec::Vec;

/// Shared sending capability. Both client and server send responses/requests
/// over named pipes, so this is extracted as a common supertrait.
pub trait Sender {
    /// Send a serialized message to the pipe at `path`.
    fn send(&self, path: &str, msg: &[u8]) -> Result<(), RpcError>;
}

/// Transport for the client side: stateless per-call send + receive.
/// Receive opens a unique reply pipe path, reads one message, and closes it.
pub trait ClientTransport: Sender {
    /// Receive a response into `out` from the reply pipe at `reply_path`.
    /// Returns the number of bytes written into `out`.
    fn receive(&self, out: &mut [u8], reply_path: &str) -> Result<usize, RpcError>;
}

/// Transport for the server side: stateful receive with a persistent file handle.
/// `receive_next` blocks until a complete message arrives, transparently
/// re-opening the request pipe after each client disconnects (EOF).
pub trait ServerTransport: Sender {
    /// Block until the next complete request message is available.
    /// Returns the raw serialized bytes.
    fn receive_next(&mut self) -> Result<Vec<u8>, RpcError>;
}
