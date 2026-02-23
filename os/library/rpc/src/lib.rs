#![no_std]

// Cap'n Proto generated code
pub mod schema_capnp {
    include!("../schema/schema_capnp.rs");
}

// Expose centralized constants
pub mod consts;

// Expose error types
pub mod error;
pub use error::RpcError;

// Re-export standard log macros
pub use log::{debug, error, info, trace, warn};

// Expose I/O helpers
pub mod io_helpers;

// Expose RPC method handlers
pub mod handlers;

// Expose transport module (contains Sender, ClientTransport, ServerTransport traits).
pub mod transport;
// Client-side pipe transport (implements Sender + ClientTransport)
pub mod pipe_transport;
// Server-side pipe transport (implements Sender + ServerTransport)
pub mod server;
pub mod server_pipe_transport;
pub use pipe_transport::PipeTransport;
pub use server_pipe_transport::ServerPipeTransport;
pub use transport::{ClientTransport, Sender, ServerTransport};

// Load client modules from the `client/` folder and re-export `HelloClient`.
pub mod client {
    pub mod client_stub;
    pub mod serializer;
}
pub use client::client_stub::HelloClient;
