#![no_std]

// Cap'n Proto generated code
pub mod hello_capnp {
    include!("./hello_capnp.rs");
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

// Expose transport module (contains the `Transport` trait) so submodules can import it.
pub mod pipe_transport;
pub mod server;
pub mod transport;
pub use pipe_transport::PipeTransport;

// Load client modules from the `client/` folder and re-export `HelloClient`.
pub mod client {
    pub mod client_stub;
}
pub use client::client_stub::HelloClient;
