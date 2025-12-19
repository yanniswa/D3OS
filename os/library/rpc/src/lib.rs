#![no_std]

use core::fmt;

// Include generated capability wrapper (contains `pub mod hello_service { ... }`).
// Using `include!` keeps the generated file untouched and exposes the module at crate root.

// Expose transport module (contains the `Transport` trait) so submodules can import it.
pub mod transport;
pub mod server;
pub mod pipe_transport;
pub use pipe_transport::PipeTransport;

// Load client modules from the `client/` folder and re-export `HelloClient`.
pub mod client { pub mod client_stub; }
pub use client::client_stub::HelloClient;

/// A view into a message buffer without taking ownership (zero-copy).
/// The buffer is borrowed as `&[u8]` and any lifetime rules apply to the caller.
pub struct MessageView<'a> {
    buf: &'a [u8],
}

impl<'a> MessageView<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Return the inner bytes slice.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.buf
    }
}

impl<'a> fmt::Debug for MessageView<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MessageView(len={})", self.buf.len())
    }
}


/// High-level client API (very small for skeleton).
pub trait Client {
    /// Call a remote method with zero-copy request view; the response will be
    /// written into `out` and the number of bytes written returned.
    fn call(&self, method: u16, req: &MessageView<'_>, out: &mut [u8]) -> Result<usize, i32>;
}

/// High-level server handler trait.
pub trait Handler {
    /// handle a single request message; return number of bytes written to `out`.
    fn handle(&self, req: &MessageView<'_>, out: &mut [u8]) -> Result<usize, i32>;
}


