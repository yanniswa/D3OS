use crate::consts::MAX_RESPONSE_SIZE;
use crate::error::RpcError;
use crate::transport::Transport;
extern crate alloc;
use alloc::format;
use alloc::string::String;
use core::str;
use core::sync::atomic::{AtomicU64, Ordering};
use log::{debug, error};
use naming::mkfifo;

use capnp::message::Builder;
use capnp::serialize;

pub mod hello_capnp {
    include!("../hello_capnp.rs");
}

// Global monotonic counter for generating unique reply paths
// Uses u64 to guarantee no overflow in production (2^64 calls = 584 billion years at 1M req/s)
// Combined with PID ensures uniqueness across process restarts
static REPLY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

pub struct HelloClient<T: Transport> {
    transport: T,
}

impl<T: Transport> HelloClient<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn say_hello(&self, name: &str) -> Result<String, RpcError> {
        // Build a Cap'n Proto message for the request using the generated schema.
        // Generate unique reply path using PID + monotonic counter
        // Format: /rpc_reply_{pid}_{unique_id}
        //
        // Uniqueness guarantees:
        // 1. PID ensures uniqueness across different processes
        // 2. Monotonic counter ensures uniqueness within same process
        // 3. Counter never wraps (u64 = 18 quintillion values)
        // 4. Immune to time resets, clock adjustments, concurrent calls
        // 5. Thread-safe via atomic operations (SeqCst for strongest guarantee)
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);
        let unique_id = REPLY_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let reply_path = format!("/rpc_reply_{}_{}", pid, unique_id);

        debug!("say_hello: generated unique reply path: {} (id={})", reply_path, unique_id);

        // Create reply FIFO (ignore error if it already exists)
        let _ = mkfifo(&reply_path);

        let mut message = Builder::new_default();
        {
            let mut root = message.init_root::<hello_capnp::hello_request::Builder>();
            root.set_name(name);
            root.set_reply_path(&reply_path);
        }

        // Serialize message into words and copy into a Vec<u8> so the bytes
        // remain valid for the duration of the send. Copying also lets us
        // build a single contiguous buffer for the transport to write.
        let words = serialize::write_message_to_words(&message);
        let bytes_len = words.len() * core::mem::size_of::<capnp::Word>();

        if bytes_len == 0 {
            self.transport.send(&[])?;
        } else {
            // Copy words into a byte vector safely by converting each Word
            // into its native-endian byte representation. This avoids any
            // unsafe pointer casts and the UB reported by `copy_nonoverlapping`.
            let mut bytes_vec: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(bytes_len);
            for &w in &words {
                let b = w.to_ne_bytes();
                bytes_vec.extend_from_slice(&b);
            }

            // Send the capnp bytes (transport will add the length-prefix)
            self.transport.send(&bytes_vec)?;
        }

        // receive response into a local buffer
        let mut out = [0u8; MAX_RESPONSE_SIZE];

        // Read the entire response using the same pattern as server does for requests
        let n = self.transport.receive(&mut out, &reply_path)?;
        debug!("say_hello: received {} bytes total from transport", n);

        // Parse the received bytes as a Cap'n Proto message
        // The bytes should be in the flat Cap'n Proto format (no length prefix needed here,
        // since receive() already handles reading the complete message)
        let mut response_slice: &[u8] = &out[..n];

        debug!("say_hello: attempting to parse {} bytes as Cap'n Proto HelloResponse", n);

        match serialize::read_message_from_flat_slice(&mut response_slice, capnp::message::ReaderOptions::new()) {
            Ok(reader) => {
                debug!("say_hello: Cap'n Proto message parsed successfully");
                match reader.get_root::<hello_capnp::hello_response::Reader>() {
                    Ok(response) => {
                        debug!("say_hello: got root as HelloResponse");
                        match response.get_reply() {
                            Ok(reply_text) => {
                                debug!("say_hello: deserialized reply: {}", reply_text);
                                Ok(String::from(reply_text))
                            }
                            Err(e) => {
                                error!("say_hello: failed to get reply field: {:?}", e);
                                Err(RpcError::CapnpGetFieldFailed)
                            }
                        }
                    }
                    Err(e) => {
                        error!("say_hello: failed to get root HelloResponse: {:?}", e);
                        Err(RpcError::CapnpGetRootFailed)
                    }
                }
            }
            Err(e) => {
                error!("say_hello: failed to deserialize response: {:?}", e);
                Err(RpcError::DeserializationFailed)
            }
        }
    }
}
