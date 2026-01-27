use crate::consts::MAX_RESPONSE_SIZE;
use crate::error::RpcError;
use crate::transport::Transport;
extern crate alloc;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::str;
use core::sync::atomic::{AtomicU64, Ordering};
use log::{debug, error};
use naming::mkfifo;

use capnp::message::Builder;
use capnp::serialize;

use crate::hello_capnp;

// Global monotonic counter for generating unique reply paths
// Uses u64 to guarantee no overflow in production (2^64 calls = 584 billion years at 1M req/s)
// Combined with PID ensures uniqueness across process restarts
static REPLY_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// RPC Client stub for type-safe remote procedure calls
///
/// # Adding New Methods
///
/// To add a new RPC method, follow this simple pattern:
///
/// ```rust,ignore
/// pub fn my_method(&self, param: SomeType) -> Result<ReturnType, RpcError> {
///     // 1. Build the request
///     let response_bytes = self.call_method(
///         |message, reply_path| {
///             let mut root = message.init_root::<hello_capnp::rpc_request::Builder>();
///             root.set_reply_path(reply_path);
///             let mut params = root.get_method().init_my_method();
///             params.set_param(param);  // Set your parameters
///         },
///         "my_method",
///     )?;
///
///     // 2. Parse the response (just extract your result from the union)
///     self.parse_response(response_bytes, |result| {
///         match result.which() {
///             Ok(hello_capnp::rpc_response::result::MyMethodResult(r)) => {
///                 let value = r?.get_value();
///                 Ok(value)
///             }
///             _ => Err(RpcError::InvalidMessageFormat)
///         }
///     }, "my_method")
/// }
/// ```
///
/// That's it! All the boilerplate (serialization, transport, error handling) is handled
/// by `call_method()` and `parse_response()`.
pub struct HelloClient<T: Transport> {
    transport: T,
}

impl<T: Transport> HelloClient<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Generate unique reply path for this RPC call
    fn generate_reply_path() -> String {
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);
        let unique_id = REPLY_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("/rpc_reply_{}_{}", pid, unique_id)
    }

    /// Send request and wait for response (low-level)
    fn call_method<F>(&self, build_request: F, method_name: &str) -> Result<Vec<u8>, RpcError>
    where
        F: FnOnce(&mut capnp::message::Builder<capnp::message::HeapAllocator>, &str),
    {
        let reply_path = Self::generate_reply_path();
        debug!("{}: generated unique reply path: {}", method_name, reply_path);

        // Create reply FIFO
        let _ = mkfifo(&reply_path);

        let mut message = Builder::new_default();
        build_request(&mut message, &reply_path);

        // Serialize and send
        let words = serialize::write_message_to_words(&message);
        let bytes_len = words.len() * core::mem::size_of::<capnp::Word>();

        if bytes_len > 0 {
            let mut bytes_vec: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(bytes_len);
            for &w in &words {
                bytes_vec.extend_from_slice(&w.to_ne_bytes());
            }
            self.transport.send(&bytes_vec)?;
        }

        // Receive response
        let mut out = [0u8; MAX_RESPONSE_SIZE];
        let n = self.transport.receive(&mut out, &reply_path)?;

        Ok(out[..n].to_vec())
    }

    /// Parse RPC response and extract result using a custom extractor function
    ///
    /// This generic helper eliminates code duplication across all RPC methods.
    ///
    /// # Type Parameters
    /// * `R` - The return type of the RPC method
    /// * `F` - Function that extracts the result from the Cap'n Proto union
    fn parse_response<R, F>(&self, response_bytes: Vec<u8>, extractor: F, method_name: &str) -> Result<R, RpcError>
    where
        F: FnOnce(hello_capnp::rpc_response::result::Reader) -> Result<R, RpcError>,
    {
        let mut response_slice: &[u8] = &response_bytes[..];

        // Deserialize message
        let reader = serialize::read_message_from_flat_slice(&mut response_slice, capnp::message::ReaderOptions::new()).map_err(|e| {
            error!("{}: failed to deserialize response: {:?}", method_name, e);
            RpcError::DeserializationFailed
        })?;

        // Get response root
        let response = reader.get_root::<hello_capnp::rpc_response::Reader>().map_err(|e| {
            error!("{}: failed to get root RpcResponse: {:?}", method_name, e);
            RpcError::CapnpGetRootFailed
        })?;

        // Get result union and check for error
        let result = response.get_result();
        match result.which() {
            Ok(hello_capnp::rpc_response::result::Error(err_text)) => {
                let err = err_text.unwrap_or("unknown error");
                error!("{}: server returned error: {}", method_name, err);
                Err(RpcError::DeserializationFailed)
            }
            Ok(_) => {
                // Delegate to method-specific extractor
                extractor(result)
            }
            Err(e) => {
                error!("{}: unexpected response type: {:?}", method_name, e);
                Err(RpcError::InvalidMessageFormat)
            }
        }
    }

    /// Call sayHello method
    pub fn say_hello(&self, name: &str) -> Result<String, RpcError> {
        debug!("say_hello: calling with name='{}'", name);

        let response_bytes = self.call_method(
            |message, reply_path| {
                let mut root = message.init_root::<hello_capnp::rpc_request::Builder>();
                root.set_reply_path(reply_path);

                // Set method to sayHello with parameters
                let mut say_hello_params = root.get_method().init_say_hello();
                say_hello_params.set_name(name);
            },
            "say_hello",
        )?;

        self.parse_response(
            response_bytes,
            |result| match result.which() {
                Ok(hello_capnp::rpc_response::result::SayHelloResult(r)) => {
                    let reader = r.map_err(|_| RpcError::CapnpGetFieldFailed)?;
                    let greeting: Result<&str, _> = reader.get_greeting();
                    let greeting = greeting.unwrap_or("").to_string();
                    debug!("say_hello: received greeting: {}", greeting);
                    Ok(greeting)
                }
                _ => {
                    error!("say_hello: unexpected result variant");
                    Err(RpcError::InvalidMessageFormat)
                }
            },
            "say_hello",
        )
    }

    /// Call add method
    pub fn add(&self, a: i32, b: i32) -> Result<i32, RpcError> {
        debug!("add: calling with a={}, b={}", a, b);

        let response_bytes = self.call_method(
            |message, reply_path| {
                let mut root = message.init_root::<hello_capnp::rpc_request::Builder>();
                root.set_reply_path(reply_path);

                // Set method to add with parameters
                let mut add_params = root.get_method().init_add();
                add_params.set_a(a);
                add_params.set_b(b);
            },
            "add",
        )?;

        self.parse_response(
            response_bytes,
            |result| match result.which() {
                Ok(hello_capnp::rpc_response::result::AddResult(r)) => {
                    let reader = r.map_err(|_| RpcError::CapnpGetFieldFailed)?;
                    let sum: i32 = reader.get_sum();
                    debug!("add: received sum: {}", sum);
                    Ok(sum)
                }
                _ => {
                    error!("add: unexpected result variant");
                    Err(RpcError::InvalidMessageFormat)
                }
            },
            "add",
        )
    }
}
