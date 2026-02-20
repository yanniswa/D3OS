/// by `call_method()` and `parse_response()`.
use crate::client::serializer::RpcSerializer;
use crate::consts::MAX_RESPONSE_SIZE;
use crate::error::RpcError;
use crate::hello_capnp;
use crate::transport::ClientTransport;
extern crate alloc;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::sync::atomic::{AtomicU64, Ordering};
use log::{debug, error};
use naming::{mkfifo, unlink};

// Global monotonic counter for generating unique reply paths
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
pub struct HelloClient<T: ClientTransport> {
    transport: T,
    serializer: RpcSerializer,
}

impl<T: ClientTransport> HelloClient<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            serializer: RpcSerializer::new(),
        }
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

        let _ = mkfifo(&reply_path);

        let bytes = self.serializer.serialize_request(&reply_path, build_request)?;
        if !bytes.is_empty() {
            self.transport.send(crate::consts::REQUEST_PIPE_PATH, &bytes)?;
        }

        // Receive response
        let mut out = [0u8; MAX_RESPONSE_SIZE];
        let n = self.transport.receive(&mut out, &reply_path)?;

        // Clean up the reply pipe from the naming service to free its memory.
        let _ = unlink(&reply_path);

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
        self.serializer.deserialize_response(&response_bytes, method_name, extractor)
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
