extern crate alloc;

use crate::error::RpcError;
use crate::handlers;
use crate::server_pipe_transport::ServerPipeTransport;
use crate::transport::ServerTransport;
use alloc::vec::Vec;
use capnp::message::ReaderOptions;
use capnp::serialize;
use concurrent::thread;
use core::sync::atomic::{AtomicBool, Ordering};
use log::{debug, error, info};

use crate::hello_capnp;

pub struct RpcServer<T: ServerTransport> {
    transport: T,
}

static SERVER_STARTED: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

impl<T: ServerTransport> RpcServer<T> {
    /// Create a server with any transport that implements [`ServerTransport`].
    pub fn with_transport(transport: T) -> Self {
        RpcServer { transport }
    }

    pub fn is_started() -> bool {
        SERVER_STARTED.load(Ordering::SeqCst)
    }

    pub fn shutdown() {
        info!("Shutdown requested for RPC Server");
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
    }

    pub fn is_shutdown_requested() -> bool {
        SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
    }

    pub fn run(&mut self) -> Result<(), RpcError> {
        let thread_handle = thread::current().unwrap();
        let tid = thread_handle.id();
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);

        info!("RPC Server started (pid={} tid={})", pid, tid);
        info!("RPC Server: ready to accept requests");

        loop {
            if Self::is_shutdown_requested() {
                info!("RPC Server shutting down gracefully (pid={} tid={})", pid, tid);
                return Ok(());
            }

            let flat = match self.transport.receive_next() {
                Ok(b) => b,
                Err(e) => {
                    error!("Server: fatal transport error: {:?}", e);
                    return Err(e);
                }
            };

            // Deserialize locally — Reader lifetime is bounded to this loop iteration.
            let mut flat_slice: &[u8] = &flat;
            let message_reader = match serialize::read_message_from_flat_slice(&mut flat_slice, ReaderOptions::new()) {
                Ok(r) => r,
                Err(e) => {
                    error!("Server: deserialize failed: {:?}", e);
                    continue;
                }
            };

            // Dispatch the parsed Cap'n Proto message.
            match message_reader.get_root::<hello_capnp::rpc_request::Reader>() {
                Ok(req) => {
                    let reply_path = req.get_reply_path().unwrap_or("");
                    match req.get_method().which() {
                        Ok(hello_capnp::rpc_request::method::SayHello(params)) => match params {
                            Ok(p) => {
                                let name: &str = p.get_name().unwrap_or("unknown");
                                debug!("RPC Request: sayHello('{}')", name);
                                let greeting = handlers::say_hello(name);
                                self.send_response(reply_path, |msg_builder| {
                                    let response = msg_builder.init_root::<hello_capnp::rpc_response::Builder>();
                                    let mut result = response.get_result().init_say_hello_result();
                                    result.set_greeting(&greeting);
                                });
                            }
                            Err(e) => {
                                error!("Failed to get sayHello params: {:?}", e);
                                self.send_error(reply_path, "Invalid sayHello parameters");
                            }
                        },
                        Ok(hello_capnp::rpc_request::method::Add(params)) => match params {
                            Ok(p) => {
                                let a: i32 = p.get_a();
                                let b: i32 = p.get_b();
                                debug!("RPC Request: add({}, {})", a, b);
                                let sum = handlers::add(a, b);
                                self.send_response(reply_path, |msg_builder| {
                                    let response = msg_builder.init_root::<hello_capnp::rpc_response::Builder>();
                                    let mut result = response.get_result().init_add_result();
                                    result.set_sum(sum);
                                });
                                debug!("RPC: add({}, {}) = {}", a, b, sum);
                            }
                            Err(e) => {
                                error!("Failed to get add params: {:?}", e);
                                self.send_error(reply_path, "Invalid add parameters");
                            }
                        },
                        Err(e) => {
                            error!("Unknown method in RPC request: {:?}", e);
                            self.send_error(reply_path, "Unknown method");
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to parse RPC request root: {:?}", e);
                }
            }
            debug!("Server: request processed, waiting for next request");
        }
    }

    fn send_response<F>(&self, reply_path: &str, build_response: F)
    where
        F: FnOnce(&mut capnp::message::Builder<capnp::message::HeapAllocator>),
    {
        let mut msg_builder = capnp::message::Builder::new_default();
        build_response(&mut msg_builder);

        let mut tmp_buf = Vec::new();
        if let Err(e) = serialize::write_message(&mut tmp_buf, &msg_builder) {
            error!("Failed to serialize response: {:?}", e);
            return;
        }

        if let Err(e) = self.transport.send(reply_path, &tmp_buf) {
            error!("Failed to send response to {}: {:?}", reply_path, e);
        } else {
            debug!("RPC Response sent: {} bytes to {}", tmp_buf.len(), reply_path);
        }
    }

    /// Helper function to send an error RPC response
    fn send_error(&self, reply_path: &str, error_msg: &str) {
        self.send_response(reply_path, |msg_builder| {
            let response = msg_builder.init_root::<hello_capnp::rpc_response::Builder>();
            response.get_result().set_error(error_msg);
        });
    }
}
