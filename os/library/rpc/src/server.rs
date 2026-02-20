extern crate alloc;

use crate::consts::SERVER_CLOSE_DELAY_MS;
use crate::error::RpcError;
use crate::handlers;
use crate::io_helpers::{read_raw_bytes_from_pipe, write_exact};
use alloc::vec::Vec;
use capnp::message::ReaderOptions;
use capnp::serialize;
use concurrent::thread;
use core::sync::atomic::{AtomicBool, Ordering};
use log::{debug, error, info};
use naming::shared_types::OpenOptions;
use naming::{close, mkfifo, open};

use crate::hello_capnp;

pub struct RpcServer {}

static SERVER_STARTED: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

impl RpcServer {
    pub const fn new() -> Self {
        RpcServer {}
    }

    pub fn init() {
        // Use compare_exchange to atomically check and set the started flag
        // This prevents race conditions when multiple threads call init()
        match SERVER_STARTED.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => {
                // We successfully transitioned from false to true
                info!("Starting RPC Server");
                let res = mkfifo(crate::consts::REQUEST_PIPE_PATH);
                if res.is_err() {
                    error!("mkfifo failed: {:?}, server cannot start", res);
                    SERVER_STARTED.store(false, Ordering::SeqCst);
                    return;
                }
                let _ = Self::run_pipe_server();
            }
            Err(_) => {
                // Server already started by another thread
                debug!("init() called but server already started");
            }
        }
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

    pub fn run_pipe_server() -> Result<(), RpcError> {
        let thread_handle = thread::current().unwrap();
        let tid = thread_handle.id();
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);

        info!("RPC Server started (pid={} tid={})", pid, tid);

        // Open request pipe once and keep it open for all requests
        // This prevents deadlock where server blocks on open() while client waits for response
        // Made mutable to allow re-opening when all writers disconnect
        let mut request_fh = loop {
            match open(crate::consts::REQUEST_PIPE_PATH, OpenOptions::READONLY) {
                Ok(fh) => break fh,
                Err(_) => {
                    // Pipe not ready, yield and retry
                    thread::switch();
                    continue;
                }
            }
        };

        info!("RPC Server: request pipe opened, ready to accept requests");

        loop {
            // Check for shutdown signal
            if Self::is_shutdown_requested() {
                info!("RPC Server shutting down gracefully (pid={} tid={})", pid, tid);
                let _ = close(request_fh);
                return Ok(());
            }

            // Read next Cap'n Proto message directly from the pipe using
            // the standard framing (same as client send and server send_response).
            // read_message blocks until data is available or returns an error.
            let flat = match read_raw_bytes_from_pipe(request_fh) {
                Ok(b) => b,
                Err(RpcError::UnexpectedEof) | Err(RpcError::ReadFailed) => {
                    // No writer on the pipe — close and re-open to block until next client.
                    debug!("Server: pipe EOF/ReadFailed — re-opening request pipe");
                    let _ = close(request_fh);
                    match open(crate::consts::REQUEST_PIPE_PATH, OpenOptions::READONLY) {
                        Ok(new_fh) => {
                            request_fh = new_fh;
                            continue;
                        }
                        Err(_) => return Err(RpcError::PipeOpenFailed),
                    }
                }
                Err(RpcError::Timeout) => {
                    continue;
                }
                Err(e) => {
                    error!("Server: fatal read error: {:?}", e);
                    let _ = close(request_fh);
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
                                Self::send_response(reply_path, |msg_builder| {
                                    let response = msg_builder.init_root::<hello_capnp::rpc_response::Builder>();
                                    let mut result = response.get_result().init_say_hello_result();
                                    result.set_greeting(&greeting);
                                });
                            }
                            Err(e) => {
                                error!("Failed to get sayHello params: {:?}", e);
                                Self::send_error(reply_path, "Invalid sayHello parameters");
                            }
                        },
                        Ok(hello_capnp::rpc_request::method::Add(params)) => match params {
                            Ok(p) => {
                                let a: i32 = p.get_a();
                                let b: i32 = p.get_b();
                                debug!("RPC Request: add({}, {})", a, b);
                                let sum = handlers::add(a, b);
                                Self::send_response(reply_path, |msg_builder| {
                                    let response = msg_builder.init_root::<hello_capnp::rpc_response::Builder>();
                                    let mut result = response.get_result().init_add_result();
                                    result.set_sum(sum);
                                });
                                debug!("RPC: add({}, {}) = {}", a, b, sum);
                            }
                            Err(e) => {
                                error!("Failed to get add params: {:?}", e);
                                Self::send_error(reply_path, "Invalid add parameters");
                            }
                        },
                        Err(e) => {
                            error!("Unknown method in RPC request: {:?}", e);
                            Self::send_error(reply_path, "Unknown method");
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

    /// Helper function to send a successful RPC response
    fn send_response<F>(reply_path: &str, build_response: F)
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

        // Use the same pipe-open / write / close pattern as writer_thread so
        // the client receive side sees identical Cap'n Proto framing.
        match open(reply_path, OpenOptions::WRITEONLY) {
            Ok(reply_fh) => {
                if let Err(e) = write_exact(reply_fh, &tmp_buf) {
                    error!("Failed to write response: {:?}", e);
                }
                thread::sleep(SERVER_CLOSE_DELAY_MS);
                let _ = close(reply_fh);
                debug!("RPC Response sent: {} bytes to {}", tmp_buf.len(), reply_path);
            }
            Err(_) => error!("Failed to open reply pipe: {}", reply_path),
        }
    }

    /// Helper function to send an error RPC response
    fn send_error(reply_path: &str, error_msg: &str) {
        Self::send_response(reply_path, |msg_builder| {
            let response = msg_builder.init_root::<hello_capnp::rpc_response::Builder>();
            response.get_result().set_error(error_msg);
        });
    }
}
