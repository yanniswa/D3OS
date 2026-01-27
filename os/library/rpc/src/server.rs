#![allow(unused_imports)]

extern crate alloc;

use crate::consts::{MAX_PAYLOAD_SIZE, SERVER_CLOSE_DELAY_MS};
use crate::error::RpcError;
use crate::handlers;
use crate::io_helpers::{read_exact, write_exact};
use alloc::vec;
use alloc::vec::Vec;
use capnp::message::ReaderOptions;
use capnp::serialize;
use concurrent::thread;
use core::sync::atomic::{AtomicBool, Ordering};
use log::{debug, error, info, trace, warn};
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
                let res = mkfifo("/myrpcpiperequest");
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
            match open("/myrpcpiperequest", OpenOptions::READONLY) {
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

            // Read 4-byte length prefix
            let mut len_buf = [0u8; 4];
            match read_exact(request_fh, &mut len_buf) {
                Ok(()) => {
                    terminal::println!("Server: read 4-byte length prefix successfully");
                }
                Err(RpcError::UnexpectedEof) | Err(RpcError::ReadFailed) => {
                    // EOF or ReadFailed means no writer - close and re-open pipe to block until next writer
                    // This prevents busy-looping and properly waits for the next client
                    terminal::println!("Server: No writer detected - closing and re-opening pipe...");
                    let _ = close(request_fh);

                    // Re-open will block until a new writer opens the pipe
                    terminal::println!("Server: re-opening pipe, will block until next writer...");
                    match open("/myrpcpiperequest", OpenOptions::READONLY) {
                        Ok(new_fh) => {
                            request_fh = new_fh;
                            terminal::println!("Server: new writer connected, resuming...");
                            continue;
                        }
                        Err(e) => {
                            terminal::println!("Server: FATAL - failed to re-open request pipe: {:?}", e);
                            return Err(RpcError::PipeOpenFailed);
                        }
                    }
                }
                Err(RpcError::Timeout) => {
                    // Timeout waiting for data - just continue waiting
                    terminal::println!("Server: read timeout, continuing to wait for requests...");
                    continue;
                }
                Err(e) => {
                    terminal::println!("Server: FATAL - error reading from request pipe: {:?}", e);
                    let _ = close(request_fh);
                    return Err(e);
                }
            }

            let payload_len = u32::from_le_bytes(len_buf) as usize;

            if payload_len == 0 || payload_len > MAX_PAYLOAD_SIZE {
                error!("Invalid payload length: {} (max: {})", payload_len, MAX_PAYLOAD_SIZE);
                continue;
            }

            // Allocate buffer for payload
            // TODO: Add proper heap space check to prevent OOM
            let mut buf = vec![0u8; payload_len];
            match read_exact(request_fh, &mut buf) {
                Ok(()) => {}
                Err(RpcError::UnexpectedEof) => {
                    warn!("EOF while reading payload - incomplete request");
                    continue;
                }
                Err(e) => {
                    error!("Failed to read payload: {:?}, continuing server loop", e);
                    continue;
                }
            }

            // Parse Cap'n Proto message and dispatch to appropriate method
            if buf.len() > 0 {
                let mut slice: &[u8] = &buf[..];
                match serialize::read_message_from_flat_slice(&mut slice, ReaderOptions::new()) {
                    Ok(message_reader) => {
                        // Try new RpcRequest format first
                        match message_reader.get_root::<hello_capnp::rpc_request::Reader>() {
                            Ok(req) => {
                                let reply_path = req.get_reply_path().unwrap_or("");

                                // Dispatch based on method
                                match req.get_method().which() {
                                    Ok(hello_capnp::rpc_request::method::SayHello(params)) => {
                                        match params {
                                            Ok(p) => {
                                                let name: &str = p.get_name().unwrap_or("unknown");
                                                debug!("RPC Request: sayHello('{}'')", name);

                                                // Call handler and build response
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
                                        }
                                    }
                                    Ok(hello_capnp::rpc_request::method::Add(params)) => {
                                        match params {
                                            Ok(p) => {
                                                let a: i32 = p.get_a();
                                                let b: i32 = p.get_b();
                                                debug!("RPC Request: add({}, {})", a, b);

                                                // Call handler and build response
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
                                        }
                                    }
                                    Err(e) => {
                                        error!("Unknown method in RPC request: {:?}", e);
                                        Self::send_error(reply_path, "Unknown method");
                                    }
                                }
                            }
                            Err(_) => {
                                // Fall back to legacy HelloRequest format for backwards compatibility
                                match message_reader.get_root::<hello_capnp::hello_request::Reader>() {
                                    Ok(req) => {
                                        let name = req.get_name().unwrap_or("unknown");
                                        debug!("RPC Request (legacy): say_hello('{}')", name);
                                        // Legacy format doesn't have reply_path - cannot send response
                                        warn!("Received legacy HelloRequest without reply path - cannot send response");
                                    }
                                    Err(e) => error!("Failed to parse request: {:?}", e),
                                }
                            }
                        }
                    }
                    Err(e) => error!("Failed to deserialize message: {:?}", e),
                }
            }
            // Keep request_fh open for next request - don't close it here
            terminal::println!("Server: request processed, looping back for next request...");
        }
    }

    /// Helper function to send a successful RPC response
    fn send_response<F>(reply_path: &str, build_response: F)
    where
        F: FnOnce(&mut capnp::message::Builder<capnp::message::HeapAllocator>),
    {
        match open(reply_path, OpenOptions::WRITEONLY) {
            Ok(reply_fh) => {
                let mut msg_builder = capnp::message::Builder::new_default();
                build_response(&mut msg_builder);

                let mut tmp_buf = Vec::new();
                if let Err(e) = serialize::write_message(&mut tmp_buf, &msg_builder) {
                    error!("Failed to serialize response: {:?}", e);
                    let _ = close(reply_fh);
                    return;
                }

                if let Err(e) = write_exact(reply_fh, &tmp_buf) {
                    error!("Failed to write response: {:?}", e);
                    let _ = close(reply_fh);
                    return;
                }

                thread::sleep(SERVER_CLOSE_DELAY_MS);
                let _ = close(reply_fh);
                debug!("RPC Response sent: {} bytes", tmp_buf.len());
            }
            Err(_) => {
                error!("Failed to open reply pipe: {}", reply_path);
            }
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
