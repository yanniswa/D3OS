#![allow(unused_imports)]

extern crate alloc;

use crate::consts::{MAX_PAYLOAD_SIZE, SERVER_CLOSE_DELAY_MS};
use crate::error::RpcError;
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

pub mod hello_capnp {
    include!("./hello_capnp.rs");
}

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

        loop {
            // Check for shutdown signal
            if Self::is_shutdown_requested() {
                info!("RPC Server shutting down gracefully (pid={} tid={})", pid, tid);
                return Ok(());
            }
            let res = open("/myrpcpiperequest", OpenOptions::READONLY);
            if res.is_err() {
                // Pipe not ready, yield and retry
                thread::switch();
                continue;
            }
            let fh = res.unwrap();

            // Read 4-byte length prefix
            let mut len_buf = [0u8; 4];
            match read_exact(fh, &mut len_buf) {
                Ok(()) => {}
                Err(RpcError::UnexpectedEof) => {
                    let _ = close(fh);
                    continue;
                }
                Err(e) => {
                    let _ = close(fh);
                    return Err(e);
                }
            }

            let payload_len = u32::from_le_bytes(len_buf) as usize;

            if payload_len == 0 || payload_len > MAX_PAYLOAD_SIZE {
                error!("Invalid payload length: {} (max: {})", payload_len, MAX_PAYLOAD_SIZE);
                let _ = close(fh);
                continue;
            }

            // Allocate buffer for payload
            // TODO: Add proper heap space check to prevent OOM
            let mut buf = vec![0u8; payload_len];
            match read_exact(fh, &mut buf) {
                Ok(()) => {}
                Err(RpcError::UnexpectedEof) => {
                    let _ = close(fh);
                    continue;
                }
                Err(e) => {
                    error!("Failed to read payload: {:?}, continuing server loop", e);
                    let _ = close(fh);
                    continue;
                }
            }

            // Parse Cap'n Proto message
            if buf.len() > 0 {
                let mut slice: &[u8] = &buf[..];
                match serialize::read_message_from_flat_slice(&mut slice, ReaderOptions::new()) {
                    Ok(message_reader) => {
                        match message_reader.get_root::<hello_capnp::hello_request::Reader>() {
                            Ok(req) => {
                                let name = req.get_name().unwrap_or("unknown");
                                debug!("RPC Request: say_hello('{}')", name);

                                // Send reply if reply_path is present
                                if req.has_reply_path() {
                                    if let Ok(reply_path) = req.get_reply_path() {
                                        match open(reply_path, OpenOptions::WRITEONLY) {
                                            Ok(reply_fh) => {
                                                // Build Cap'n Proto HelloResponse
                                                let mut msg_builder = capnp::message::Builder::new_default();
                                                {
                                                    let mut response = msg_builder.init_root::<hello_capnp::hello_response::Builder>();
                                                    response.set_reply("Hallo vom Server");
                                                }

                                                // Serialize to temporary buffer
                                                let mut tmp_buf = Vec::new();
                                                if let Err(e) = serialize::write_message(&mut tmp_buf, &msg_builder) {
                                                    error!("Failed to serialize response: {:?}", e);
                                                    let _ = close(reply_fh);
                                                    // Don't close fh here - it will be closed at loop end
                                                    continue;
                                                }

                                                // Write full buffer using write_exact helper
                                                if let Err(e) = write_exact(reply_fh, &tmp_buf) {
                                                    error!("Failed to write response: {:?}", e);
                                                    let _ = close(reply_fh);
                                                    // Continue serving other requests
                                                    let _ = close(fh);
                                                    continue;
                                                }

                                                // TODO: Remove this sleep workaround. This is a race condition fix
                                                // that should be replaced with proper pipe-close protocol or ACK mechanism.
                                                thread::sleep(SERVER_CLOSE_DELAY_MS);
                                                let _ = close(reply_fh);
                                                debug!("RPC Response sent: {} bytes", tmp_buf.len());
                                            }
                                            Err(_) => {
                                                error!("Failed to open reply pipe");
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => error!("Failed to parse request: {:?}", e),
                        }
                    }
                    Err(e) => error!("Failed to deserialize message: {:?}", e),
                }
            }

            let _ = close(fh);
        }
    }
} //#[allow(unreachable_code)]
//Ok(())
