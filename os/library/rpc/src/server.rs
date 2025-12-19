#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use capnp::message::ReaderOptions;
use capnp::serialize;
use concurrent::thread;
use core::str;
use core::sync::atomic::{AtomicBool, Ordering};
use naming::shared_types::OpenOptions;
use naming::{close, mkfifo, open, read, write};
use terminal::println;
pub mod hello_capnp {
    include!("./hello_capnp.rs");
}
pub struct RPCServer {}

// Module-level atomic flag indicating whether the RPC server has been started.
// Using a static atomic avoids needing to keep a singleton instance around
// and works well with the current static-method style of `RPCServer`.
static SERVER_STARTED: AtomicBool = AtomicBool::new(false);

impl RPCServer {
    pub const fn new() -> Self {
        RPCServer {}
    }

    pub fn init() {
        // Avoid starting the server multiple times.
        if SERVER_STARTED.load(Ordering::SeqCst) {
            println!("rpc: init() called but server already started");
        } else {
            println!("Boot RPCServer");
            let res = mkfifo("/myrpcpiperequest");
            if res.is_err() {
                println!("mkfifo failed, error: {:?}", res);
            }
            println!("mkfifo: ok");
        }

        let server_handle = thread::create(|| {
            // Mark as started when the server thread actually begins running.
            SERVER_STARTED.store(true, Ordering::SeqCst);
            Self::run_pipe_server();
            // If run_pipe_server ever returns, clear the flag so init can retry later.
            //SERVER_STARTED.store(false, Ordering::SeqCst);
        });

        if server_handle.is_some() {
            println!("pipe_transport: started server thread");
        } else {
            println!("pipe_transport: server thread create returned None");
        }
    }

    /// Return whether the server has already been started.
    pub fn is_started() -> bool {
        SERVER_STARTED.load(Ordering::SeqCst)
    }

    /// Very small synchronous request handler used for local testing.
    ///
    /// It intentionally does not parse Cap'n Proto messages — for the first
    /// iteration we simply interpret the incoming request bytes as UTF‑8 and
    /// return `b"Hello " + request` as response bytes.
    pub fn handle_request_sync(req: &[u8]) -> Vec<u8> {
        // Try to interpret request as UTF-8, fall back to a placeholder name.
        let name = match str::from_utf8(req) {
            Ok(s) => s,
            Err(_) => "world",
        };

        let mut out = String::from("Hello ");
        out.push_str(name);
        out.into_bytes()
    }

    /// Run a simple pipe-based server loop that listens on `req_pipe` and
    /// replies to client reply pipes indicated inside each framed request.
    pub fn run_pipe_server() -> Result<(), i32> {
        let thread = thread::current().unwrap();

        println!("server_thread (tid={}): start", thread.id());
        let res = open("/myrpcpiperequest", OpenOptions::READONLY);
        if res.is_err() {
            println!("server_thread: open failed, error: {:?}", res);
            return Err(res.unwrap_err() as i32);
        }
        let fh = res.unwrap();
        println!("server_thread (tid={}): start reading", thread.id());
        // First read a 4-byte little-endian length prefix, then read the
        // payload of that exact length. This matches the writer which sends
        // a u32 length before the Cap'n Proto bytes.
        let mut len_buf = [0u8; 4];
        let mut off = 0usize;
        while off < 4 {
            match read(fh, &mut len_buf[off..4]) {
                Ok(n) if n > 0 => off += n,
                Ok(0) => {
                    println!("server_thread: unexpected EOF while reading length");
                    let _ = close(fh);
                    return Ok(());
                }
                Err(e) => {
                    println!("server_thread: read(length) failed: {:?}", e);
                    let _ = close(fh);
                    return Err(e as i32);
                }
                _ => {
                    println!("server_thread: unknown read result while reading length");
                    let _ = close(fh);
                    return Err(-1);
                }
            }
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        println!("server_thread: incoming payload length = {}", payload_len);

        let mut buf = vec![0u8; payload_len];
        let mut got = 0usize;
        while got < payload_len {
            match read(fh, &mut buf[got..]) {
                Ok(n) if n > 0 => got += n,
                Ok(0) => {
                    println!("server_thread: unexpected EOF while reading payload");
                    break;
                }
                Err(e) => {
                    println!("server_thread: read(payload) failed: {:?}", e);
                    let _ = close(fh);
                    return Err(e as i32);
                }
                _ => {
                    println!("server_thread: unknown read result while reading payload");
                    let _ = close(fh);
                    return Err(-2);
                }
            }
        }

        println!("server_thread: read payload bytes = {}", got);

        // Try to parse the payload as a Cap'n Proto message using the
        // byte-oriented API: `read_message_from_flat_slice` expects a
        // `&mut &[u8]` pointing to the flat slice of bytes.
        if got == 0 {
            println!("server_thread: empty payload");
        } else {
            let mut slice: &[u8] = &buf[..got];
            match serialize::read_message_from_flat_slice(&mut slice, ReaderOptions::new()) {
                Ok(message_reader) => match message_reader.get_root::<hello_capnp::hello_request::Reader>() {
                    Ok(req) => match req.get_name() {
                        Ok(name) => println!("capnp: HelloRequest.name = {}", name),
                        Err(_) => println!("capnp: HelloRequest.name missing or invalid"),
                    },
                    Err(e) => println!("capnp: get_root failed: {:?}", e),
                },
                Err(e) => println!("capnp: read_message_from_flat_slice failed: {:?}", e),
            }
        }

        let _ = close(fh);

        Ok(())
    }
}

//#[allow(unreachable_code)]
//Ok(())
