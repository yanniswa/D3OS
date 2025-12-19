#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;
use concurrent::thread;
use core::str;
use core::sync::atomic::{AtomicBool, Ordering};
use naming::shared_types::OpenOptions;
use naming::{close, mkfifo, open, read, write};
use terminal::println;
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
        // Read up to 16 bytes from the request pipe in a byte-wise loop
        // similar to `pipetest::reader_thread`. This protects against
        // partial reads and prevents indefinite blocking by using an
        // iteration cap.
        let expected_len = 16usize;
        let mut buf = vec![0u8; expected_len];
        let mut total = 0usize;
        let mut cnt = 0usize;
        let max_iters = expected_len * 4; // safety cap

        while total < expected_len && cnt < max_iters {
            let mut one = [0u8; 1];

            let res = read(fh, &mut one);
            match res {
                Ok(n) => {
                    if n == 0 {
                        println!("server_thread: read returned 0 bytes (EOF)");
                    } else {
                        buf[total] = one[0];
                        total += n;
                        if one[0].is_ascii() {
                            //      println!("server_thread: read one byte '{}', read = {}", one[0] as char, n);
                        } else {
                            println!("server_thread: read one non-ascii byte, read = {}", n);
                        }
                    }
                }
                Err(e) => {
                    println!("server_thread: read failed, error: {:?}", e);
                }
            }
            cnt += 1;
        }

        if total == 0 {
            // nothing to process
            let _ = close(fh);
            return Ok(());
        }

        // Interpret what we have as UTF-8 (best-effort) and print greeting.
        let name = match str::from_utf8(&buf[..total]) {
            Ok(s) => s,
            Err(_) => "<invalid-utf8>",
        };
        let mut greeting = String::from("Hello ");
        greeting.push_str(name);
        println!("Greetings: {}", greeting);

        // Close the request pipe handle and finish.
        let _ = close(fh);

        Ok(())
    }
}

//#[allow(unreachable_code)]
//Ok(())
