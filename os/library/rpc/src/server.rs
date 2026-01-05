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
use naming::{close, mkfifo, open, read};
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
        let tid = thread.id();
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);

        println!("server_thread (tid={}): start", tid);

        // Persistent accept loop: open the request FIFO, read exactly one
        // framed request (length prefix + payload), process it, then close
        // and reopen. This avoids aborting if a writer closes early.
        const MAX_PAYLOAD: usize = 64 * 1024; // 64 KiB sanity limit

        loop {
            let res = open("/myrpcpiperequest", OpenOptions::READONLY);
            if res.is_err() {
                println!("server_thread (pid={} tid={}): open failed, error: {:?}", pid, tid, res);
                // yield / retry
                let _ = thread::current();
                continue;
            }
            let fh = res.unwrap();
            if fh == 0 {
                println!("server_thread (pid={} tid={}): WARNING: open returned fh=0 - possible fd reuse?", pid, tid);
            }
            println!("server_thread (pid={} tid={}): opened fh={} and waiting for request", pid, tid, fh);

            // read_exact helper: returns Err(0) on EOF, Err(n) on read error
            let mut read_exact = |buf: &mut [u8]| -> Result<(), i32> {
                let mut off = 0usize;
                while off < buf.len() {
                    match read(fh, &mut buf[off..]) {
                        Ok(n) if n > 0 => {
                            println!("server_thread (tid={}): read returned n={} off={}/{} fh={}", tid, n, off, buf.len(), fh);
                            off += n;
                        }
                        Ok(0) => {
                            if off > 0 {
                                // Partial read followed by EOF — retry a few times to allow
                                // the writer to finish delivering the remaining bytes.
                                let mut retries = 0usize;
                                const MAX_RETRIES: usize = 8;
                                println!(
                                    "server_thread (tid={}): partial EOF (off={}), retrying up to {} times fh={}",
                                    tid, off, MAX_RETRIES, fh
                                );
                                let mut got_something = false;
                                while retries < MAX_RETRIES && off < buf.len() {
                                    match read(fh, &mut buf[off..]) {
                                        Ok(n) if n > 0 => {
                                            println!("server_thread (tid={}): retry read returned n={} off={}/{} fh={}", tid, n, off, buf.len(), fh);
                                            off += n;
                                            got_something = true;
                                            break;
                                        }
                                        Ok(0) => {
                                            retries += 1;
                                            // Yield CPU to give writer time to finish
                                            thread::switch();
                                            continue;
                                        }
                                        Err(e) => {
                                            println!("server_thread (tid={}): retry read error: {:?} off={}/{} fh={}", tid, e, off, buf.len(), fh);
                                            return Err(e as i32);
                                        }
                                        _ => {
                                            println!("server_thread (tid={}): retry read unexpected off={}/{} fh={}", tid, off, buf.len(), fh);
                                            return Err(-1);
                                        }
                                    }
                                }
                                if !got_something && off < buf.len() {
                                    println!(
                                        "server_thread (tid={}): EOF persisted after {} retries, partial off={}/{} fh={}",
                                        tid,
                                        retries,
                                        off,
                                        buf.len(),
                                        fh
                                    );
                                    return Err(0);
                                }
                            } else {
                                println!("server_thread (tid={}): read returned 0 (EOF) off={}/{} fh={}", tid, off, buf.len(), fh);
                                return Err(0);
                            }
                        }
                        Err(e) => {
                            println!("server_thread (tid={}): read error: {:?} off={}/{} fh={}", tid, e, off, buf.len(), fh);
                            return Err(e as i32);
                        }
                        _ => {
                            println!(
                                "server_thread (tid={}): read returned unexpected value off={}/{} fh={}",
                                tid,
                                off,
                                buf.len(),
                                fh
                            );
                            return Err(-1);
                        }
                    }
                }
                Ok(())
            };

            // Read 4-byte length prefix
            let mut len_buf = [0u8; 4];
            match read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(0) => {
                    println!("server_thread (tid={}): EOF while reading length, closing and retrying", tid);
                    let _ = close(fh);
                    continue;
                }
                Err(e) => {
                    println!("server_thread (tid={}): read(length) failed: {:?}", tid, e);
                    let _ = close(fh);
                    return Err(e as i32);
                }
            }

            let payload_len = u32::from_le_bytes(len_buf) as usize;
            println!("server_thread (tid={}): incoming payload length = {}", tid, payload_len);

            if payload_len == 0 || payload_len > MAX_PAYLOAD {
                println!("server_thread (tid={}): invalid payload_len={}", tid, payload_len);
                let _ = close(fh);
                continue;
            }

            let mut buf = vec![0u8; payload_len];
            match read_exact(&mut buf) {
                Ok(()) => println!("server_thread (tid={}): read payload bytes = {}", tid, payload_len),
                Err(0) => {
                    println!("server_thread (tid={}): writer closed before payload complete, discarding and reopening", tid);
                    let _ = close(fh);
                    continue;
                }
                Err(e) => {
                    println!("server_thread (tid={}): read(payload) failed: {:?}", tid, e);
                    let _ = close(fh);
                    return Err(e as i32);
                }
            }

            // Diagnostic: hex dump of received payload for debugging
            {
                let preview_len = core::cmp::min(buf.len(), 32);
                let mut s = String::new();
                for b in &buf[..preview_len] {
                    use core::fmt::Write as _;
                    let _ = write!(&mut s, "{:02x}", b);
                }
                println!("server_thread (tid={}): payload hex ({} bytes) = {}", tid, preview_len, s);
            }

            // Try to parse the payload as a Cap'n Proto message using the
            // byte-oriented API: `read_message_from_flat_slice` expects a
            // `&mut &[u8]` pointing to the flat slice of bytes.
            if buf.len() == 0 {
                println!("server_thread (tid={}): empty payload", tid);
            } else {
                println!("server_thread (tid={}): attempting to parse {} bytes as capnp", tid, buf.len());
                let mut slice: &[u8] = &buf[..];
                match serialize::read_message_from_flat_slice(&mut slice, ReaderOptions::new()) {
                    Ok(message_reader) => {
                        println!("server_thread (tid={}): capnp message parsed successfully", tid);
                        let root_result = message_reader.get_root::<hello_capnp::hello_request::Reader>();
                        println!("server_thread (tid={}): get_root() completed, now matching on result...", tid);
                        match root_result {
                            Ok(req) => {
                                println!("server_thread (tid={}): got root as HelloRequest", tid);
                                // Debug: check whether the reply_path field is present
                                if req.has_reply_path() {
                                    match req.get_reply_path() {
                                        Ok(path) => println!("capnp: HelloRequest.reply_path = {}", path),
                                        Err(e) => println!("capnp: HelloRequest.reply_path present but invalid: {:?}", e),
                                    }
                                } else {
                                    println!("capnp (tid={}): HelloRequest has no reply_path field set", tid);
                                }
                                // Also log name if present
                                if req.has_name() {
                                    match req.get_name() {
                                        Ok(n) => println!("capnp (tid={}): HelloRequest.name = {}", tid, n),
                                        Err(e) => println!("capnp (tid={}): HelloRequest.name invalid: {:?}", tid, e),
                                    }
                                } else {
                                    println!("capnp (tid={}): HelloRequest has no name field set", tid);
                                }

                                // Send reply to client if reply_path is present
                                if req.has_reply_path() {
                                    if let Ok(reply_path) = req.get_reply_path() {
                                        println!("server_thread (pid={} tid={}): opening reply pipe: {}", pid, tid, reply_path);
                                        match open(reply_path, OpenOptions::WRITEONLY) {
                                            Ok(reply_fh) => {
                                                println!("server_thread (pid={} tid={}): opened reply_fh={}", pid, tid, reply_fh);

                                                // Build reply message: "Hallo vom Server"
                                                let reply_msg = b"Hallo vom Server";
                                                let reply_len = (reply_msg.len() as u32).to_le_bytes();

                                                // Build length-prefixed buffer
                                                let mut reply_buf: Vec<u8> = Vec::with_capacity(4 + reply_msg.len());
                                                reply_buf.extend_from_slice(&reply_len);
                                                reply_buf.extend_from_slice(reply_msg);

                                                // Write full buffer (similar to writer_thread)
                                                let mut off = 0usize;
                                                while off < reply_buf.len() {
                                                    match naming::write(reply_fh, &reply_buf[off..]) {
                                                        Ok(n) if n > 0 => {
                                                            println!("server_thread: reply write n={} off={}/{}", n, off, reply_buf.len());
                                                            off += n;
                                                        }
                                                        Ok(0) => {
                                                            println!("server_thread: reply write returned 0");
                                                            break;
                                                        }
                                                        Err(e) => {
                                                            println!("server_thread: reply write error: {:?}", e);
                                                            break;
                                                        }
                                                        _ => break,
                                                    }
                                                }

                                                println!("server_thread (pid={} tid={}): reply sent, {} bytes written", pid, tid, off);
                                                let _ = close(reply_fh);
                                                println!("server_thread (pid={} tid={}): closed reply_fh={}", pid, tid, reply_fh);
                                            }
                                            Err(e) => {
                                                println!("server_thread (pid={} tid={}): failed to open reply pipe: {:?}", pid, tid, e);
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => println!("capnp (tid={}): get_root failed: {:?}", tid, e),
                        }
                    }
                    Err(e) => println!("capnp (tid={}): read_message_from_flat_slice failed: {:?}", tid, e),
                }
            }

            let _ = close(fh);
        }
    }
}

//#[allow(unreachable_code)]
//Ok(())
