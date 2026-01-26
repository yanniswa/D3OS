#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use concurrent::thread::{self, sleep};
use naming::shared_types::OpenOptions;
use naming::{close, mkfifo, open, read, write};
use syscall::return_vals::Errno;
use terminal::println;

use crate::server::RPCServer;
/// A simple PipeTransport that implements the `Transport` trait using
/// a well-known request pipe and a per-client reply pipe. Messages are
/// framed as:
/// [u32 total_len][u16 reply_path_len][reply_path bytes][payload bytes]
/// The server must read the reply_path and send the response to it as
/// [u32 resp_len][resp_bytes]
use crate::transport::Transport;
use core::cmp::min;

pub struct PipeTransport {}

impl PipeTransport {
    pub const fn new() -> Self {
        PipeTransport {}
    }
}

fn pipe_server_runner() {
    println!("writer_thread: calling RPCServer::run_pipe_server()...");
    RPCServer::run_pipe_server();
}
fn writer_thread(msg: &[u8]) -> Option<Result<(), i32>> {
    let thread = thread::current().unwrap();
    // Include process id (if available) and thread id in logs for tracing
    let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);
    println!("writer_thread (pid={} tid={}): start, msg.len()={}", pid, thread.id(), msg.len());

    let res = open("/myrpcpiperequest", OpenOptions::WRITEONLY);
    if res.is_err() {
        println!("writer_thread (pid={} tid={}): open failed, error: {:?}", pid, thread.id(), res);
        return Some(Err(res.unwrap_err() as i32));
    }
    let fh = res.unwrap();

    println!("writer_thread (pid={} tid={}): opened fh={}", pid, thread.id(), fh);

    // Build a single contiguous buffer containing [len_prefix | payload].
    // If this buffer is <= PIPE_BUF the kernel will write it atomically
    // (avoiding interleaving with other writers). For larger buffers we
    // still perform a full write loop, but interleaving between writers is
    // possible for those sizes.
    let total_len = msg.len();
    println!("writer_thread (pid={} tid={}): total_len={}", pid, thread.id(), total_len);
    if total_len > (u32::MAX as usize) {
        println!("writer_thread: payload too large {}", total_len);
        let _ = close(fh);
        return Some(Err(-1));
    }
    let len_be = (total_len as u32).to_le_bytes();

    println!("writer_thread (pid={} tid={}): allocating buffer, capacity={}", pid, thread.id(), 4 + total_len);
    let mut full_buf: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(4 + total_len);
    println!("writer_thread (pid={} tid={}): buffer allocated, extending...", pid, thread.id());
    full_buf.extend_from_slice(&len_be);
    full_buf.extend_from_slice(msg);
    println!(
        "writer_thread (pid={} tid={}): buffer ready, full_buf.len()={}",
        pid,
        thread.id(),
        full_buf.len()
    );

    // Helper to write a full buffer (may require multiple write() calls).
    let mut write_full = |buf: &[u8]| -> Result<usize, i32> {
        let mut off = 0usize;
        while off < buf.len() {
            match write(fh, &buf[off..]) {
                Ok(n) if n > 0 => {
                    println!("writer_thread: write returned n={} off={}/{} fh={}", n, off, buf.len(), fh);
                    off += n;
                }
                Ok(0) => {
                    println!("writer_thread: write returned 0 (unexpected) off={}/{} fh={}", off, buf.len(), fh);
                    return Err(-5);
                }
                Err(e) => {
                    println!("writer_thread: write error: {:?} off={}/{} fh={}", e, off, buf.len(), fh);
                    return Err(e as i32);
                }
                _ => {
                    println!("writer_thread: write returned unknown result off={}/{} fh={}", off, buf.len(), fh);
                    return Err(-6);
                }
            }
        }
        Ok(off)
    };

    // Try to perform the write. If the total message fits into PIPE_BUF,
    // the kernel will perform it atomically which prevents interleaving
    // with other writers. Otherwise we still write fully but other
    // writers may interleave.
    const PIPE_BUF: usize = 4096;
    // Diagnostic: log overall len and a short hex preview of the buffer.
    println!("writer_thread: full_buf.len() = {}", full_buf.len());
    {
        let preview_len = core::cmp::min(full_buf.len(), 16);
        let mut s = String::new();
        for b in &full_buf[..preview_len] {
            use core::fmt::Write as _;
            let _ = write!(&mut s, "{:02x}", b);
        }
        println!("writer_thread: preview ({} bytes) = {}", preview_len, s);
    }

    if full_buf.len() <= PIPE_BUF {
        match write_full(&full_buf) {
            Ok(n) => println!("writer_thread: atomic send complete, {} bytes written", n),
            Err(e) => {
                println!("writer_thread: write failed: {:?}", e);
                let _ = close(fh);
                return Some(Err(e));
            }
        }
    } else {
        // For large messages, just use write_full (may be interleaved)
        match write_full(&full_buf) {
            Ok(n) => println!("writer_thread: send complete, {} bytes written", n),
            Err(e) => {
                println!("writer_thread: write failed: {:?}", e);
                let _ = close(fh);
                return Some(Err(e));
            }
        }
    }

    thread::sleep(500);
    match close(fh) {
        Ok(_) => println!("writer_thread: closed fh={}", fh),
        Err(e) => println!("writer_thread: close failed fh={} err={:?}", fh, e),
    }

    None
}
//TODO: mkfifo darf nicht in send passieren, bei mehrfachen send Aufruf --> problem
//TODO: Pipename dynamisch generieren für reply pipe und mit client id versehen
impl Transport for PipeTransport {
    fn send(&self, msg: &[u8]) -> Result<(), i32> {
        // Start the server in a background thread so the client (writer)
        // and the server run concurrently and do not deadlock on FIFO
        // open/read semantics.
        //  RPCServer::init();
        /*  let server_handle = thread::create(|| {
                    pipe_server_runner();
                });
                if server_handle.is_some() {
                    println!("pipe_transport: started server thread");
                } else {
                    println!("pipe_transport: server thread create returned None");
                }
        */
        // Perform the write from this thread (synchronous). If the writer
        // encounters an error, propagate it as Err(i32).
        if let Some(res) = writer_thread(msg) {
            return res;
        }

        Ok(())
    }

    fn receive<'a>(&self, out: &'a mut [u8]) -> Result<usize, i32> {
        let thread = thread::current().unwrap();
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);

        const REPLY_PATH: &str = "/myrpcpipereply";
        println!("receive (pid={} tid={}): opening reply pipe: {}", pid, thread.id(), REPLY_PATH);

        let res = open(REPLY_PATH, OpenOptions::READONLY);
        if res.is_err() {
            println!("receive (pid={} tid={}): open reply failed: {:?}", pid, thread.id(), res);
            return Err(res.unwrap_err() as i32);
        }
        let fh = res.unwrap();
        println!("receive (pid={} tid={}): opened reply fh={}", pid, thread.id(), fh);

        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        let mut off = 0usize;
        while off < 4 {
            match read(fh, &mut len_buf[off..]) {
                Ok(n) if n > 0 => {
                    println!("receive: read length n={} off={}/4", n, off);
                    off += n;
                }
                Ok(0) => {
                    if off > 0 {
                        // Partial read - retry like server does
                        let mut retries = 0usize;
                        const MAX_RETRIES: usize = 8;
                        println!("receive: partial EOF at off={}, retrying up to {} times", off, MAX_RETRIES);
                        let mut got_something = false;
                        while retries < MAX_RETRIES && off < 4 {
                            match read(fh, &mut len_buf[off..]) {
                                Ok(n) if n > 0 => {
                                    println!("receive: retry read n={} off={}/4", n, off);
                                    off += n;
                                    got_something = true;
                                    break;
                                }
                                Ok(0) => {
                                    retries += 1;
                                    thread::switch();
                                    continue;
                                }
                                Err(e) => {
                                    println!("receive: retry read error: {:?}", e);
                                    let _ = close(fh);
                                    return Err(e as i32);
                                }
                                _ => {
                                    let _ = close(fh);
                                    return Err(-3);
                                }
                            }
                        }
                        if !got_something && off < 4 {
                            println!("receive: EOF persisted after {} retries", retries);
                            let _ = close(fh);
                            return Err(-2);
                        }
                    } else {
                        println!("receive: EOF while reading length at start");
                        let _ = close(fh);
                        return Err(-2);
                    }
                }
                Err(e) => {
                    println!("receive: read length error: {:?}", e);
                    let _ = close(fh);
                    return Err(e as i32);
                }
                _ => {
                    let _ = close(fh);
                    return Err(-3);
                }
            }
        }

        let reply_len = u32::from_le_bytes(len_buf) as usize;
        println!("receive (pid={} tid={}): reply length = {}", pid, thread.id(), reply_len);

        if reply_len > out.len() {
            println!("receive: reply too large ({} > {})", reply_len, out.len());
            let _ = close(fh);
            return Err(-4);
        }

        // Read reply payload
        off = 0;
        while off < reply_len {
            match read(fh, &mut out[off..reply_len]) {
                Ok(n) if n > 0 => {
                    println!("receive: read payload n={} off={}/{}", n, off, reply_len);
                    off += n;
                }
                Ok(0) => {
                    if off > 0 {
                        // Partial read - retry like server does
                        let mut retries = 0usize;
                        const MAX_RETRIES: usize = 8;
                        println!("receive: partial EOF in payload at off={}/{}, retrying", off, reply_len);
                        let mut got_something = false;
                        while retries < MAX_RETRIES && off < reply_len {
                            match read(fh, &mut out[off..reply_len]) {
                                Ok(n) if n > 0 => {
                                    println!("receive: retry payload n={} off={}/{}", n, off, reply_len);
                                    off += n;
                                    got_something = true;
                                    break;
                                }
                                Ok(0) => {
                                    retries += 1;
                                    thread::switch();
                                    continue;
                                }
                                Err(e) => {
                                    println!("receive: retry payload error: {:?}", e);
                                    let _ = close(fh);
                                    return Err(e as i32);
                                }
                                _ => {
                                    let _ = close(fh);
                                    return Err(-6);
                                }
                            }
                        }
                        if !got_something && off < reply_len {
                            println!("receive: EOF persisted in payload after {} retries", retries);
                            let _ = close(fh);
                            return Err(-5);
                        }
                    } else {
                        println!("receive: EOF while reading payload at start");
                        let _ = close(fh);
                        return Err(-5);
                    }
                }
                Err(e) => {
                    println!("receive: read payload error: {:?}", e);
                    let _ = close(fh);
                    return Err(e as i32);
                }
                _ => {
                    let _ = close(fh);
                    return Err(-6);
                }
            }
        }

        println!("receive (pid={} tid={}): received {} bytes, closing fh={}", pid, thread.id(), reply_len, fh);
        let _ = close(fh);
        Ok(reply_len)
    }
}
