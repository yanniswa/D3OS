#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use concurrent::thread;
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
    println!("writer_thread (tid={}): start", thread.id());

    let res = open("/myrpcpiperequest", OpenOptions::WRITEONLY);
    if res.is_err() {
        println!("writer_thread: open failed, error: {:?}", res);
        return Some(Err(res.unwrap_err() as i32));
    }
    let fh = res.unwrap();

    // Build a single contiguous buffer containing [len_prefix | payload].
    // If this buffer is <= PIPE_BUF the kernel will write it atomically
    // (avoiding interleaving with other writers). For larger buffers we
    // still perform a full write loop, but interleaving between writers is
    // possible for those sizes.
    let total_len = msg.len();
    if total_len > (u32::MAX as usize) {
        println!("writer_thread: payload too large {}", total_len);
        let _ = close(fh);
        return Some(Err(-1));
    }
    let len_be = (total_len as u32).to_le_bytes();

    let mut full_buf: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(4 + total_len);
    full_buf.extend_from_slice(&len_be);
    full_buf.extend_from_slice(msg);

    // Helper to write a full buffer (may require multiple write() calls).
    let mut write_full = |buf: &[u8]| -> Result<usize, i32> {
        let mut off = 0usize;
        while off < buf.len() {
            match write(fh, &buf[off..]) {
                Ok(n) if n > 0 => off += n,
                Ok(0) => return Err(-5), // treat 0 as broken pipe / unexpected
                Err(e) => return Err(e as i32),
                _ => return Err(-6),
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
        RPCServer::init();
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
        // Dummy-Antwort: "Test" in den Ausgabepuffer kopieren und Länge zurückgeben
        let data = b"Test";
        if out.len() < data.len() {
            return Err(-1); // Puffer zu klein
        }
        out[..data.len()].copy_from_slice(data);
        Ok(data.len())
    }
}
