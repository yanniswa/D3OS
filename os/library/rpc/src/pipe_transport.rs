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

    // Write a 4-byte little-endian length prefix followed by the payload in
    // larger chunks. This prevents huge numbers of syscalls and avoids the
    // visual "infinite write" caused by byte-per-byte logging.
    let total_len = msg.len();
    let len_be = (total_len as u32).to_le_bytes();

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

    if let Err(e) = write_full(&len_be) {
        println!("writer_thread: failed to write length prefix: {:?}", e);
        let _ = close(fh);
        return Some(Err(e));
    }

    // Write payload in chunks
    let chunk_size = 256usize;
    let mut off = 0usize;
    while off < total_len {
        let end = min(off + chunk_size, total_len);
        let chunk = &msg[off..end];
        match write_full(chunk) {
            Ok(n) => off += n,
            Err(e) => {
                println!("writer_thread: write chunk failed: {:?}", e);
                let _ = close(fh);
                return Some(Err(e));
            }
        }
    }

    println!("writer_thread: send complete, {} bytes written", off);

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
