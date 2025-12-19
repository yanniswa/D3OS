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
    println!("writer_thread: opened fh={}", fh);
    // Write the message one byte at a time, similar to `pipetest::writer_thread`.
    // This produces clearer per-byte logs and matches the test pattern used
    // elsewhere in the repo.
    let mut off = 0usize;
    let mut cnt: u32 = 0;
    while off < msg.len() {
        let b: u8 = msg[off];
        let wbuff: [u8; 1] = [b];
        let res = write(fh, &wbuff);
        if res.is_err() {
            println!("writer_thread: write failed, error: {:?}", res);
        } else {
            let n = res.unwrap();
            if wbuff[0].is_ascii() {
                println!("writer_thread: wrote one byte = '{}'", wbuff[0] as char);
            } else {
                println!("writer_thread: wrote one non-ascii byte, read = {}", n);
            }
            off += n;
        }
        cnt += 1;
        // Optional safety: prevent extremely long loops if something goes wrong
        if cnt > (msg.len() as u32 * 8) {
            println!("writer_thread: too many iterations, aborting");
            break;
        }
    }

    println!("writer_thread: send complete, {} bytes written", off);
    //TODO in den Server schieben

    // write all bytes

    // close the write handle
    //question schließt das hier schon die pipe?
    //answer
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
