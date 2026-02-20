#![allow(unused_imports)]

extern crate alloc;

use crate::consts::{CLIENT_CLOSE_DELAY_MS, PIPE_BUF};
use crate::error::RpcError;
use crate::io_helpers::{read_raw_bytes_from_pipe, write_exact};
use crate::transport::Transport;
use concurrent::thread;
use log::{debug, error};
use naming::shared_types::OpenOptions;
use naming::{close, open};

pub struct PipeTransport {}

impl PipeTransport {
    pub const fn new() -> Self {
        PipeTransport {}
    }
}

fn writer_thread(path: &str, msg: &[u8]) -> Option<Result<(), RpcError>> {
    let thread = thread::current().unwrap();
    let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);
    debug!("writer_thread (pid={} tid={}): start, path={}, msg.len()={}", pid, thread.id(), path, msg.len());

    let res = open(path, OpenOptions::WRITEONLY);
    if res.is_err() {
        error!("writer_thread (pid={} tid={}): open failed, error: {:?}", pid, thread.id(), res);
        return Some(Err(RpcError::PipeOpenFailed));
    }
    let fh = res.unwrap();
    debug!("writer_thread (pid={} tid={}): opened fh={}", pid, thread.id(), fh);

    // msg already contains the raw Cap'n Proto framing produced by
    // serialize::write_message_to_words in the caller.  We write it as-is
    // so both sides share the same framing without an extra length prefix.
    let full_buf = msg;

    if let Err(e) = write_exact(fh, full_buf) {
        error!("writer_thread (pid={} tid={}): write_exact failed: {:?}", pid, thread.id(), e);
        let _ = close(fh);
        return Some(Err(e));
    }

    debug!(
        "writer_thread (pid={} tid={}): send complete, {} bytes written",
        pid,
        thread.id(),
        full_buf.len()
    );

    // TODO: Remove this sleep workaround. This is a race condition fix
    // that should be replaced with proper pipe-close protocol or ACK mechanism.
    thread::sleep(CLIENT_CLOSE_DELAY_MS);
    match close(fh) {
        Ok(_) => debug!("writer_thread: closed fh={}", fh),
        Err(e) => error!("writer_thread: close failed fh={} err={:?}", fh, e),
    }

    None
}

impl Transport for PipeTransport {
    fn send(&self, path: &str, msg: &[u8]) -> Result<(), RpcError> {
        // Perform the write from this thread (synchronous). If the writer
        // encounters an error, propagate it as Err(RpcError).
        if let Some(res) = writer_thread(path, msg) {
            return res;
        }

        Ok(())
    }

    fn receive<'a>(&self, out: &'a mut [u8], reply_path: &str) -> Result<usize, RpcError> {
        let thread = thread::current().unwrap();
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);

        debug!("receive (pid={} tid={}): opening reply pipe: {}", pid, thread.id(), reply_path);

        let res = open(reply_path, OpenOptions::READONLY);
        if res.is_err() {
            error!("receive (pid={} tid={}): open reply failed: {:?}", pid, thread.id(), res);
            return Err(RpcError::PipeOpenFailed);
        }
        let fh = res.unwrap();
        debug!("receive (pid={} tid={}): opened reply fh={}", pid, thread.id(), fh);

        let bytes = match read_raw_bytes_from_pipe(fh) {
            Ok(b) => b,
            Err(e) => {
                error!("receive (pid={} tid={}): read_raw_bytes_from_pipe failed: {:?}", pid, thread.id(), e);
                let _ = close(fh);
                return Err(e);
            }
        };
        let _ = close(fh);

        if bytes.len() > out.len() {
            error!("receive: message too large ({} > {})", bytes.len(), out.len());
            return Err(RpcError::MessageTooLarge {
                size: bytes.len(),
                max: out.len(),
            });
        }

        let n = bytes.len();
        out[..n].copy_from_slice(&bytes);

        debug!("receive (pid={} tid={}): received {} bytes total", pid, thread.id(), n);
        Ok(n)
    }
}
