extern crate alloc;

use crate::error::RpcError;
use crate::io_helpers::{read_raw_bytes_from_pipe, write_exact};
use crate::transport::{ClientTransport, Sender};
use concurrent::thread;
use log::{debug, error};
use naming::shared_types::OpenOptions;
use naming::{close, open};
use crate::consts::CLIENT_CLOSE_DELAY_MS;

pub struct PipeTransport {}

impl PipeTransport {
    pub const fn new() -> Self {
        PipeTransport {}
    }
}

impl Sender for PipeTransport {
    fn send(&self, path: &str, msg: &[u8]) -> Result<(), RpcError> {
        let thread = thread::current().unwrap();
        let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);
        debug!("send (pid={} tid={}): start, path={}, msg.len()={}", pid, thread.id(), path, msg.len());

        let res = open(path, OpenOptions::WRITEONLY);
        if res.is_err() {
            error!("send (pid={} tid={}): open failed, error: {:?}", pid, thread.id(), res);
            return Err(RpcError::PipeOpenFailed);
        }
        let fh = res.unwrap();
        debug!("send (pid={} tid={}): opened fh={}", pid, thread.id(), fh);

        if let Err(e) = write_exact(fh, msg) {
            error!("send (pid={} tid={}): write_exact failed: {:?}", pid, thread.id(), e);
            let _ = close(fh);
            return Err(e);
        }

        debug!("send (pid={} tid={}): send complete, {} bytes written", pid, thread.id(), msg.len());

        // TODO: Remove this sleep workaround. This is a race condition fix
        // that should be replaced with proper pipe-close protocol or ACK mechanism.
        thread::sleep(CLIENT_CLOSE_DELAY_MS);
        match close(fh) {
            Ok(_) => debug!("send: closed fh={}", fh),
            Err(e) => error!("send: close failed fh={} err={:?}", fh, e),
        }

        Ok(())
    }
}

impl ClientTransport for PipeTransport {
    fn receive(&self, out: &mut [u8], reply_path: &str) -> Result<usize, RpcError> {
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
