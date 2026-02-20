extern crate alloc;

use alloc::vec::Vec;
use crate::consts::SERVER_CLOSE_DELAY_MS;
use crate::error::RpcError;
use crate::io_helpers::{read_raw_bytes_from_pipe, write_exact};
use crate::transport::{Sender, ServerTransport};
use concurrent::thread;
use log::{debug, error};
use naming::shared_types::OpenOptions;
use naming::{close, mkfifo, open};

/// Server-side pipe transport.
///
/// Holds a persistent read file handle on the request pipe across many
/// requests. When a client disconnects (EOF / ReadFailed), the handle is
/// closed and immediately re-opened so the server blocks again waiting for
/// the next client — without any special-casing in the dispatch loop.
pub struct ServerPipeTransport {
    /// Path of the well-known request pipe (e.g. "/myrpcpiperequest").
    request_path: &'static str,
    /// Currently open read handle. `None` before the first `receive_next` call.
    request_fh: Option<usize>,
}

impl ServerPipeTransport {
    /// Construct without creating the pipe. Use [`create`] when you want the
    /// transport to own the `mkfifo` setup step.
    pub const fn new(request_path: &'static str) -> Self {
        ServerPipeTransport {
            request_path,
            request_fh: None,
        }
    }

    /// Create the named pipe at `request_path` and return a ready transport.
    /// This is the normal entry point — the caller does not need to call
    /// `mkfifo` separately.
    pub fn create(request_path: &'static str) -> Result<Self, crate::error::RpcError> {
        mkfifo(request_path).map_err(|_| crate::error::RpcError::PipeOpenFailed)?;
        Ok(Self::new(request_path))
    }

    /// Open (or re-open) the request pipe, spinning until it succeeds.
    fn open_request_pipe(&mut self) -> Result<(), RpcError> {
        let fh = loop {
            match open(self.request_path, OpenOptions::READONLY) {
                Ok(fh) => break fh,
                Err(_) => thread::switch(),
            }
        };
        self.request_fh = Some(fh);
        debug!("ServerPipeTransport: request pipe opened fh={}", fh);
        Ok(())
    }
}

impl Sender for ServerPipeTransport {
    fn send(&self, path: &str, msg: &[u8]) -> Result<(), RpcError> {
        let res = open(path, OpenOptions::WRITEONLY);
        if res.is_err() {
            error!("ServerPipeTransport::send: open failed for path={}", path);
            return Err(RpcError::PipeOpenFailed);
        }
        let fh = res.unwrap();

        if let Err(e) = write_exact(fh, msg) {
            error!("ServerPipeTransport::send: write_exact failed: {:?}", e);
            let _ = close(fh);
            return Err(e);
        }

        // Match the client-side delay so the reader has time to finish before
        // we close — same workaround as PipeTransport::send on the client.
        thread::sleep(SERVER_CLOSE_DELAY_MS);
        match close(fh) {
            Ok(_) => debug!("ServerPipeTransport::send: closed fh={}, {} bytes sent to {}", fh, msg.len(), path),
            Err(e) => error!("ServerPipeTransport::send: close failed fh={} err={:?}", fh, e),
        }
        Ok(())
    }
}

impl ServerTransport for ServerPipeTransport {
    /// Block until the next complete request message arrives.
    ///
    /// On EOF or read failure the pipe is transparently re-opened so the
    /// caller never has to handle the reconnect case.
    fn receive_next(&mut self) -> Result<Vec<u8>, RpcError> {
        // Lazily open on the first call.
        if self.request_fh.is_none() {
            self.open_request_pipe()?;
        }

        loop {
            let fh = self.request_fh.unwrap();
            match read_raw_bytes_from_pipe(fh) {
                Ok(bytes) => return Ok(bytes),
                Err(RpcError::UnexpectedEof) | Err(RpcError::ReadFailed) => {
                    debug!("ServerPipeTransport: EOF/ReadFailed — re-opening request pipe");
                    let _ = close(fh);
                    self.request_fh = None;
                    self.open_request_pipe()?;
                    // loop again with the new handle
                }
                Err(RpcError::Timeout) => {
                    // Transient — just retry on the same handle.
                    continue;
                }
                Err(e) => {
                    error!("ServerPipeTransport: fatal read error: {:?}", e);
                    let _ = close(fh);
                    self.request_fh = None;
                    return Err(e);
                }
            }
        }
    }
}
