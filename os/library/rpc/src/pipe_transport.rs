#![allow(unused_imports)]

extern crate alloc;

use crate::consts::{CLIENT_CLOSE_DELAY_MS, MAX_CAPNP_SEGMENTS, MAX_SEGMENT_TABLE_SIZE, PIPE_BUF};
use crate::error::RpcError;
use crate::io_helpers::{read_exact, write_exact};
use crate::transport::Transport;
use alloc::string::String;
use concurrent::thread;
use log::{debug, error, trace};
use naming::shared_types::OpenOptions;
use naming::{close, open};

pub struct PipeTransport {}

impl PipeTransport {
    pub const fn new() -> Self {
        PipeTransport {}
    }
}

fn writer_thread(msg: &[u8]) -> Option<Result<(), RpcError>> {
    let thread = thread::current().unwrap();
    // Include process id (if available) and thread id in logs for tracing
    let pid = concurrent::process::current().map(|p| p.id()).unwrap_or(0);
    debug!("writer_thread (pid={} tid={}): start, msg.len()={}", pid, thread.id(), msg.len());

    let res = open("/myrpcpiperequest", OpenOptions::WRITEONLY);
    if res.is_err() {
        error!("writer_thread (pid={} tid={}): open failed, error: {:?}", pid, thread.id(), res);
        return Some(Err(RpcError::PipeOpenFailed));
    }
    let fh = res.unwrap();

    debug!("writer_thread (pid={} tid={}): opened fh={}", pid, thread.id(), fh);

    // Build a single contiguous buffer containing [len_prefix | payload].
    // If this buffer is <= PIPE_BUF the kernel will write it atomically
    // (avoiding interleaving with other writers). For larger buffers we
    // still perform a full write loop, but interleaving between writers is
    // possible for those sizes.
    let total_len = msg.len();
    debug!("writer_thread (pid={} tid={}): total_len={}", pid, thread.id(), total_len);
    if total_len > (u32::MAX as usize) {
        error!(
            "writer_thread (pid={} tid={}): payload too large {} (max: {})",
            pid,
            thread.id(),
            total_len,
            u32::MAX as usize
        );
        let _ = close(fh);
        return Some(Err(RpcError::MessageTooLarge {
            size: total_len,
            max: u32::MAX as usize,
        }));
    }
    let len_be = (total_len as u32).to_le_bytes();

    debug!("writer_thread (pid={} tid={}): allocating buffer, capacity={}", pid, thread.id(), 4 + total_len);
    let mut full_buf: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(4 + total_len);
    debug!("writer_thread (pid={} tid={}): buffer allocated, extending...", pid, thread.id());
    full_buf.extend_from_slice(&len_be);
    full_buf.extend_from_slice(msg);
    debug!(
        "writer_thread (pid={} tid={}): buffer ready, full_buf.len()={}",
        pid,
        thread.id(),
        full_buf.len()
    );

    // Helper to write the full buffer using the write_exact helper
    if let Err(e) = write_exact(fh, &full_buf) {
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
    fn send(&self, msg: &[u8]) -> Result<(), RpcError> {
        // Perform the write from this thread (synchronous). If the writer
        // encounters an error, propagate it as Err(RpcError).
        if let Some(res) = writer_thread(msg) {
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

        // Read Cap'n Proto wire format: first segment count (4 bytes)
        let mut segment_count_buf = [0u8; 4];
        if let Err(e) = read_exact(fh, &mut segment_count_buf) {
            error!("receive: failed to read segment count");
            let _ = close(fh);
            return Err(e);
        }

        // Cap'n Proto wire format stores (segment_count - 1) in the first 4 bytes
        // to save one bit. We add 1 back to get the actual segment count.
        let segment_count = u32::from_le_bytes(segment_count_buf).wrapping_add(1);
        debug!("receive (pid={} tid={}): segment_count = {}", pid, thread.id(), segment_count);

        if segment_count == 0 || segment_count > MAX_CAPNP_SEGMENTS as u32 {
            error!("receive: invalid segment_count={}", segment_count);
            let _ = close(fh);
            return Err(RpcError::InvalidSegmentCount);
        }

        // Read segment sizes (4 bytes per segment)
        let sizes_len = (segment_count as usize) * 4;
        let mut sizes_buf = [0u8; MAX_SEGMENT_TABLE_SIZE];
        if sizes_len > sizes_buf.len() {
            error!("receive: too many segments");
            let _ = close(fh);
            return Err(RpcError::InvalidSegmentCount);
        }

        if let Err(e) = read_exact(fh, &mut sizes_buf[..sizes_len]) {
            error!("receive: failed to read segment sizes");
            let _ = close(fh);
            return Err(e);
        }

        // Calculate total message size
        let mut total_words = 0usize;
        for i in 0..segment_count as usize {
            let size = u32::from_le_bytes([sizes_buf[i * 4], sizes_buf[i * 4 + 1], sizes_buf[i * 4 + 2], sizes_buf[i * 4 + 3]]) as usize;
            total_words += size;
        }

        // Padding after segment table (if odd number of segments)
        let padding = if segment_count % 2 == 0 { 4 } else { 0 };
        if padding > 0 {
            let mut pad_buf = [0u8; 4];
            if let Err(e) = read_exact(fh, &mut pad_buf[..padding]) {
                error!("receive: failed to read padding");
                let _ = close(fh);
                return Err(e);
            }
        }

        debug!(
            "receive (pid={} tid={}): total_words = {}, padding = {}",
            pid,
            thread.id(),
            total_words,
            padding
        );

        // Read actual message data (in words = 8 bytes each)
        let total_bytes = total_words * 8;
        if total_bytes > out.len() {
            error!("receive: message too large ({} > {})", total_bytes, out.len());
            let _ = close(fh);
            return Err(RpcError::MessageTooLarge {
                size: total_bytes,
                max: out.len(),
            });
        }

        if let Err(e) = read_exact(fh, &mut out[..total_bytes]) {
            error!("receive: failed to read message data");
            let _ = close(fh);
            return Err(e);
        }

        // Now we need to reconstruct the complete message in out buffer
        // Format: [segment_count][segment_sizes][padding?][data]
        // We'll build it by shifting data and prepending the header

        // Calculate header size
        let header_size = 4 + sizes_len + padding;
        let complete_size = header_size + total_bytes;

        if complete_size > out.len() {
            error!("receive: complete message too large ({} > {})", complete_size, out.len());
            let _ = close(fh);
            return Err(RpcError::MessageTooLarge {
                size: complete_size,
                max: out.len(),
            });
        }

        // Shift data to make room for header using optimized slice operations
        // This is more efficient than byte-by-byte copy
        out.copy_within(0..total_bytes, header_size);

        // Write header
        out[0..4].copy_from_slice(&segment_count_buf);
        out[4..4 + sizes_len].copy_from_slice(&sizes_buf[..sizes_len]);
        if padding > 0 {
            out[4 + sizes_len..4 + sizes_len + padding].fill(0);
        }

        debug!(
            "receive (pid={} tid={}): received {} bytes total, closing fh={}",
            pid,
            thread.id(),
            complete_size,
            fh
        );
        let _ = close(fh);
        Ok(complete_size)
    }
}
