/// I/O helper functions for RPC communication over pipes
extern crate alloc;
use crate::consts::{MAX_READ_RETRIES, READ_TIMEOUT_MS};
use crate::error::RpcError;
use capnp::message::ReaderOptions;
use capnp::serialize;
use concurrent::thread;
use log::{debug, error, trace};
use naming::{read, write};

/// Read exactly `buf.len()` bytes from file handle `fh`.
///
/// This function implements retry logic to handle partial reads and EOF scenarios
/// that can occur with FIFO pipes when the writer hasn't finished writing yet.
///
/// # Arguments
/// * `fh` - File handle to read from
/// * `buf` - Buffer to read into
///
/// # Returns
/// * `Ok(())` - Successfully read all bytes
/// * `Err(RpcError::UnexpectedEof)` - EOF encountered (writer closed)
/// * `Err(RpcError)` - Read error occurred
pub fn read_exact(fh: usize, buf: &mut [u8]) -> Result<(), RpcError> {
    let mut off = 0usize;
    let start_time = time::systime().num_milliseconds();

    while off < buf.len() {
        // Check for timeout
        if time::systime().num_milliseconds() - start_time > READ_TIMEOUT_MS {
            error!("read_exact: timeout after {}ms, read {}/{} bytes fh={}", READ_TIMEOUT_MS, off, buf.len(), fh);
            return Err(RpcError::Timeout);
        }

        match read(fh, &mut buf[off..]) {
            Ok(n) if n > 0 => {
                trace!("read_exact: read n={} off={}/{} fh={}", n, off, buf.len(), fh);
                off += n;
            }
            Ok(0) => {
                // EOF encountered
                if off > 0 {
                    // Partial read - retry to give writer time to finish
                    let mut retries = 0usize;
                    debug!(
                        "read_exact: partial EOF at off={}/{} fh={}, retrying up to {} times",
                        off,
                        buf.len(),
                        fh,
                        MAX_READ_RETRIES
                    );

                    let mut got_something = false;
                    while retries < MAX_READ_RETRIES && off < buf.len() {
                        match read(fh, &mut buf[off..]) {
                            Ok(n) if n > 0 => {
                                trace!("read_exact: retry read n={} off={}/{} fh={}", n, off, buf.len(), fh);
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
                            Err(_) => {
                                error!("read_exact: retry read error at off={}/{} fh={}", off, buf.len(), fh);
                                return Err(RpcError::ReadFailed);
                            }
                            _ => {
                                error!("read_exact: retry unexpected result at off={}/{} fh={}", off, buf.len(), fh);
                                return Err(RpcError::UnknownIoResult);
                            }
                        }
                    }

                    if !got_something && off < buf.len() {
                        error!(
                            "read_exact: EOF persisted after {} retries, partial off={}/{} fh={}",
                            retries,
                            off,
                            buf.len(),
                            fh
                        );
                        return Err(RpcError::UnexpectedEof);
                    }
                } else {
                    // EOF at start
                    debug!("read_exact: EOF at start (no bytes read) fh={}", fh);
                    return Err(RpcError::UnexpectedEof);
                }
            }
            Err(e) => {
                error!("read_exact: read error: {:?} off={}/{} fh={}", e, off, buf.len(), fh);
                return Err(RpcError::ReadFailed);
            }
            _ => {
                error!("read_exact: read returned unexpected value off={}/{} fh={}", off, buf.len(), fh);
                return Err(RpcError::UnknownIoResult);
            }
        }
    }

    Ok(())
}

/// Write exactly `buf.len()` bytes to file handle `fh`.
///
/// This function ensures that the entire buffer is written, handling
/// partial writes that can occur with pipes. Returns an error if any
/// write fails or returns 0 bytes.
///
/// # Arguments
/// * `fh` - File handle to write to
/// * `buf` - Buffer to write from
///
/// # Returns
/// * `Ok(())` - Successfully wrote all bytes
/// * `Err(RpcError::WriteReturnedZero)` - Write returned 0 (pipe closed)
/// * `Err(RpcError::WriteFailed)` - Write operation failed
pub fn write_exact(fh: usize, buf: &[u8]) -> Result<(), RpcError> {
    let mut off = 0usize;

    while off < buf.len() {
        match write(fh, &buf[off..]) {
            Ok(0) => {
                error!("write_exact: write returned 0 at off={}/{} fh={}", off, buf.len(), fh);
                return Err(RpcError::WriteReturnedZero);
            }
            Ok(n) => {
                trace!("write_exact: wrote n={} off={}/{} fh={}", n, off, buf.len(), fh);
                off += n;
            }
            Err(e) => {
                error!("write_exact: write error: {:?} off={}/{} fh={}", e, off, buf.len(), fh);
                return Err(RpcError::WriteFailed);
            }
        }
    }

    debug!("write_exact: completed {} bytes fh={}", buf.len(), fh);
    Ok(())
}

/// Read a single Cap'n Proto message from a FIFO handle.
///
/// Reads a Cap'n Proto message from a pipe and returns the raw framed bytes.
///
/// This is the lower-level counterpart to `read_message_from_pipe`: it reads
/// the same wire format but returns the assembled byte buffer instead of a
/// parsed `Reader`.  Use this when you need to hand the bytes to a caller who
/// will deserialize them separately (e.g. `Transport::receive`).
pub fn read_raw_bytes_from_pipe(fh: usize) -> Result<alloc::vec::Vec<u8>, RpcError> {
    use crate::consts::{MAX_CAPNP_SEGMENTS, MAX_SEGMENT_TABLE_SIZE};
    use alloc::vec;

    // --- segment count (4 bytes) ---
    let mut seg_count_buf = [0u8; 4];
    read_exact(fh, &mut seg_count_buf)?;
    let segment_count = u32::from_le_bytes(seg_count_buf).wrapping_add(1) as usize;
    if segment_count == 0 || segment_count > MAX_CAPNP_SEGMENTS {
        error!("read_raw_bytes_from_pipe: invalid segment_count={}", segment_count);
        return Err(RpcError::InvalidSegmentCount);
    }

    // --- segment size table ---
    let sizes_len = segment_count * 4;
    let mut sizes_buf = [0u8; MAX_SEGMENT_TABLE_SIZE];
    if sizes_len > sizes_buf.len() {
        return Err(RpcError::InvalidSegmentCount);
    }
    read_exact(fh, &mut sizes_buf[..sizes_len])?;

    let mut total_words = 0usize;
    for i in 0..segment_count {
        let w = u32::from_le_bytes([sizes_buf[i * 4], sizes_buf[i * 4 + 1], sizes_buf[i * 4 + 2], sizes_buf[i * 4 + 3]]) as usize;
        total_words += w;
    }

    // --- optional 4-byte padding ---
    let padding = if segment_count % 2 == 0 { 4 } else { 0 };
    if padding > 0 {
        let mut pad_buf = [0u8; 4];
        read_exact(fh, &mut pad_buf)?;
    }

    // --- message data ---
    let total_bytes = total_words * 8;
    let mut data_buf = vec![0u8; total_bytes];
    if total_bytes > 0 {
        read_exact(fh, &mut data_buf)?;
    }

    // Re-assemble the canonical framed byte stream
    let header_size = 4 + sizes_len + padding;
    let mut flat: alloc::vec::Vec<u8> = alloc::vec::Vec::with_capacity(header_size + total_bytes);
    flat.extend_from_slice(&seg_count_buf);
    flat.extend_from_slice(&sizes_buf[..sizes_len]);
    if padding > 0 {
        flat.extend(core::iter::repeat(0u8).take(padding));
    }
    flat.extend_from_slice(&data_buf);

    Ok(flat)
}
