/// I/O helper functions for RPC communication over pipes
use crate::consts::{MAX_READ_RETRIES, READ_TIMEOUT_MS};
use crate::error::RpcError;
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
