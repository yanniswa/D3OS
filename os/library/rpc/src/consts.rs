/// Central configuration constants for the RPC framework
///
/// These values define size limits, timeouts, and buffer sizes across
/// the entire RPC system to ensure consistency and prevent misconfiguration.

/// Well-known path of the server's request FIFO.
/// Both client (send) and server (mkfifo/open) reference this constant
/// so that renaming the pipe only requires changing one place.
pub const REQUEST_PIPE_PATH: &str = "/myrpcpiperequest";

/// Maximum payload size accepted by server and client (64 KB)
///
/// Rationale: Cap'n Proto messages rarely exceed 64KB for typical RPC calls.
/// This limit prevents DoS attacks via oversized messages and ensures
/// bounded memory usage in a no_std environment.
pub const MAX_PAYLOAD_SIZE: usize = 64 * 1024;

/// Maximum response buffer size for RPC clients (64 KB)
///
/// Must be >= MAX_PAYLOAD_SIZE to avoid truncation errors.
/// This buffer is allocated on the stack in client code.
pub const MAX_RESPONSE_SIZE: usize = 64 * 1024;

/// Maximum number of retries when encountering partial reads
///
/// Rationale: 8 retries with thread::switch() yields between attempts
/// gives the writer ~8 scheduling cycles to complete the write.
/// More retries would indicate a systemic problem (dead writer).
pub const MAX_READ_RETRIES: usize = 8;

/// Maximum time to wait for read operations (in milliseconds)
///
/// Rationale: 5 seconds is enough for most RPC operations even under
/// high system load. Prevents indefinite blocking if writer crashes.
pub const READ_TIMEOUT_MS: i64 = 5000;

/// POSIX.1-2001 PIPE_BUF constant (4096 bytes on Linux)
///
/// Writes of PIPE_BUF or fewer bytes are guaranteed to be atomic
/// when writing to a pipe. This prevents message interleaving when
/// multiple writers access the same pipe concurrently.
pub const PIPE_BUF: usize = 4096;

/// Maximum Cap'n Proto segments supported in a single message
///
/// Rationale: Most messages use 1-4 segments. 1024 is a generous upper
/// bound that prevents stack overflow (1024 * 4 bytes = 4KB buffer).
pub const MAX_CAPNP_SEGMENTS: usize = 1024;

/// Maximum stack buffer for Cap'n Proto segment table (4 KB)
///
/// Calculated as MAX_CAPNP_SEGMENTS * 4 bytes per segment size entry.
/// This is allocated on the stack in receive() - ensure thread stacks
/// are at least 8KB to accommodate this plus call frames.
pub const MAX_SEGMENT_TABLE_SIZE: usize = MAX_CAPNP_SEGMENTS * 4;

/// Client-side sleep before closing write pipe (milliseconds)
///
/// TODO: This is a workaround for a race condition. Replace with
/// proper close protocol or ACK mechanism where reader signals
/// when it has finished consuming the message.
pub const CLIENT_CLOSE_DELAY_MS: usize = 500;

/// Server-side sleep before closing reply pipe (milliseconds)
///
/// TODO: This is a workaround for a race condition. Replace with
/// proper close protocol or ACK mechanism where reader signals
/// when it has finished consuming the response.
pub const SERVER_CLOSE_DELAY_MS: usize = 1000;
