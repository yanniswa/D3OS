/// RPC Framework error types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcError {
    /// Failed to open a pipe
    PipeOpenFailed,

    /// Failed to create a FIFO
    FifoCreationFailed,

    /// Read operation failed
    ReadFailed,

    /// Write operation failed
    WriteFailed,

    /// Pipe closed unexpectedly (EOF)
    UnexpectedEof,

    /// Message exceeds maximum allowed size
    MessageTooLarge { size: usize, max: usize },

    /// Failed to serialize Cap'n Proto message
    SerializationFailed,

    /// Failed to deserialize Cap'n Proto message
    DeserializationFailed,

    /// Invalid message format (e.g., malformed segment table)
    InvalidMessageFormat,

    /// Invalid payload length
    InvalidPayloadLength,

    /// Server not initialized
    ServerNotInitialized,

    /// Thread creation failed
    ThreadCreationFailed,

    /// Invalid segment count in Cap'n Proto message
    InvalidSegmentCount,

    /// Failed to get Cap'n Proto message root
    CapnpGetRootFailed,

    /// Failed to get Cap'n Proto field
    CapnpGetFieldFailed,

    /// Write returned 0 unexpectedly
    WriteReturnedZero,

    /// Unknown read/write result
    UnknownIoResult,

    /// Operation timed out
    Timeout,
}

impl RpcError {
    /// Convert RpcError to an i32 error code for FFI compatibility
    pub fn to_code(&self) -> i32 {
        match self {
            RpcError::PipeOpenFailed => -1,
            RpcError::FifoCreationFailed => -2,
            RpcError::ReadFailed => -3,
            RpcError::WriteFailed => -4,
            RpcError::UnexpectedEof => -5,
            RpcError::MessageTooLarge { .. } => -6,
            RpcError::SerializationFailed => -7,
            RpcError::DeserializationFailed => -8,
            RpcError::InvalidMessageFormat => -9,
            RpcError::InvalidPayloadLength => -10,
            RpcError::ServerNotInitialized => -11,
            RpcError::ThreadCreationFailed => -12,
            RpcError::InvalidSegmentCount => -13,
            RpcError::CapnpGetRootFailed => -14,
            RpcError::CapnpGetFieldFailed => -15,
            RpcError::WriteReturnedZero => -16,
            RpcError::UnknownIoResult => -17,
            RpcError::Timeout => -18,
        }
    }

    /// Convert errno-like i32 code to RpcError
    pub fn from_code(code: i32) -> Option<Self> {
        match code {
            -1 => Some(RpcError::PipeOpenFailed),
            -2 => Some(RpcError::FifoCreationFailed),
            -3 => Some(RpcError::ReadFailed),
            -4 => Some(RpcError::WriteFailed),
            -5 => Some(RpcError::UnexpectedEof),
            -6 => Some(RpcError::MessageTooLarge { size: 0, max: 0 }),
            -7 => Some(RpcError::SerializationFailed),
            -8 => Some(RpcError::DeserializationFailed),
            -9 => Some(RpcError::InvalidMessageFormat),
            -10 => Some(RpcError::InvalidPayloadLength),
            -11 => Some(RpcError::ServerNotInitialized),
            -12 => Some(RpcError::ThreadCreationFailed),
            -13 => Some(RpcError::InvalidSegmentCount),
            -14 => Some(RpcError::CapnpGetRootFailed),
            -15 => Some(RpcError::CapnpGetFieldFailed),
            -16 => Some(RpcError::WriteReturnedZero),
            -17 => Some(RpcError::UnknownIoResult),
            -18 => Some(RpcError::Timeout),
            _ => None,
        }
    }
}

impl From<RpcError> for i32 {
    fn from(err: RpcError) -> i32 {
        err.to_code()
    }
}
