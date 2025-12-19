/// Minimal trait for an RPC transport. Implementations should ensure
/// correct synchronization and zero-copy semantics where possible.
pub trait Transport {
    /// Send a message buffer (owned or borrowed depending on transport)
    fn send(&self, msg: &[u8]) -> Result<(), i32>;

    /// Receive a message into the provided buffer view; returns the length
    /// of the received message or an error code.
    fn receive<'a>(&self, out: &'a mut [u8]) -> Result<usize, i32>;
}

// ---------------------------------------------------------------------------
// Very small LoopbackTransport for local testing.
// - no_std compatible
// - synchronous: `send` writes a canned response into an internal static
//   response buffer, `receive` copies it into the provided output buffer.
// This is NOT a production transport; it only helps testing the client stub
// without any external system.
// ---------------------------------------------------------------------------

use crate::server;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const LOOPBACK_BUF_SIZE: usize = 512;

static mut LOOPBACK_RESPONSE: [u8; LOOPBACK_BUF_SIZE] = [0u8; LOOPBACK_BUF_SIZE];
static LOOPBACK_RESPONSE_LEN: AtomicUsize = AtomicUsize::new(0);
static LOOPBACK_READY: AtomicBool = AtomicBool::new(false);

pub struct LoopbackTransport {}

impl LoopbackTransport {
    pub const fn new() -> Self {
        LoopbackTransport {}
    }
}
//TODO implementiere einen Transport, der die das senden und empfangen einfach simuliert
//TODO ich brauche einen Dummy Server, der die Anfrage entgegennimmt und eine Antwort zurückgibt
impl Transport for LoopbackTransport {
    fn send(&self, msg: &[u8]) -> Result<(), i32> {
        // For the dummy in‑process server we synchronously call the
        // `server::handle_request_sync` helper and copy the response into
        // the static loopback response buffer so `receive` can fetch it.
        /*TODO
            normalerweise würde hier die Nachricht über ein echtes Transportmedium geschickt werden
            das ist hier erstmal okay, für morgen sollte hier zB Pipes verwendet werden
            zusätzlich muss der Server in einem eigenen Prozess/Thread laufen
            außerdem sollten der Server mit entsprechenden Methoden erweitert werden
            für das empfangen und senden von Capnproto Nachrichten
            --> generell muss alles noch auf CAPNPROTO umgestellt werden
            Der server braucht mechanismen um Anfragen korrekt zu empfanfen, zu verarbeiten und
            Antworten korrekt zurückzusenden --> receive in Transport

        */
        /*  let resp = server::handle_request_sync(msg);

        // copy into static buffer
        let len = resp.len().min(LOOPBACK_BUF_SIZE);
        unsafe {
            // Write bytes into the static response buffer
            LOOPBACK_RESPONSE[0..len].copy_from_slice(&resp[..len]);
        }
        LOOPBACK_RESPONSE_LEN.store(len, Ordering::Release);
        LOOPBACK_READY.store(true, Ordering::Release);*/

        Ok(())
    }

    fn receive<'a>(&self, out: &'a mut [u8]) -> Result<usize, i32> {
        // Busy‑wait until a response is ready (simple test semantics).
        while !LOOPBACK_READY.load(Ordering::Acquire) {}

        let len = LOOPBACK_RESPONSE_LEN.load(Ordering::Acquire);
        let to_copy = core::cmp::min(len, out.len());
        unsafe {
            out[0..to_copy].copy_from_slice(&LOOPBACK_RESPONSE[0..to_copy]);
        }

        // Reset ready flag so next request waits for a new response.
        LOOPBACK_READY.store(false, Ordering::Release);

        Ok(to_copy)
    }
}
