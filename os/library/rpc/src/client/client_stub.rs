use crate::transport::Transport;
extern crate alloc;
use alloc::string::String;
use core::str;
use naming::mkfifo;
use terminal::println;

use capnp::message::Builder;
use capnp::serialize;

pub mod hello_capnp {
    include!("../hello_capnp.rs");
}

pub struct HelloClient<T: Transport> {
    transport: T,
}

impl<T: Transport> HelloClient<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn say_hello(&self, name: &str) -> Result<String, i32> {
        /*

        Cap'n Proto Builder erzeugen
        Parameter in das Struct schreiben
        Message serialisieren
        Service-ID + Method-ID hinzufügen
        Header mit Capnp serialisieren
        Request über Transport senden
        Antwort abwarten
        Antwort deserialisieren
        Ergebnis extrahieren und zurückgeben


        Ich habe einen Client Stub --> erstmal hardcoden ?
        welche Methoden sollte man anbieten für das OS ?
        Kommunikation zwischen Servern
            - naming service
            - oskernel src naming api --> naming service
        */
        // Build a Cap'n Proto message for the request using the generated schema.
        // The generated code will be available as `hello_capnp` (via build.rs).
        const reply_path: &str = "/myrpcpipereply";
        let res = mkfifo(reply_path);
        if res.is_err() {
            println!("mkfifo failed for reply, error: {:?}", res);
        }
        println!("mkfifo for reply: ok");
        let mut message = Builder::new_default();
        {
            let mut root = message.init_root::<hello_capnp::hello_request::Builder>();
            root.set_name(name);
            root.set_reply_path(reply_path);
        }

        // Serialize message into words and reinterpret as bytes
        let words = serialize::write_message_to_words(&message);
        let bytes_len = words.len() * core::mem::size_of::<capnp::Word>();
        let bytes: &[u8] = unsafe { core::slice::from_raw_parts(words.as_ptr() as *const u8, bytes_len) };

        // Send the capnp bytes (transport will add the length-prefix)
        self.transport.send(bytes)?;

        // receive response into a local buffer
        let mut out = [0u8; 2048];
        let n = self.transport.receive(&mut out)?;

        // For now, assume the server replies with a UTF-8 reply inside the capnp response payload
        // If the server sends a capnp message, you would parse it similarly with capnp::serialize::read_message_from_flat_slice
        match str::from_utf8(&out[..n]) {
            Ok(s) => Ok(String::from(s)),
            Err(_) => Err(-3),
        }
    }
}
