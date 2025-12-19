

use crate::transport::Transport;
extern crate alloc;
use alloc::string::String;
use core::str;

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
    // send the raw name bytes as request
    self.transport.send(name.as_bytes())?;

    // receive response into a local buffer
    let mut out = [0u8; 512];
    let n = self.transport.receive(&mut out)?;

    // interpret as UTF-8 string
    match str::from_utf8(&out[..n]) {
        Ok(s) => Ok(String::from(s)),
        Err(_) => Err(-3),
    }
}
}