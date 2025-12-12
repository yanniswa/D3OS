use crate::Transport;

pub struct HelloClient {
    
}

impl HelloClient {

pub fn say_hello(&self, name: &str) -> Result<&str, i32> {
    /*

    Cap'n Proto Builder erzeugen
    Parameter in das Struct schreiben
    Message serialisieren
    Service-ID + Method-ID hinzufügen
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
    Ok("TODO")
}
}