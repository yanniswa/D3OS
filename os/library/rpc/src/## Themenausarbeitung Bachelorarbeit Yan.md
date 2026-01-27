## Themenausarbeitung Bachelorarbeit Yannis Wagner

### Titel:

    - deutsch: Entwicklung eines RPC-Frameworks für ein Rust basiertes Betriebssystem
    - englisch: Design and Implementation of a RPC Framework for no_std-based Operating Systems

### Aufbau:

    - Ziel der Arbeit:
        - Entwicklung eines leichtgewichtigen RPC-Frameworks zur Kommunikation in einem no_std-basierten Betriebssystem (z. B. D3OS).

    - Motivation:
        - In no_std-Umgebungen fehlen oft komfortable Mechanismen für Interprozesskommunikation. Ziel ist eine Lösung, die ohne Standardbibliothek, aber mit hoher Effizienz funktioniert.

    - Fokus:
       - Design eines modularen Frameworks
       - Serialisierung erfolgt hierbei über Captn Proto
       - Implementierung und Kommunikation erstmal über Pipes
       - später eventuell auf Netzwerk ausweiten  ?

    - Anforderungen:

        - Kompatibel mit no_std (keine Abhängigkeit zur Standardbibliothek)

### Erwartetes Ergebnis:

    Ein funktionierendes RPC-Framework für no_std-Systeme, das als Basis für zukünftige verteilte oder modulare Komponenten im Betriebssystem dienen kann.

erstmal in einer main methode die serialisierung in einem einzelnen Thread serialisierung und antwort deserialisieren

dann 2 Threads in einem Adressraum und dann 2 Threads in unterschiedlichen Adressräumen

1.  Prüfer:
    - Fabian Ruhland als 1. Prüfer angeben
    - Mauve als 2.
    - Betreuer Niklas eingeben

Frage: - Wieso ist in der Open Implementierung in der tmpfs.rs per default der Error drinne - Wie kann ich den Autostart von dem Server einrichten? - Server automatisch starten ?
-boot.rs

//Debug:
-exec add-symbol-file loader/initrd/bin/hello
-exec break main
-exec add-symbol-file loader/initrd/bin/rpctest
-exec add-symbol-file loader/initrd/bin/rpctest 0x10000000000

file /home/yannis/Bachelorarbeit/D3OS/loader/initrd/bin/rpctest
-exec break os/library/rpc/src/server.rs:87
-exec break os/library/rpc/src/server.rs:201

Aktueller Stand: - das Schreiben und Lesen wurde umgebaut und scheint auf den ersten Blick robuster zu sein, da in einem Zug geschrieben wird -
--> ich glaube dadurch, dass wir das Programm öfter starten, arbeiten mit der Zeit mehrere Thread gleichzeitig auf der Pipe --> dadurch dann falsches lesen der Payload und Probleme

// manchmal passiert nichts im server
// debugger
// Pipe implementierung update
// automatisches booten ?

Problem:

Problem:

1. Pipe Blockierung

   ```rust
   // In tmpfs.rs - Pipe::open()
   if flags.contains(OpenFlags::O_RDONLY) {
       while !pipe.has_writer {
           wait_queue.block(); // Thread blockiert hier!
       }
   }
   ```

2. Unblock wird ausgelöst
   ```rust
   // Wenn Writer die Pipe öffnet:
   pipe.has_writer = true;
   wait_queue.notify_one(); // Weckt den blockierten Reader
   // In WaitQueue::notify_one():
   scheduler().unblock(pid, tid); // ← Ruft unblock auf!
   ```
3. unblock ruft ready auf

   ```rust
   pub fn unblock(&self, pid: usize, tid: usize) -> bool {
       if let Some(thread) = blocked_thread {
           thread.set_state(ThreadState::Ready);
           self.ready(thread);  // ← Hier wird ready() aufgerufen!
           return true;
       }
   }
   ```

4. ready() überschrieb vorher die join_map

   ```rust
   // VORHER (BUG):
   pub fn ready(&self, thread: Arc<Thread>) {
       state.ready_queue.push_front(thread);
       join_map.insert(id, Vec::new());  // ← Überschreibt IMMER!
   }

   // Wenn Thread 7 eine join_map-Eintrag hatte:
   // join_map[7] = [Thread 6 (Shell)]  ← Waiter-Liste
   //
   // Nach ready() durch unblock():
   // join_map[7] = []  ← GELÖSCHT! Shell verloren!
   ```

5. Shell wurde vergessen

   ```rust
   // Shell hatte vorher join(7) aufgerufen:
   join_map[7] = [Thread 6]  // Shell wartet auf Th       if let Some(waiting_threads) = join_map.remove(&7) {
           // waiting_threads = []  ← Leer! Shell nicht drin!
           for thread in waiting_threads {  // Loop läuft 0 mal
               self.unblock(thread.pid(), thread.id());
           }
       }
   }
   // Shell wird NIE geweckt → hängt für immer!
   ```

6. FIX

   ```rust
   // JETZT (FIX):
   pub fn ready(&self, thread: Arc<Thread>) {
       state.ready_queue.push_front(thread);

       if !join_map.contains_key(&id) {  // ← Nur wenn noch nicht vorhanden!
           join_map.insert(id, Vec::new());
       }
       // Wenn Thread 7 schon drin ist, passiert nichts!
       // join_map[7] = [Thread 6] bleibt erhalten!
   }
   ```

- join_map speichert für jeden Thread eine Liste von Threads die auf das beenden dieses Threads warten
- wenn der Thread nach wieder unblock wird --> nach einem Pipe open zB, wird wieder ready aufgerufen
- --> dabei wurde die join map überschrieben und die Threads konnten nicht mehr benachrichtigt werden

Lost Wakeup Race Condition:

- Server hat die Antwort geschrieben und direkt wieder geschlossen, dadurch wurde wieder hasWriter=false
- wartender Thread konnte nicht benachrichtigt werden
  read 7

  // Thread 7 blockiert bei Pipe-Operation
  // Thread 7 wird unblocked → ready(7) → join_map[7] = [] ← BUG!

  // Thread 7 beendet sich:
  pub fn exit(&self) {
