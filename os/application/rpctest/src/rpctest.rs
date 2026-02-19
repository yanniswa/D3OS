#![no_std]

extern crate alloc;

use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use capnp::message::{Builder, Reader, ReaderOptions, SegmentArray};
use concurrent::{
    process,
    thread::{self, sleep},
};
use naming::mkfifo;
use rpc::HelloClient;
use rpc::PipeTransport;
use rpc::server;
#[allow(unused_imports)]
use runtime::*;
use spin::Mutex;
use terminal::println;
use core::sync::atomic::AtomicBool;

pub mod mydata_capnp {
    include!("../mydata_capnp.rs");
}


use logger::Logger;
use log::{info, SetLoggerError, LevelFilter};
use spin::Once;

static LOGGER: Once<Logger> = Once::new();

pub fn logger() -> &'static Logger {
    LOGGER.call_once(Logger::new)
}


pub fn init_logger() -> Result<(), SetLoggerError> {
    log::set_logger(logger())
        .map(|()| log::set_max_level(LevelFilter::Trace))
}





static SHARED_BYTES: Mutex<[u8; 512]> = Mutex::new([0u8; 512]);
//static READY: Mutex<bool> = Mutex::new(false);
static READY: AtomicBool = AtomicBool::new(false);


pub fn writer_capnp() {
    // Call the simple HelloClient::say_hello for demonstration
    // spawn a simple pipe-based server in a thread for local testing
    // ensure /rpc directory and request fifo exist before spawning server

    // connect transport to request pipe and client reply pipe
    let client = HelloClient::new(PipeTransport::new());
    let request = "Hallo vom writer";
    println!("Calling say_hello from writer...");
    println!("RPC request payload: '{}'", request);

    match client.say_hello(request) {
        Ok(resp) => {
            println!("say_hello returned: {}", resp);
            println!("RPC response (raw bytes): {:?}", resp.as_bytes());
        }
        Err(_) => println!("say_hello failed"),
    }

    match client.add(10, 6) {
        Ok(sum) => println!("add returned: {}", sum),
        Err(_) => println!("add failed"),
    }

  //  sleep(10000);
    match client.add(10, 6) {
        Ok(sum) => println!("add returned: {}", sum),
        Err(_) => println!("add failed"),
    }
    let mut msg = Builder::new_default();
println!("pos 1");
    {
        let mut root = msg.init_root::<mydata_capnp::my_data::Builder>();
        root.set_a(123);
        root.set_b(456);
        root.set_c("Hallo OS!");
    }

    let mut buffer = SHARED_BYTES.lock();
    let cursor = &mut buffer[..];

    capnp::serialize::write_message(cursor, &msg).unwrap();

    READY.store(true, core::sync::atomic::Ordering::SeqCst);
}

pub fn reader_capnp() -> Result<(u32, u64, String), capnp::Error> {
    while !READY.load(core::sync::atomic::Ordering::SeqCst) {}

    let buffer = SHARED_BYTES.lock();
    let slice: &[u8] = &buffer[..];

    let mut cursor = slice;

    let reader = capnp::serialize::read_message(&mut cursor, ReaderOptions::new())?;

    let root = reader.get_root::<mydata_capnp::my_data::Reader>()?;

    let a = root.get_a();
    let b = root.get_b();
    let c = root.get_c()?.to_string();

    Ok((a, b, c))
}

#[unsafe(no_mangle)]
fn main() {
    println!("rpctest: app startet…");
    init_logger();
    info!("rpctest: logger activated");

    writer_capnp();
    println!("************* ");
    match reader_capnp() {
        Ok((a, b, c)) => println!("gelesen: a={} b={} c={}", a, b, c),
        Err(_) => println!("Fehler beim Lesen"),
    }
}


