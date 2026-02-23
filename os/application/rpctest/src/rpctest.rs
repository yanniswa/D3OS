#![no_std]

extern crate alloc;

use capnp::message::{Builder, Reader, ReaderOptions, SegmentArray};
use concurrent::{
    process,
    thread::{self, sleep},
};
use core::sync::atomic::AtomicBool;
use rpc::HelloServiceClient;
use rpc::PipeTransport;
#[allow(unused_imports)]
use runtime::*;
use spin::Mutex;
use terminal::println;

pub mod mydata_capnp {
    include!("../mydata_capnp.rs");
}

use log::{LevelFilter, SetLoggerError, info};
use logger::Logger;
use spin::Once;

static LOGGER: Once<Logger> = Once::new();

pub fn logger() -> &'static Logger {
    LOGGER.call_once(Logger::new)
}

pub fn init_logger() -> Result<(), SetLoggerError> {
    log::set_logger(logger()).map(|()| log::set_max_level(LevelFilter::Trace))
}

static SHARED_BYTES: Mutex<[u8; 512]> = Mutex::new([0u8; 512]);
//static READY: Mutex<bool> = Mutex::new(false);
static READY: AtomicBool = AtomicBool::new(false);

pub fn writer_capnp() {
    let client = HelloServiceClient::new(PipeTransport::new());
    let request = "Benutzer";
    println!("Calling say_hello from writer...");
    match client.say_hello(request) {
        Ok(resp) => {
            println!("say_hello returned: {}", resp);
        }
        Err(_) => println!("say_hello failed"),
    }

    println!("Calling add from writer...");
    match client.add(10, 6) {
        Ok(sum) => println!("add returned: {}", sum),
        Err(_) => println!("add failed"),
    }

    sleep(1000); //sleep so the server reopens pipe after the first call
    println!("Calling add again from writer...");
    match client.add(10, 60) {
        Ok(sum) => println!("add returned: {}", sum),
        Err(_) => println!("add failed"),
    }
}

#[unsafe(no_mangle)]
fn main() {
    init_logger();
    info!("rpctest: logger activated");
    info!("rpctest: app startet…");
    writer_capnp();

    sleep(100);
}
