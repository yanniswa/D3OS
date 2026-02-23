#![no_std]

extern crate alloc;
use runtime::*;

use log::{LevelFilter, SetLoggerError, info, warn};
use logger::Logger;
use rpc::server::RpcServer;
use rpc::server_pipe_transport::ServerPipeTransport;
use spin::Once;
use terminal::println;

static LOGGER: Once<Logger> = Once::new();

pub fn logger() -> &'static Logger {
    LOGGER.call_once(Logger::new)
}

pub fn init_logger() -> Result<(), SetLoggerError> {
    log::set_logger(logger()).map(|()| log::set_max_level(LevelFilter::Trace))
}

#[unsafe(no_mangle)]
pub fn main() {
    init_logger();

    info!("rpc_server: logger initialized *********");
    info!("rpc_server: starting");
    let transport = match ServerPipeTransport::create(rpc::consts::REQUEST_PIPE_PATH) {
        Ok(t) => t,
        Err(e) => {
            println!("rpc_server: failed to create transport: {:?}", e);
            return;
        }
    };
    let mut server = RpcServer::with_transport(transport);
    let _ = server.run();
    warn!("rpc_server: ended, SHOULD NEVER OCCUR!!!!");
}
