#![no_std]

extern crate alloc;
use runtime::*;

use rpc::server::RPCServer;
use terminal::println;

#[unsafe(no_mangle)]
pub fn main() {
    println!("rpc_server: starting");
    RPCServer::init();
    println!("rpc_server: ended");
}
