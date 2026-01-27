#![no_std]

extern crate alloc;
use runtime::*;

use rpc::server::RpcServer;
use terminal::println;

#[unsafe(no_mangle)]
pub fn main() {
    println!("rpc_server: starting");
    RpcServer::init();
    println!("rpc_server: ended");
}
