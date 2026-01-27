/// RPC method handlers
///
/// This module contains the actual implementation of RPC methods.
/// The server's responsibility is just to dispatch requests to these handlers.
extern crate alloc;
use alloc::format;
use alloc::string::String;

/// Handler for the sayHello RPC method
///
/// # Arguments
/// * `name` - The name to greet
///
/// # Returns
/// A greeting string
pub fn say_hello(name: &str) -> String {
    format!("Hello, {}!", name)
}

/// Handler for the add RPC method
///
/// # Arguments
/// * `a` - First operand
/// * `b` - Second operand
///
/// # Returns
/// The sum of a and b
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
