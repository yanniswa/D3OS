@0x9b8f1e2b6f5a4c1a;

# Generic RPC Request with method dispatching
struct RpcRequest {
  replyPath @0 :Text;
  
  # Method to call - use union for type-safe method parameters
  method :union {
    sayHello @1 :SayHelloParams;
    add @2 :AddParams;
  }
}

# Parameters for sayHello method
struct SayHelloParams {
  name @0 :Text;
}

# Parameters for add method
struct AddParams {
  a @0 :Int32;
  b @1 :Int32;
}

# Generic RPC Response
struct RpcResponse {
  # Result - use union for type-safe return values
  result :union {
    sayHelloResult @0 :SayHelloResult;
    addResult @1 :AddResult;
    error @2 :Text;
  }
}

# Return value for sayHello
struct SayHelloResult {
  greeting @0 :Text;
}

# Return value for add
struct AddResult {
  sum @0 :Int32;
}

