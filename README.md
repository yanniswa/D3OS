<p align="center">
  <a href="https://www.uni-duesseldorf.de/home/en/home.html"><img src="media/d3os.png" width=460></a>
</p>

**A new distributed operating system for data centers, developed by the [operating systems group](https://www.cs.hhu.de/en/research-groups/operating-systems.html) of the department of computer science at [Heinrich Heine University Düsseldorf](https://www.hhu.de)**

<p align="center">
  <a href="https://www.uni-duesseldorf.de/home/en/home.html"><img src="media/hhu.svg" width=300></a>
</p>

<p align="center">
  <a href="https://github.com/hhu-bsinfo/D3OS/actions/workflows/build.yml"><img src="https://github.com/hhu-bsinfo/D3OS/actions/workflows/build.yml/badge.svg"></a>
  <img src="https://img.shields.io/badge/Rust-2024-blue.svg">
  <img src="https://img.shields.io/badge/license-GPLv3-orange.svg">
</p>

## Requirements

For building D3OS, the following packages for Debian/Ubuntu based systems (or their equivalent packages on other distributions) need to be installed:

```bash
apt install rustup build-essential nasm dosfstools wget qemu-system-x86
```

This has been tested on Ubuntu 24.04.

For macOS, the same can be achieved with:

```bash
xcode-select --install
brew install rustup dosfstools nasm x86_64-elf-gcc gnu-tar wget qemu
brew link --force rustup
```

This has been tested on macOS 14.

[rustup](https://rustup.rs/) will download a _rust nightly_ toolchain on the first compile.

To run the build, the commands _cargo-make_ and _cargo-license_ are required. Install them with:

```bash
cargo install --no-default-features cargo-make cargo-license
```

## Build and Run

To build D3OS and run it in QEMU, just execute:

```bash
cargo make --no-workspace
```

To build a release version of D3OS (much faster) and run it in QEMU, just execute:

```bash
cargo make --no-workspace --profile production
```

To only build the bootable image _d3os.img_, run:

```bash
cargo make --no-workspace image
```

## Debugging

### In a terminal with gdb

Open a terminal and compile and start D3OS in `qemu` halted by `gdb` with the following commands:

```bash
cargo make --no-workspace clean
cargo make --no-workspace debug
```

Open another terminal and start `gdb` with:

```bash
cargo make --no-workspace gdb
```

This will fire booting D3OS and stop in `boot.rs::start`.

Setting a breakpoint in `gdb`:

```bash
break kernel::naming::api::init
```

This way, a single application can also be debugged:

```bash
add-symbol-file loader/initrd/bin/hello
break main
```

For further commands check [GDB Quick Reference](docs/gdb-commands.pdf).

### In your editor

The repository contains debug configurations for RustRover, Visual Studio Code and Zed.
To debug userspace applications, you might need to modify them.

## Creating a bootable USB stick

### Using towboot

D3OS uses [towboot](https://github.com/hhuOS/towboot) which is already installed after you have successfully compiled D3OS.

Use following command (in the D3OS directory) to create a bootable media for the device referenced by `/mnt/external`

`$ towbootctl install /mnt/external --removable -- -config towboot.toml`

### Using balenaEtcher

Write the file `d3os.img` using [balenaEtcher](https://etcher.balena.io) to your USB stick.

## Repeatedly booting on a physical device

If you're trying to fix a bug, the workflow of "building D3OS, plugging a USB stick into your development device, flashing it, plugging the USB stick into your target device, boot" can get annoying.

If you do have a working network connection between these devices, this can get easier:

1. grab `towboot.efi` (from the [GitHub releases](https://github.com/hhuOS/towboot/releases) or with `./towbootctl extract --x86-64 loader/towboot.efi`) and place it into `loader/towboot.efi`
2. grab [`ipxe.efi`](https://boot.ipxe.org/ipxe.efi) and place it on a USB stick under `/BOOT/EFI/BOOTX64.EFI` (or on a FAT partition on the target device)
3. put the following into `/autoexec.ipxe`:

```sh
#!ipxe
dhcp
chain http://IP_OF_YOUR_HOST:8000/command.ipxe
```

4. `cd loader/; python3 -m http.server`
5. compile D3OS and boot with the created stick

This way, you only need to recompile and reboot the target, no need to re-flash.

## Passing an existing PCI device to the VM

To use a real device with QEMU, change the Makefile so that it uses `${CARGO_MAKE_WORKSPACE_WORKING_DIRECTORY}/qemu-pci.sh` instead of `qemu-system-x86_64`.
Also take a look at that script and fill in the constants at the top.

If you want to run D3OS on a different device, build with `cargo make --no-workspace image` and copy over `qemu-pci.sh`, `RELEASEX64_OVMF.fd` and `d3os.img`.
Run it with `./qemu-pci.sh -bios RELEASEX64_OVMF.fd -hda d3os.img`.

## RPC Framework

D3OS includes a `no_std` RPC framework built on [Cap'n Proto](https://capnproto.org/) serialization and named pipes (FIFOs) as the transport medium.

### Architecture

```
┌──────────────────────────────┐        named pipe         ┌──────────────────────────────┐
│         Client Process       │  ──── /myrpcpiperequest ──▶│        Server Process        │
│                              │                            │                              │
│  HelloServiceClient          │  ◀─── /rpc_reply_{pid}_{n} │  RpcServer<T>                │
│    └─ RpcSerializer          │        (per-call pipe)     │    └─ ServerPipeTransport     │
│    └─ PipeTransport          │                            │         (persistent FH)       │
└──────────────────────────────┘                            └──────────────────────────────┘
```

**Request flow:**

1. Client creates a unique reply pipe `/rpc_reply_{pid}_{n}` via `mkfifo`
2. Client serializes the request (Cap'n Proto) and writes it to `/myrpcpiperequest`
3. Server reads the request, dispatches to the appropriate handler
4. Server serializes the response and writes it to the reply pipe path embedded in the request
5. Client reads the response from its reply pipe, then calls `unlink` to free the pipe

### Crate layout

| Path                                          | Purpose                                                           |
| --------------------------------------------- | ----------------------------------------------------------------- |
| `os/library/rpc/`                             | Core RPC library (transport traits, serialization, server/client) |
| `os/library/rpc/schema/schema.capnp`          | Cap'n Proto schema defining all request/response types            |
| `os/library/rpc/src/transport.rs`             | `Sender`, `ClientTransport`, `ServerTransport` traits             |
| `os/library/rpc/src/pipe_transport.rs`        | `PipeTransport` — client-side pipe transport                      |
| `os/library/rpc/src/server_pipe_transport.rs` | `ServerPipeTransport` — server-side transport with persistent FH  |
| `os/library/rpc/src/client/client_stub.rs`    | `HelloServiceClient` — typed client stub                          |
| `os/library/rpc/src/client/serializer.rs`     | `RpcSerializer` — all Cap'n Proto encode/decode logic             |
| `os/library/rpc/src/server.rs`                | `RpcServer<T>` — generic server dispatch loop                     |
| `os/library/rpc/src/handlers.rs`              | Business logic for each RPC method                                |
| `os/application/rpc_server/`                  | Server application entry point                                    |
| `os/application/rpctest/`                     | Client test application                                           |

### Transport traits

```
Sender
  ├── ClientTransport   →  PipeTransport          (stateless, per-call open/close)
  └── ServerTransport   →  ServerPipeTransport    (stateful, persistent file handle)
```

`Sender` is a shared supertrait — both sides use the same `send(path, msg)` contract. The receive side differs intentionally: the client opens a fresh reply pipe per call, while the server holds one request pipe open across many requests and transparently re-opens it on EOF.

### Usage

#### Starting the server

The server is a standalone D3OS application (`rpc_server`). It creates the request pipe, then blocks in its dispatch loop until a client connects:

```rust
use rpc::server::RpcServer;
use rpc::server_pipe_transport::ServerPipeTransport;
use rpc::consts::REQUEST_PIPE_PATH;

let transport = match ServerPipeTransport::create(REQUEST_PIPE_PATH) {
    Ok(t) => t,
    Err(e) => { /* handle error */ return; }
};
let mut server = RpcServer::with_transport(transport);
let _ = server.run(); // blocks until shutdown
```

Or use the built-in convenience entry point which does the same in one call:

```rust
RpcServer::<ServerPipeTransport>::init(); // creates pipe + runs server
```

#### Calling methods from a client

```rust
use rpc::{HelloServiceClient, PipeTransport};

let client = HelloServiceClient::new(PipeTransport::new());

// Call sayHello
match client.say_hello("Alice") {
    Ok(greeting) => info!("Response: {}", greeting), // "Hello, Alice!"
    Err(e)       => error!("RPC failed: {:?}", e),
}

// Call add
match client.add(10, 32) {
    Ok(sum) => info!("10 + 32 = {}", sum), // 42
    Err(e)  => error!("RPC failed: {:?}", e),
}
```

Each call is fully synchronous — `say_hello` and `add` block until the server responds. Internally, a unique reply pipe `/rpc_reply_{pid}_{n}` is created per call and freed automatically after the response is received.

#### Error handling

All RPC operations return `Result<_, RpcError>`. The relevant variants for callers are:

| Variant                 | When it occurs                                        |
| ----------------------- | ----------------------------------------------------- |
| `PipeOpenFailed`        | Server not running or pipe path wrong                 |
| `MessageTooLarge`       | Response exceeded `MAX_RESPONSE_SIZE` (64 KB)         |
| `DeserializationFailed` | Response could not be parsed                          |
| `Timeout`               | Server did not respond within `READ_TIMEOUT_MS` (5 s) |

### Adding a new RPC method

**1. Extend the schema** (`schema/schema.capnp`):

```capnp
struct MyMethodParams {
  value @0 :Int32;
}
struct MyMethodResult {
  result @0 :Text;
}
# Add to RpcRequest.method union:   myMethod @3 :MyMethodParams;
# Add to RpcResponse.result union:  myMethodResult @3 :MyMethodResult;
```

**2. Regenerate the Rust bindings** and add the handler in `handlers.rs`:

```rust
pub fn my_method(value: i32) -> String { ... }
```

**3. Add the dispatch arm** in `server.rs`:

```rust
Ok(schema_capnp::rpc_request::method::MyMethod(params)) => {
    let result = handlers::my_method(params?.get_value());
    self.send_response(reply_path, |b| { /* fill response */ });
}
```

**4. Add the client method** in `client_stub.rs`:

```rust
pub fn my_method(&self, value: i32) -> Result<String, RpcError> {
    let response_bytes = self.call_method(|msg, reply_path| {
        let mut root = msg.init_root::<schema_capnp::rpc_request::Builder>();
        root.set_reply_path(reply_path);
        root.get_method().init_my_method().set_value(value);
    }, "my_method")?;

    self.parse_response(response_bytes, |result| {
        match result.which() {
            Ok(schema_capnp::rpc_response::result::MyMethodResult(r)) => {
                Ok(r?.get_result()?.to_string())
            }
            _ => Err(RpcError::InvalidMessageFormat),
        }
    }, "my_method")
}
```

### Using a custom transport

`RpcServer` is generic over any `T: ServerTransport`. To use a different transport (e.g. network sockets):

```rust
struct MyNetworkTransport { ... }
impl Sender for MyNetworkTransport { ... }
impl ServerTransport for MyNetworkTransport { ... }

let mut server = RpcServer::with_transport(MyNetworkTransport::new(...));
server.run();
```

### Known limitations

- The close delay (`CLIENT_CLOSE_DELAY_MS`, `SERVER_CLOSE_DELAY_MS`) is a workaround for a pipe close race condition and should be replaced with a proper ACK mechanism.
- The server blocks indefinitely if a client opens the reply pipe path but never reads the response.
