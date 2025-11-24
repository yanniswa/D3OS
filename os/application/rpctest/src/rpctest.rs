#![no_std]

extern crate alloc;

use alloc::{string::{String, ToString}, sync::Arc, vec::Vec};
use concurrent::{process, thread};
use spin::Mutex;
use capnp::message::{Builder, Reader, ReaderOptions, SegmentArray};
#[allow(unused_imports)]
use runtime::*;
use terminal::println;


pub mod mydata_capnp {
    include!("../mydata_capnp.rs");
}


static SHARED_BYTES: Mutex<Option<Arc<Vec<u8>>>> = Mutex::new(None);
static READY: Mutex<bool> = Mutex::new(false);

pub fn writer_capnp() {
  
    let mut msg = Builder::new_default();
  

    {
        let mut root = msg.init_root::<mydata_capnp::my_data::Builder>();
        root.set_a(123);
        root.set_b(456);
        root.set_c("Hallo OS!"); 
    }

    let segments = msg.get_segments_for_output();
    assert!(segments.len() == 1);

    let bytes = segments[0].to_vec();
    *SHARED_BYTES.lock() = Some(Arc::new(bytes));
    *READY.lock() = true;
}

pub fn reader_capnp() -> Result<(u32, u64, String), capnp::Error> {
    while !*READY.lock() {}
    let arc = SHARED_BYTES.lock().as_ref().unwrap().clone();

  
    let binding = [arc.as_slice()];
    let reader: Reader<SegmentArray<'_>> =
        Reader::new(SegmentArray::new(&binding), ReaderOptions::new());

    let root = reader.get_root::<mydata_capnp::my_data::Reader>()?;
    let a = root.get_a();
    let b = root.get_b();
    let c_str = root.get_c()?;      
    let c_owned = c_str.to_string(); 

    Ok((a, b, c_owned))
}


#[unsafe(no_mangle)]
fn main() {
    println!("hello app startet…");
    writer_capnp();
    match reader_capnp() {
        Ok((a, b, c)) => println!("gelesen: a={} b={} c={}", a, b, c),
        Err(_) => println!("Fehler beim Lesen"),
    }
}