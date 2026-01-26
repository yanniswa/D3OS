#![no_std]

extern crate alloc;

use naming::shared_types::OpenOptions;
use naming::{close, mkfifo, open, read, write};
use syscall::return_vals::Errno;

use concurrent::{thread,process};
#[allow(unused_imports)]
use runtime::*;
use terminal::println;

const NR_OF_ITERATIONS: u32 = 6;
const FIFO_PATH: &str = "/mypipe";


fn writer_thread() {
    let thread = thread::current().unwrap();
    let writer_tid = thread.id();
    println!("w [{:02}]: start", writer_tid);
    let res = open(FIFO_PATH, OpenOptions::WRITEONLY);
    if res.is_err() {
        println!("w [{:02}]: open failed, error: {:?}", writer_tid, res);
        return;
    }
    let fh = res.unwrap();

    println!("w [{:02}]: pipe handle = {:?}", writer_tid, fh);
    let mut cnt = 0;
    let mut wbuff: [u8; 1] = [0; 1];
    let mut ch: u8 = b'A'; // start at ASCII 'A'
    loop {
        wbuff[0] = ch;
        let res = write(fh, &wbuff);
        if res.is_err() {
            println!("w [{:02}]: write failed, error: {:?}", writer_tid, res);
        } else {
            println!("w [{:02}]: wrote one byte = '{}'", writer_tid, ch as char);
            // Next letter
            ch = if ch == b'Z' {
                b'A' // wrap around after 'Z'
            } else {
                ch + 1
            };
        }
        cnt = cnt + 1;
        if cnt > NR_OF_ITERATIONS {
            break;
        }
//        concurrent::thread::sleep(1000);
    }

    close(fh).expect("Failed to close pipe");
    println!("w [{:02}]: end", writer_tid);
}

fn reader_thread() {
    let thread = thread::current().unwrap();
    let reader_tid = thread.id();
    println!("r [{:02}]: start", reader_tid);
    let res = open(FIFO_PATH, OpenOptions::READONLY);
    if res.is_err() {
        println!("r [{:02}]: open failed, error: {:?}", reader_tid, res);
        return;
    }
    let fh = res.unwrap();

    println!("r [{:02}]: pipe handle = {:?}", reader_tid, fh);
    let mut rbuff: [u8; 1] = [0; 1];
    let mut cnt = 0;
    loop {
        let res = read(fh, &mut rbuff);
        if res.is_err() {
            println!("r [{:02}]: read failed, error: {:?}", reader_tid, res);
        } else {
            if rbuff[0].is_ascii() {
                let ch = rbuff[0] as char;
                println!("r [{:02}]: read one byte '{}', read = {}", reader_tid, ch, res.unwrap());
            } else {
                println!("r [{:02}]: read invalid data", reader_tid);
            }
        }
        cnt = cnt + 1;
        if cnt > NR_OF_ITERATIONS {
            break;
        }
//        concurrent::thread::sleep(1000);
    }

    close(fh).expect("Failed to close pipe");
    println!("r [{:02}]: end", reader_tid);
}



#[unsafe(no_mangle)]
pub fn main() {
    let process = process::current().unwrap();
    let thread = thread::current().unwrap();
    let main_tid = thread.id();
    println!("named pipe demo: pid={}, tid={}", process.id(), main_tid);
    println!("------------------------------");

    let res = mkfifo(FIFO_PATH);
    match res {
        Ok(_) =>  println!("m [{:02}]: mkfifo success", main_tid),
        Err(e) => {
            if e == Errno::EEXIST {
                println!("m [{:02}]: mkfifo: pipe already exists", main_tid);
            } else {
                println!("m [{:02}]: mkfifo failed, error: {:?}", main_tid, e);
                return;
            }
        }
    }

    let writer = thread::create(|| {
        writer_thread();
    });

    let reader = thread::create(|| {
        reader_thread();
    });

    println!("named pipe demo: reader.join()");
    let res = reader.unwrap().join();
    println!("named pipe demo: reader.join() result: {:?}", res);
    
    println!("named pipe demo: writer.join()");
    let res = writer.unwrap().join();
    println!("named pipe demo: writer.join() result: {:?}", res);


/* 
    Currently, exit codes of threads are not stored. If reader and writer
    threads end in another order then the programed joins, the main
    thread would get stuck.
    
    println!("named pipe demo: wait for writer thread to end");
    if let Some(w) = writer {
        w.join();
    }

    println!("named pipe demo: wait for reader thread to end");
    if let Some(r) = reader {
        r.join();
    }
*/
    const WAIT_SECONDS: i32 = 4;
    let mut i = 0;
    loop {
        concurrent::thread::sleep(1000);
        i = i + 1;
        println!("m [{:02}]: terminating in {} seconds", main_tid, WAIT_SECONDS - i);
        if i >= WAIT_SECONDS {
            break;
        }
    }
    println!("named pipe demo: done");
}
