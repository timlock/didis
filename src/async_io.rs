#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[cfg(not(rust_analyzer))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::io;
use std::mem::zeroed;
use std::ptr::null_mut;

pub struct IO{
   ring: io_uring 
}
pub fn test_uring() -> io::Result<()> {
    let queue_depth: u32 = 1;
    let mut ring = setup_io_uring(queue_depth)?;

    println!("Submitting NOP operation");
    submit_noop(&mut ring)?;

    println!("Waiting for completion");
    wait_for_completion(&mut ring)?;

    unsafe { io_uring_queue_exit(&mut ring) };
    Ok(())
}

fn setup_io_uring(queue_depth: u32) -> io::Result<io_uring> {
    let mut ring: io_uring = unsafe { zeroed() };
    let ret = unsafe { io_uring_queue_init(queue_depth, &mut ring, 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(ring)
}

fn submit_noop(ring: &mut io_uring) -> io::Result<()> {
    unsafe {
        let sqe = io_uring_get_sqe(ring);
        if sqe.is_null() {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get SQE"));
        }

        io_uring_prep_nop(sqe);
        (*sqe).user_data = 0x88;

        let ret = io_uring_submit(ring);
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
    }

    Ok(())
}

fn wait_for_completion(ring: &mut io_uring) -> io::Result<()> {
    let mut cqe: *mut io_uring_cqe = null_mut();
    let ret = unsafe { io_uring_wait_cqe(ring, &mut cqe) };

    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    unsafe {
        println!("NOP completed with result: {}", (*cqe).res);
        println!("User data: 0x{:x}", (*cqe).user_data);
        io_uring_cqe_seen(ring, cqe);
    }

    Ok(())
}
