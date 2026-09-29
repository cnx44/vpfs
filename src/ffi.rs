//! C ABI over a process-wide client connected to the default daemon port.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uchar, c_ulong, c_void};
use std::ptr;
use std::sync::OnceLock;

use crate::VPFS;

static GLOBAL_VPFS: OnceLock<VPFS> = OnceLock::new();
const DEFAULT_PORT: u16 = 8082;

fn get_vpfs() -> &'static VPFS {
    GLOBAL_VPFS.get_or_init(|| {
        VPFS::connect(DEFAULT_PORT)
            .expect("Failed to connect to VPFS daemon")
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpfs_open(
    name: *const c_char,
) -> c_int {
    if name.is_null() {
        return -1;
    }

    let vpfs = get_vpfs();
    let name = unsafe { CStr::from_ptr(name).to_str().unwrap() };

    match vpfs.open(name) {
        Ok(fd) => fd,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpfs_read_fd(
    fd: c_int,
    buf: *mut c_uchar,
    bufsize: usize,
) -> isize {
    if buf.is_null() {
        return -1;
    }

    let vpfs = get_vpfs();

    match vpfs.read_fd(fd, bufsize) {
        Ok(read_buf) => {
            let n = read_buf.len();

            // Copy into caller buffer
            unsafe { 
                ptr::copy_nonoverlapping(
                    read_buf.as_ptr(),
                    buf,
                    n,
                ) 
            };

            n as isize
        }
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn vpfs_ioctl(
    fd: c_int,
    request: c_ulong,
    argp: *mut c_void,
) -> c_int {
    if argp.is_null() {
        return -1;
    }

    let vpfs = get_vpfs();

    unsafe {
        match vpfs.ioctl(fd as i32, request as u64, &mut *argp) {
            Ok(_) => 0,
            Err(_) => -1,
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vpfs_close(fd: c_int) -> c_int {
    let vpfs = get_vpfs();
    match vpfs.close(fd) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

