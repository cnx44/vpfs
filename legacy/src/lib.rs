use anyhow::Error;
use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter};
use libc::FIONREAD;

use std::sync::OnceLock;
use std::ffi::{CStr};
use std::os::raw::{c_char, c_int, c_uchar, c_ulong, c_void};

use std::ptr;
use std::os::linux::net::TcpStreamExt;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpStream};
use std::sync::{Arc, Mutex};

pub mod messages;
use messages::*;

pub struct VPFS {
    pub local: String, // name
    connection: Mutex<TcpStream>,
    client_to_daemon_fd: Mutex<BTreeMap<i32, i32>>,
    open_files: Mutex<BTreeMap<i32, FileEntry>>,
}

static GLOBAL_VPFS: OnceLock<VPFS> = OnceLock::new();
const DEFAULT_PORT: u16 = 8082;

fn get_vpfs() -> &'static VPFS {
    GLOBAL_VPFS.get_or_init(|| {
        VPFS::connect(DEFAULT_PORT)
            .expect("Failed to connect to VPFS daemon")
    })
}

impl VPFS {
    pub fn connect(listen_port: u16) -> Result<VPFS, std::io::Error> {
        let mut stream = TcpStream::connect(format!("localhost:{}", listen_port))?;
        _ = stream.set_nodelay(true);
        _ =stream.set_quickack(true);

        // serde_bare::to_writer(&stream, &Hello::ClientHello)?;
        // Serialize message
        let buf = serde_bare::to_vec(&Hello::ClientHello)?;
        // Write length
        _ = stream.write_all(&(buf.len() as u64).to_be_bytes())?;
        // Write payload
        _ = stream.write_all(&buf)?;

        let mut len_buf = [0u8; 8];
        _ = stream.read_exact(&mut len_buf);
        let len = u64::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        _ = stream.read_exact(&mut buf);

        // Deserialize message
        let hello_response = serde_bare::from_slice(&buf);
        if let Ok(HelloResponse::ClientHello(local_string)) = hello_response{
            let vpfs = VPFS { 
            local: local_string,
            connection: Mutex::new(stream),
            client_to_daemon_fd: Mutex::new(BTreeMap::new()),
            open_files: Mutex::new(BTreeMap::new()),
            };
            Ok(vpfs)
        }
        else {
            panic!("Got wrong hello response");
        }
        
    }

    fn send_request_async(&self, stream: &mut TcpStream, req: ClientRequest) {
        // Serialize message
        let buf = serde_bare::to_vec(&req).unwrap();

        // Write length
        _ = stream.write_all(&(buf.len() as u64).to_be_bytes());
        // Write payload
        _ = stream.write_all(&buf);
    }

    fn receive_response_async(&self, stream: &mut TcpStream) -> ClientResponse {
        // Read length
        let mut len_buf = [0u8; 8];
        _ = stream.read_exact(&mut len_buf);
        let len = u64::from_be_bytes(len_buf) as usize;

        // Read payload
        let mut buf = vec![0u8; len];
        _ = stream.read_exact(&mut buf);

        // Deserialize message
        let msg = serde_bare::from_slice(&buf).unwrap();
        msg
    }

    fn send_request(&self, req: ClientRequest) -> ClientResponse {
        let mut stream = self.connection.lock().unwrap();
        self.send_request_async(&mut stream, req);
        self.receive_response_async(&mut stream)
    }

    fn send_buf(&self, stream: &mut TcpStream, buf: &Vec<u8>) {
        stream.write_all(&buf).unwrap();
    }

    fn receive_buf(&self, stream: &mut TcpStream, len: usize) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn find(&self, path: &str) -> Result<FileEntry, VPFSError> {
        if let ClientResponse::Find(find_result) = self.send_request(ClientRequest::Find(path.to_string())) {
            find_result
        }
        else {
            panic!("Bad response to find")
        }
    }

    pub fn place(&self, path: &str, at: String) -> Result<FileEntry, VPFSError>{
        if let ClientResponse::Place(place_result) = self.send_request(ClientRequest::Place(path.to_string(), at)) {
            place_result
        }
        else {
            panic!("Bad response to place")
        }
    }

    pub fn ls(&self, path: &str) -> Result<Vec<FileEntry>, VPFSError> {
        if let ClientResponse::ListFiles(fs_result) = self.send_request(ClientRequest::ListFiles(path.to_string())) {
            fs_result
        }
        else {
            panic!("Bad response to ls")
        }
    }

    pub fn read(&self, what: FileEntry) -> Result<Vec<u8>, VPFSError> {
        let mut stream = self.connection.lock().unwrap();
        self.send_request_async(&mut stream, ClientRequest::Read(what));
        match self.receive_response_async(&mut stream) {
            ClientResponse::Read(Ok(len)) => {
                let buf = self.receive_buf(&mut stream, len).unwrap();
                Ok(buf)
            },
            ClientResponse::Read(Err(error)) => {
                Err(error)
            },
            _ => panic!("Bad response to read!"),
        }
    } 
    pub fn write(&self, what: FileEntry, buf: &Vec<u8>) -> Result<(), VPFSError> {
        let mut stream = self.connection.lock().unwrap();
        self.send_request_async(&mut stream, ClientRequest::Write(what, buf.len()));
        self.send_buf(&mut stream, &buf);

        match self.receive_response_async(&mut stream) {
            ClientResponse::Write(Ok(len)) => {
                assert!(len == buf.len());
                Ok(())
            },
            ClientResponse::Write(Err(error)) => {
                Err(error)
            },
            _ => panic!("Bad response to write!"),
        }
    }

    pub fn fetch(&self, name: &str) -> Result<Vec<u8>, VPFSError> {
        let file_entry = self.find(name)?;
        self.read(file_entry)
    }

    pub fn store(&self, name: &str, buf: &Vec<u8>) -> Result<(), VPFSError> {
        let file_entry = match self.place(name, self.local.clone()) {
            Ok(file_entry) => file_entry,
            Err(VPFSError::AlreadyExists(file_entry)) => file_entry,
            Err(error) => return Err(error),
        };
        self.write(file_entry.clone(), buf)
    }

    fn add_to_open_files(&self, daemon_fd: i32, file_entry: FileEntry) -> i32 {
        let mut open_files = self.open_files.lock().unwrap();
        let mut client_to_daemon_fd = self.client_to_daemon_fd.lock().unwrap();

        let mut new_fd = 3; // 0,1,2 are stdin, stdout, stderr
        for (&fd,_) in client_to_daemon_fd.range(3..) {
            if fd == new_fd {
                new_fd += 1;
            } else {
                break;
            }
        }
        client_to_daemon_fd.insert(new_fd, daemon_fd);
        open_files.insert(new_fd, file_entry);
        new_fd
    }

    pub fn open(&self, name: &str) -> Result<i32, VPFSError> {
        let file_entry = self.find(name)?;
        if let ClientResponse::Open(open_result) = self.send_request(ClientRequest::Open(file_entry.clone())) {
            if let Ok(daemon_fd) = open_result {
                let client_fd = self.add_to_open_files(daemon_fd, file_entry);
                return Ok(client_fd);
            }
            return Err(VPFSError::FileNotOpen);
        } else {
            panic!("Bad response to open")
        }
    }
    
    pub fn read_fd(&self, fd:i32, len:usize) -> Result<Vec<u8>, VPFSError> {
        let open_files = self.open_files.lock().unwrap();
        let client_to_daemon_fd = self.client_to_daemon_fd.lock().unwrap();
        if !open_files.contains_key(&fd) || !client_to_daemon_fd.contains_key(&fd) {
            return Err(VPFSError::FileNotOpen);
        }

        let daemon_fd = client_to_daemon_fd.get(&fd).unwrap().clone();
        let file_entry = open_files.get(&fd).unwrap().clone();
        
        let mut stream = self.connection.lock().unwrap();
        self.send_request_async(&mut stream, ClientRequest::ReadFd(file_entry.clone(), daemon_fd, len));
        match self.receive_response_async(&mut stream) {
            ClientResponse::ReadFd(Ok(remote_len)) => {
                let buf = self.receive_buf(&mut stream, remote_len).unwrap();
                return Ok(buf);
            },
            ClientResponse::ReadFd(Err(error)) => {
                return Err(error);
            },
            _ => panic!("Bad response to read!"),
        }
        

    }

    pub fn read_line_fd(&self, fd:i32) -> Result<Vec<u8>, VPFSError> {
        let open_files = self.open_files.lock().unwrap();
        let client_to_daemon_fd = self.client_to_daemon_fd.lock().unwrap();
        if !open_files.contains_key(&fd) || !client_to_daemon_fd.contains_key(&fd) {
            return Err(VPFSError::FileNotOpen);
        }

        let daemon_fd = client_to_daemon_fd.get(&fd).unwrap().clone();
        let file_entry = open_files.get(&fd).unwrap().clone();
        
        let mut stream = self.connection.lock().unwrap();
        self.send_request_async(&mut stream, ClientRequest::ReadLineFd(file_entry.clone(), daemon_fd));
        match self.receive_response_async(&mut stream) {
            ClientResponse::ReadLineFd(Ok(remote_len)) => {
                let buf = self.receive_buf(&mut stream, remote_len).unwrap();
                return Ok(buf);
            },
            ClientResponse::ReadLineFd(Err(error)) => {
                return Err(error);
            },
            _ => panic!("Bad response to read!"),
        }
    }

    pub fn ioctl<T>(&self, fd:i32, request: u64, arg: &mut T,) -> Result<i32, VPFSError> {
        if request != FIONREAD as u64 {
            panic!("Unsupported ioctl request: {}", request)
        }

        let open_files = self.open_files.lock().unwrap();
        let client_to_daemon_fd = self.client_to_daemon_fd.lock().unwrap();
        if !open_files.contains_key(&fd) || !client_to_daemon_fd.contains_key(&fd) {
            return Err(VPFSError::FileNotOpen);
        }

        let _daemon_fd = client_to_daemon_fd.get(&fd).unwrap().clone();
        let _file_entry = open_files.get(&fd).unwrap().clone();

        let _n_to_read = arg as *mut T as *mut u64;
        
        return Ok(0);
    }

    pub fn close(&self, fd: i32) -> Result<(), VPFSError> {
        let mut open_files = self.open_files.lock().unwrap();
        let mut client_to_daemon_fd = self.client_to_daemon_fd.lock().unwrap();
        if !open_files.contains_key(&fd) || !client_to_daemon_fd.contains_key(&fd) {
            return Err(VPFSError::FileNotOpen);
        }

        let daemon_fd = client_to_daemon_fd.get(&fd).unwrap().clone();
        let file_entry = open_files.get(&fd).unwrap().clone();
        
        let mut stream = self.connection.lock().unwrap();
        self.send_request_async(&mut stream, ClientRequest::Close(file_entry.owner, daemon_fd));
        match self.receive_response_async(&mut stream) {
            ClientResponse::Close(Ok(())) => {
                open_files.remove(&fd);
                client_to_daemon_fd.remove(&fd);

                Ok(())
            },
            ClientResponse::Close(Err(error)) => {
                Err(error)
            },
            _ => panic!("Bad response to close!"),
        }
        
    }
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

