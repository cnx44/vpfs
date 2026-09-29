//! Client library: talks to the local daemon over TCP.

use std::collections::BTreeMap;
use std::net::TcpStream;
use std::os::linux::net::TcpStreamExt;
use std::sync::Mutex;

use libc::FIONREAD;

use crate::framing::{recv_frame, send_frame};
use crate::messages::*;

pub struct VPFS {
    pub local: String, // name
    connection: Mutex<TcpStream>,
    /// client fd -> (file, fd on the owner daemon)
    open_files: Mutex<BTreeMap<i32, (FileEntry, i32)>>,
}

impl VPFS {
    pub fn connect(listen_port: u16) -> Result<VPFS, std::io::Error> {
        let mut stream = TcpStream::connect(format!("localhost:{}", listen_port))?;
        _ = stream.set_nodelay(true);
        _ = stream.set_quickack(true);

        send_frame(&mut stream, &Hello::ClientHello)?;
        match recv_frame(&mut stream) {
            Ok(HelloResponse::ClientHello(local)) => Ok(VPFS {
                local,
                connection: Mutex::new(stream),
                open_files: Mutex::new(BTreeMap::new()),
            }),
            _ => panic!("Got wrong hello response"),
        }
    }

    /// Send a request, optionally followed by a raw payload, and wait for the response.
    /// The daemon going away is not recoverable for a client: it panics.
    fn request(&self, req: ClientRequest, payload: Option<&[u8]>) -> (ClientResponse, TcpStreamGuard<'_>) {
        let mut stream = self.connection.lock().unwrap();
        send_frame(&mut *stream, &req).expect("daemon connection lost");
        if let Some(payload) = payload {
            std::io::Write::write_all(&mut *stream, payload).expect("daemon connection lost");
        }
        let response = recv_frame(&mut *stream).expect("daemon connection lost");
        (response, stream)
    }

    fn receive_payload(stream: &mut TcpStream, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        std::io::Read::read_exact(stream, &mut buf).expect("daemon connection lost");
        buf
    }

    pub fn find(&self, path: &str) -> Result<FileEntry, VPFSError> {
        match self.request(ClientRequest::Find(path.to_string()), None).0 {
            ClientResponse::Find(result) => result,
            _ => panic!("Bad response to find"),
        }
    }

    pub fn place(&self, path: &str, at: String) -> Result<FileEntry, VPFSError> {
        self.place_kind(path, at, FileKind::Blob)
    }

    pub fn place_kind(&self, path: &str, at: String, kind: FileKind) -> Result<FileEntry, VPFSError> {
        match self.request(ClientRequest::Place(path.to_string(), at, kind), None).0 {
            ClientResponse::Place(result) => result,
            _ => panic!("Bad response to place"),
        }
    }

    pub fn ls(&self, path: &str) -> Result<Vec<FileEntry>, VPFSError> {
        match self.request(ClientRequest::ListFiles(path.to_string()), None).0 {
            ClientResponse::ListFiles(result) => result,
            _ => panic!("Bad response to ls"),
        }
    }

    pub fn read(&self, what: FileEntry) -> Result<Vec<u8>, VPFSError> {
        match self.request(ClientRequest::Read(what), None) {
            (ClientResponse::Read(Ok(len)), mut stream) => Ok(Self::receive_payload(&mut stream, len)),
            (ClientResponse::Read(Err(error)), _) => Err(error),
            _ => panic!("Bad response to read!"),
        }
    }

    pub fn write(&self, what: FileEntry, buf: &Vec<u8>) -> Result<(), VPFSError> {
        match self.request(ClientRequest::Write(what, buf.len()), Some(buf)).0 {
            ClientResponse::Write(Ok(len)) => {
                assert!(len == buf.len());
                Ok(())
            }
            ClientResponse::Write(Err(error)) => Err(error),
            _ => panic!("Bad response to write!"),
        }
    }

    /// Apply content mutations; returns the new size. Accepted mutations depend on the file kind.
    pub fn mutate(&self, what: FileEntry, mutations: Vec<Mutation>) -> Result<usize, VPFSError> {
        match self.request(ClientRequest::Mutate(what, mutations), None).0 {
            ClientResponse::Write(result) => result,
            _ => panic!("Bad response to mutate!"),
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
        self.write(file_entry, buf)
    }

    pub fn open(&self, name: &str) -> Result<i32, VPFSError> {
        let file_entry = self.find(name)?;
        match self.request(ClientRequest::Open(file_entry.clone()), None).0 {
            ClientResponse::Open(Ok(daemon_fd)) => {
                let mut open_files = self.open_files.lock().unwrap();
                // Lowest free fd, 0,1,2 are stdin, stdout, stderr
                let fd = (3..).find(|fd| !open_files.contains_key(fd)).unwrap();
                open_files.insert(fd, (file_entry, daemon_fd));
                Ok(fd)
            }
            ClientResponse::Open(Err(_)) => Err(VPFSError::FileNotOpen),
            _ => panic!("Bad response to open"),
        }
    }

    fn open_file(&self, fd: i32) -> Result<(FileEntry, i32), VPFSError> {
        self.open_files.lock().unwrap().get(&fd).cloned().ok_or(VPFSError::FileNotOpen)
    }

    pub fn read_fd(&self, fd: i32, len: usize) -> Result<Vec<u8>, VPFSError> {
        let (file_entry, daemon_fd) = self.open_file(fd)?;
        match self.request(ClientRequest::ReadFd(file_entry, daemon_fd, len), None) {
            (ClientResponse::ReadFd(Ok(len)), mut stream) => Ok(Self::receive_payload(&mut stream, len)),
            (ClientResponse::ReadFd(Err(error)), _) => Err(error),
            _ => panic!("Bad response to read!"),
        }
    }

    pub fn read_line_fd(&self, fd: i32) -> Result<Vec<u8>, VPFSError> {
        let (file_entry, daemon_fd) = self.open_file(fd)?;
        match self.request(ClientRequest::ReadLineFd(file_entry, daemon_fd), None) {
            (ClientResponse::ReadLineFd(Ok(len)), mut stream) => Ok(Self::receive_payload(&mut stream, len)),
            (ClientResponse::ReadLineFd(Err(error)), _) => Err(error),
            _ => panic!("Bad response to read!"),
        }
    }

    /// Only FIONREAD is accepted, and it is a stub: returns Ok(0) and leaves `arg` untouched.
    pub fn ioctl<T>(&self, fd: i32, request: u64, _arg: &mut T) -> Result<i32, VPFSError> {
        if request != FIONREAD as u64 {
            panic!("Unsupported ioctl request: {}", request)
        }
        self.open_file(fd)?;
        Ok(0)
    }

    pub fn close(&self, fd: i32) -> Result<(), VPFSError> {
        let (file_entry, daemon_fd) = self.open_file(fd)?;
        match self.request(ClientRequest::Close(file_entry.owner, daemon_fd), None).0 {
            ClientResponse::Close(Ok(())) => {
                self.open_files.lock().unwrap().remove(&fd);
                Ok(())
            }
            ClientResponse::Close(Err(error)) => Err(error),
            _ => panic!("Bad response to close!"),
        }
    }
}

type TcpStreamGuard<'a> = std::sync::MutexGuard<'a, TcpStream>;
