//! Receiving side of client programs (TCP, one thread per client): decodes
//! requests, hands them to the service, encodes responses. No logic of its own.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::linux::net::TcpStreamExt;
use std::sync::Arc;
use std::thread;

use tokio::runtime::Handle;

use super::service::Service;
use vpfs::framing::{recv_frame, send_frame};
use vpfs::messages::*;

/// Accept clients forever.
pub fn serve(address: &str, service: Arc<Service>, rt: Handle) {
    let listener = TcpListener::bind(address).unwrap();
    println!("Listening for client connections");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let (service, rt) = (service.clone(), rt.clone());
                thread::spawn(move || handle_client(stream, &service, &rt));
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
}

fn handle_client(mut stream: TcpStream, service: &Service, rt: &Handle) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_quickack(true);
    match recv_frame(&mut stream) {
        Ok(Hello::ClientHello) => {
            println!("User process connected");
            if send_frame(&mut stream, &HelloResponse::ClientHello(service.me.clone())).is_err() {
                return;
            }
        }
        Ok(_) => return eprintln!("Unexpected hello message"),
        Err(_) => return eprintln!("Did not receive proper hello message"),
    }
    while let Ok(req) = recv_frame::<ClientRequest>(&mut stream) {
        if handle_request(&mut stream, req, service, rt).is_err() {
            break;
        }
    }
    println!("Client disconnected");
}

fn handle_request(stream: &mut TcpStream, req: ClientRequest, service: &Service, rt: &Handle) -> io::Result<()> {
    // Responses that carry data: the length in the response, then the raw bytes.
    let with_data = |stream: &mut TcpStream, wrap: fn(Result<usize, VPFSError>) -> ClientResponse, result: Result<Vec<u8>, VPFSError>| {
        send_frame(stream, &wrap(result.as_ref().map(Vec::len).map_err(Clone::clone)))?;
        stream.write_all(&result.unwrap_or_default())
    };
    let response = match req {
        ClientRequest::ListFiles(_) => ClientResponse::ListFiles(Ok(rt.block_on(service.list()))),
        ClientRequest::Find(path) => ClientResponse::Find(rt.block_on(service.find(path))),
        ClientRequest::Place(path, owner, kind) => ClientResponse::Place(rt.block_on(service.place(path, owner, kind))),
        ClientRequest::Open(file) => ClientResponse::Open(rt.block_on(service.open(file))),
        ClientRequest::Close(owner, fd) => ClientResponse::Close(rt.block_on(service.close(owner, fd))),
        ClientRequest::Read(file) => return with_data(stream, ClientResponse::Read, rt.block_on(service.read(file))),
        ClientRequest::ReadFd(file, fd, len) => {
            return with_data(stream, ClientResponse::ReadFd, rt.block_on(service.read_fd(file.owner, fd, len)));
        }
        ClientRequest::ReadLineFd(file, fd) => {
            return with_data(stream, ClientResponse::ReadLineFd, rt.block_on(service.read_line_fd(file.owner, fd)));
        }
        ClientRequest::Write(file, len) => {
            let mut data = vec![0u8; len];
            stream.read_exact(&mut data)?;
            ClientResponse::Write(rt.block_on(service.write(file, vec![Mutation::Replace(data)])))
        }
        ClientRequest::Mutate(file, mutations) => ClientResponse::Write(rt.block_on(service.write(file, mutations))),
    };
    send_frame(stream, &response)
}
