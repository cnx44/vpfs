//! Last resort for conflicts: ask a human through the conflict resolver
//! program (TCP). Runs on its own thread so a slow or absent human never
//! blocks the node; meanwhile the conflicting path stays quarantined.

use std::net::TcpStream;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::runtime::Handle;

use super::conflict::Conflict;
use super::service::Service;
use vpfs::framing::{recv_frame, send_frame};
use vpfs::messages::{ConflictResolutionRequest, ConflictResolutionResponse};

const RETRY: Duration = Duration::from_secs(5);

/// Handle conflicts from `conflicts` one at a time, until each gets an answer.
pub fn spawn(resolver_port: u16, conflicts: Receiver<Conflict>, service: Arc<Service>, rt: Handle) {
    thread::spawn(move || {
        let mut resolver: Option<TcpStream> = None;
        for conflict in conflicts {
            let versions = vec![conflict.local.op.file().clone(), conflict.remote.op.file().clone()];
            loop {
                let stream = match resolver.as_mut() {
                    Some(stream) => stream,
                    None => match TcpStream::connect(("127.0.0.1", resolver_port)) {
                        Ok(stream) => {
                            println!("Connected to conflict resolution client");
                            resolver.insert(stream)
                        }
                        Err(e) => {
                            eprintln!("Conflict resolver unreachable ({e}); {} stays quarantined", conflict.path);
                            thread::sleep(RETRY);
                            continue;
                        }
                    },
                };
                let answer = send_frame(stream, &ConflictResolutionRequest::Versions(versions.clone()))
                    .and_then(|_| recv_frame::<ConflictResolutionResponse>(stream));
                match answer {
                    Ok(ConflictResolutionResponse::FinalVersion(chosen)) => {
                        println!("Resolved file {}: {:?}", conflict.path, chosen);
                        rt.block_on(service.resolve(conflict.path.clone(), chosen));
                        break;
                    }
                    Err(e) => {
                        eprintln!("Conflict resolver failed: {e}");
                        resolver = None;
                        thread::sleep(RETRY);
                    }
                }
            }
        }
    });
}
