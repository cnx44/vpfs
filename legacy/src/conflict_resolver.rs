use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use clap::Parser;
use vpfs::messages::{ConflictResolutionRequest, ConflictResolutionResponse};

#[derive(Parser, Debug)]
#[command(name = "conflict_resolver", about = "Interactive conflict resolution server for vpfs.")]
struct Opt {
    #[arg(short, long, default_value_t = 8083)]
    port: u16,
}

fn send_message_tcp<T: serde::Serialize>(stream: &mut TcpStream, message: T) {
    let buf = serde_bare::to_vec(&message).unwrap();
    stream.write_all(&(buf.len() as u64).to_be_bytes()).unwrap();
    stream.write_all(&buf).unwrap();
}

fn receive_message_tcp<T: serde::de::DeserializeOwned>(stream: &mut TcpStream) -> io::Result<T> {
    let mut len_buf = [0u8; 8];
    stream.read_exact(&mut len_buf)?;
    let len = u64::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    serde_bare::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn main() {
    let opt = Opt::parse();

    let address = format!("0.0.0.0:{}", opt.port);
    let listener = TcpListener::bind(&address).unwrap();
    println!("Conflict resolver listening on {}", address);

    loop {
        println!("Waiting for daemon connection...");
        let (mut stream, addr) = listener.accept().unwrap();
        println!("Daemon connected from {}", addr);

        loop {
            match receive_message_tcp::<ConflictResolutionRequest>(&mut stream) {
                Ok(ConflictResolutionRequest::Versions(versions)) => {
                    let local = &versions[0];
                    let remote = &versions[1];

                    println!("\nConflict for: {}", local.name);
                    println!("  [1] local  owner={} uri={}", local.owner, local.uri);
                    println!("  [2] remote owner={} uri={}", remote.owner, remote.uri);
                    print!("Keep which version? [1/2]: ");
                    io::stdout().flush().unwrap();

                    let mut input = String::new();
                    io::stdin().read_line(&mut input).unwrap();

                    let chosen = match input.trim() {
                        "2" => remote.clone(),
                        _   => local.clone(),
                    };

                    send_message_tcp(&mut stream, ConflictResolutionResponse::FinalVersion(chosen));
                }
                Err(_) => {
                    println!("Daemon disconnected.");
                    break;
                }
            }
        }
    }
}
