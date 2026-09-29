//! Length-prefixed serde_bare framing: u64 big-endian length, then payload.
//! Used on every channel: client TCP, conflict resolver TCP and iroh streams.

use std::io::{self, Read, Write};

use iroh::endpoint::{RecvStream, SendStream};
use serde::{de::DeserializeOwned, Serialize};

fn encode<T: Serialize>(msg: &T) -> io::Result<Vec<u8>> {
    let payload = serde_bare::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut frame = (payload.len() as u64).to_be_bytes().to_vec();
    frame.extend(payload);
    Ok(frame)
}

fn decode<T: DeserializeOwned>(payload: &[u8]) -> io::Result<T> {
    serde_bare::from_slice(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn send_frame<T: Serialize>(w: &mut impl Write, msg: &T) -> io::Result<()> {
    w.write_all(&encode(msg)?)
}

pub fn recv_frame<T: DeserializeOwned>(r: &mut impl Read) -> io::Result<T> {
    let mut len = [0u8; 8];
    r.read_exact(&mut len)?;
    let mut payload = vec![0u8; u64::from_be_bytes(len) as usize];
    r.read_exact(&mut payload)?;
    decode(&payload)
}

pub async fn send_msg<T: Serialize>(send: &mut SendStream, msg: &T) -> anyhow::Result<()> {
    send.write_all(&encode(msg)?).await?;
    Ok(())
}

pub async fn recv_msg<T: DeserializeOwned>(recv: &mut RecvStream) -> anyhow::Result<T> {
    let mut len = [0u8; 8];
    recv.read_exact(&mut len).await?;
    let mut payload = vec![0u8; u64::from_be_bytes(len) as usize];
    recv.read_exact(&mut payload).await?;
    Ok(decode(&payload)?)
}
