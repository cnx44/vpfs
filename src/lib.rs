//! VPFS client library, plus the wire types and framing shared with the daemon.

pub mod messages;
pub mod framing;
mod client;
mod ffi;

pub use client::VPFS;
pub use ffi::*;
