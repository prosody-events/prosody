//! Compiles the peer wire contract in `proto/peer.proto`.
//!
//! `protoc` must be on `PATH` — see the build prerequisites in `README.md`.
//! The generated descriptor set is written beside the generated code so gRPC
//! reflection can embed it and tests can check the hand-written frame codec
//! against the `.proto` it must agree with.

use std::env::var;
use std::io::{Error, Result};
use std::path::PathBuf;

fn main() -> Result<()> {
    let out_dir = PathBuf::from(var("OUT_DIR").map_err(Error::other)?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("peer_descriptor.bin"))
        .compile_protos(&["proto/peer.proto"], &["proto"])
}
