//! Compiles the peer wire contract in `proto/peer.proto`.
//!
//! `protoc` must be on `PATH` — see the build prerequisites in `README.md`.
//! The generated descriptor set is written beside the generated code so gRPC
//! reflection can embed it and tests can check the hand-written frame codec
//! against the `.proto` it must agree with.

use std::env::var;
use std::io::{Error, Result};
use std::path::PathBuf;

/// Lints the generated server module trips, listed rather than blanketed so a
/// lint that stops firing is reported instead of silently permitted. The
/// generated code writes absolute paths and a unit binding, and documents
/// nothing.
const GENERATED_LINTS: &str = concat!(
    "#[expect(clippy::absolute_paths, clippy::default_constructed_unit_structs, ",
    "clippy::default_trait_access, clippy::doc_markdown, unused_qualifications, ",
    "reason = \"written by tonic-prost-build, not by hand\")]"
);

fn main() -> Result<()> {
    let out_dir = PathBuf::from(var("OUT_DIR").map_err(Error::other)?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("peer_descriptor.bin"))
        // The hand-written frame decoder enforces rules prost cannot state, so
        // the frame is an extern type and the codec is ours. The client is
        // hand-written too: it encodes bytes the responder already framed.
        .build_client(false)
        .codec_path("crate::router::grpc::codec::ServerFrameCodec")
        .extern_path(
            ".prosody.peer.v1.ResponseFrame",
            "crate::response::frame::ResponseFrame",
        )
        .server_mod_attribute(".", GENERATED_LINTS)
        .compile_protos(&["proto/peer.proto"], &["proto"])
}
