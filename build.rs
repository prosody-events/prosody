//! Compiles the peer wire contract in `proto/peer.proto`.
//!
//! `protoc` must be on `PATH` — see the build prerequisites in `README.md`.
//! The generated descriptor set is written beside the generated code. gRPC
//! reflection embeds that set to publish the schema. The tests read it to check
//! the hand-written frame codec against the `.proto` it must agree with.

use std::env::var;
use std::io::{Error, Result};
use std::path::PathBuf;

/// Lints the generated server module trips, listed rather than blanketed so a
/// lint that stops firing is reported instead of silently permitted. The
/// generated code writes absolute paths and a unit binding. It documents
/// nothing.
///
/// `dead_code` does not belong on this list. The generated module opens with an
/// inner `allow(dead_code)` of its own, so an expectation here can never see a
/// diagnostic and is reported unfulfilled.
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
