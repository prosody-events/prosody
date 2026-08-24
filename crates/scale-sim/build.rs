//! Embeds the source commit in generated report metadata.

use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::Command;

fn main() -> io::Result<()> {
    println!("cargo::rustc-check-cfg=cfg(simulation_profile)");
    // Cargo reports the inherited profile name here. The simulation profile's
    // optimization level is its build-script-visible identity.
    if env::var("OPT_LEVEL").is_ok_and(|level| level == "1") {
        println!("cargo::rustc-cfg=simulation_profile");
    }

    let repository =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").map_err(io::Error::other)?).join("../..");
    let git_head = repository.join(".git/HEAD");
    println!("cargo::rerun-if-changed={}", git_head.display());
    if let Ok(head) = fs::read_to_string(&git_head)
        && let Some(reference) = head.trim().strip_prefix("ref: ")
    {
        println!(
            "cargo::rerun-if-changed={}",
            repository.join(".git").join(reference).display()
        );
    }
    let output = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .current_dir(repository)
        .output()?;
    let commit = if output.status.success() {
        String::from_utf8(output.stdout)
            .map_err(io::Error::other)?
            .trim()
            .to_owned()
    } else {
        "unknown".to_owned()
    };
    println!("cargo::rustc-env=PROSODY_GIT_COMMIT={commit}");
    Ok(())
}
