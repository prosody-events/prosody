//! Operational modes for the Prosody client.
//!
//! This module defines the `Mode` enum to represent different processing modes,
//! along with methods for displaying and parsing modes. It also includes
//! constants for mode strings and a custom error type for handling mode-related
//! errors.

use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;

/// String constant for pipeline mode.
pub const PIPELINE_MODE: &str = "pipeline";

/// String constant for low-latency mode.
pub const LOW_LATENCY_MODE: &str = "low-latency";

/// Operational modes for the Prosody client.
#[derive(Copy, Clone, Debug, Default)]
pub enum Mode {
    /// Pipeline mode for standard processing.
    #[default]
    Pipeline,
    /// Low-latency mode for faster processing with potential trade-offs.
    LowLatency,
}

impl Display for Mode {
    /// Formats the `Mode` as a string.
    ///
    /// # Arguments
    ///
    /// * `f` - A mutable reference to the `Formatter`.
    ///
    /// # Returns
    ///
    /// A `fmt::Result` indicating whether the operation was successful.
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Mode::Pipeline => f.write_str(PIPELINE_MODE),
            Mode::LowLatency => f.write_str(LOW_LATENCY_MODE),
        }
    }
}

impl FromStr for Mode {
    type Err = ModeError;

    /// Parses a string slice into a `Mode`.
    ///
    /// # Arguments
    ///
    /// * `s` - The string slice to parse.
    ///
    /// # Returns
    ///
    /// A `Result` containing the parsed `Mode` or a `ModeError` if parsing
    /// fails.
    ///
    /// # Errors
    ///
    /// Returns a `ModeError::UnknownMode` if the input string is not a valid
    /// mode.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            PIPELINE_MODE => Ok(Mode::Pipeline),
            LOW_LATENCY_MODE => Ok(Mode::LowLatency),
            unknown => Err(ModeError::UnknownMode(unknown.to_owned())),
        }
    }
}

/// Errors that can occur during mode operations.
#[derive(Debug, Error)]
pub enum ModeError {
    /// Error when an unknown mode string is provided.
    #[error("unknown mode: '{0}'")]
    UnknownMode(String),
}
