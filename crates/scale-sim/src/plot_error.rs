use std::io;

use thiserror::Error;

/// Plot generation failure.
#[derive(Debug, Error)]
pub enum PlotError {
    /// The requested plot has no samples.
    #[error("the plot trace must not be empty")]
    EmptyTrace,
    /// Plotters could not draw the requested plot.
    #[error("plot drawing failed: {0}")]
    Drawing(String),
    /// File output failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}
