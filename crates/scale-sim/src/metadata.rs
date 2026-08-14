use prosody_scale_core::{
    PriorArtifact, PriorArtifactBudget, PriorArtifactIdentity, PriorCoverageRecord,
};

/// Report generator package version.
pub const GENERATOR_VERSION: &str = env!("CARGO_PKG_VERSION");

/// One model prior family in the shared artifact contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PriorArtifactKind {
    /// Certified capacity event-path model.
    Capacity,
    /// Finite-state arrival model.
    Arrival,
    /// Class-specific retry model.
    Reliability,
    /// Fast and slow replica launch model.
    Launch,
    /// KIP-848 pause model.
    Rebalance,
}

/// Owned report view of one prior artifact.
#[derive(Clone, Debug, PartialEq)]
pub struct PriorArtifactMetadata {
    kind: PriorArtifactKind,
    identity: PriorArtifactIdentity,
    budget: PriorArtifactBudget,
    coverage: Box<[PriorCoverageRecord]>,
}

impl PriorArtifactMetadata {
    pub(crate) fn new(
        kind: PriorArtifactKind,
        identity: PriorArtifactIdentity,
        budget: PriorArtifactBudget,
        coverage: &[PriorCoverageRecord],
    ) -> Self {
        Self {
            kind,
            identity,
            budget,
            coverage: coverage.into(),
        }
    }

    pub(crate) fn from_artifact(kind: PriorArtifactKind, artifact: &PriorArtifact) -> Self {
        Self::new(
            kind,
            artifact.identity(),
            artifact.budget(),
            artifact.coverage(),
        )
    }

    /// Returns the model family that owns this artifact.
    #[must_use]
    pub const fn kind(&self) -> PriorArtifactKind {
        self.kind
    }

    /// Returns the shared artifact schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        PriorArtifactIdentity::SCHEMA_VERSION
    }

    /// Returns the artifact source, version, and random stream.
    #[must_use]
    pub const fn identity(&self) -> PriorArtifactIdentity {
        self.identity
    }

    /// Returns the artifact resource and approximation budget.
    #[must_use]
    pub const fn budget(&self) -> PriorArtifactBudget {
        self.budget
    }

    /// Returns the support and separate endpoint tail masses.
    #[must_use]
    pub const fn coverage(&self) -> &[PriorCoverageRecord] {
        &self.coverage
    }
}

/// Reproducible identity for one generated report experiment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReportMetadata {
    /// Source commit used to compile the generator.
    pub commit: &'static str,
    /// Published model package version.
    pub model_version: &'static str,
    /// Primary model artifact for this report.
    pub artifact_identity: PriorArtifactIdentity,
    /// Stochastic simulator seed.
    pub seed: u64,
    /// Exact virtual duration in microseconds.
    pub duration_micros: u64,
    /// Report generator package version.
    pub generator_version: &'static str,
}

impl ReportMetadata {
    /// Constructs metadata from one completed experiment.
    #[must_use]
    pub const fn new(
        artifact_identity: PriorArtifactIdentity,
        seed: u64,
        duration_micros: u64,
    ) -> Self {
        Self {
            commit: env!("PROSODY_GIT_COMMIT"),
            model_version: prosody_scale_core::MODEL_VERSION,
            artifact_identity,
            seed,
            duration_micros,
            generator_version: GENERATOR_VERSION,
        }
    }
}

#[cfg(test)]
mod tests {
    use prosody_scale_core::PriorArtifactIdentity;

    use super::{GENERATOR_VERSION, ReportMetadata};

    #[test]
    fn report_metadata_preserves_reproducible_identity() {
        let identity = PriorArtifactIdentity::new(7, 2, 11);
        let metadata = ReportMetadata::new(identity, 13, 17);

        assert!(!metadata.commit.is_empty());
        assert_eq!(metadata.model_version, prosody_scale_core::MODEL_VERSION);
        assert_eq!(metadata.artifact_identity, identity);
        assert_eq!(metadata.seed, 13);
        assert_eq!(metadata.duration_micros, 17);
        assert_eq!(metadata.generator_version, GENERATOR_VERSION);
    }
}
