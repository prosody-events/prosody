# Prosody Scale Core

This crate contains the predictive autoscaling algorithm.

The core performs one deterministic state transition per call. Construction
allocates all model and scratch storage. A steady-state transition allocates no
memory and performs bounded work.

The crate contains no runtime, network, storage, telemetry, or wall-clock code.
