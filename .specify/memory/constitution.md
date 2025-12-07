<!--
=====================================================================
SYNC IMPACT REPORT
=====================================================================
Version change: 1.1.1 → 1.2.0
Modified principles: None renamed
Added sections:
  - Principle VII. Documentation Independence (NON-NEGOTIABLE)
Removed sections: None
Templates requiring updates:
  - .specify/templates/plan-template.md: ✅ aligned (Constitution Check section present)
  - .specify/templates/spec-template.md: ✅ updated (replaced "user stories" with "scenarios")
  - .specify/templates/tasks-template.md: ✅ updated (replaced "user story" with "scenario")
  - .specify/templates/commands/*.md: N/A (no command templates found)
Follow-up TODOs: None
=====================================================================
-->

# Prosody Constitution

## Core Principles

### I. Error Handling (NON-NEGOTIABLE)

Never use `expect`, `unwrap`, `panic`, `unwrap_or`, or `ok()` - these are forbidden by lints. All code MUST propagate errors with `?` unless explicitly authorized to swallow them. Use `thiserror` for structured errors; box only when Clippy warns.

**Rationale**: Prosody is a distributed Kafka consumer library where silent failures cascade into data loss or processing stalls. Explicit error propagation ensures failures surface immediately and can be handled appropriately at system boundaries.

### II. Code Quality (NON-NEGOTIABLE)

Clippy MUST pass for both code and tests - zero warnings tolerated. Never suppress warnings with `#[allow(...)]` without explicit permission. All code MUST pass:
- `cargo clippy`
- `cargo clippy --tests`
- `cargo doc`
- `cargo +nightly fmt`

**Rationale**: Prosody processes high-volume message streams where subtle bugs compound. Clippy catches common Rust pitfalls before they become production incidents.

### III. Debugging Discipline

Never claim "found the issue" without rigorous proof. The debugging process MUST follow this sequence:
1. Gather evidence (logs, tests, reproducible behavior)
2. Form hypothesis
3. Test hypothesis
4. Verify fix

**Rationale**: Distributed systems exhibit non-obvious failure modes. Premature conclusions lead to whack-a-mole debugging cycles that waste time and introduce regressions.

### IV. Testing Standards

**Synchronization**: Never use `sleep` in tests except for backpressure simulation. Tests MUST use:
- Channel-based waiting (preferred): `let timer_event = env.expect_timer(5).await?`
- Notification with timeout via `tokio::select!`

**Assertions**: Use `assert` or `color_eyre::Result` in tests - never `expect`/`unwrap`.

**Integration tests (Output Preservation)**: MUST NEVER pipe test output directly to `grep`, `head`, or `tail` without first writing to a temp file. Re-running tests is expensive; output MUST be preserved for later inspection:
```bash
# CORRECT: preserve output for exploration
cargo test 2>&1 | tee /tmp/test_output.log
grep FAILED /tmp/test_output.log

# FORBIDDEN: loses output, forces expensive re-runs
cargo test 2>&1 | grep FAILED
cargo test 2>&1 | tail -50
```

**Rationale**: Flaky tests from timing dependencies erode trust in the test suite. Explicit synchronization primitives make tests deterministic and failures reproducible. Lost test output forces expensive re-runs and delays root cause analysis.

### V. Simplicity

Avoid over-engineering. Only make changes that are directly requested or clearly necessary. Keep solutions simple and focused:
- Don't add features beyond what was asked
- Don't add error handling for scenarios that cannot happen
- Don't create abstractions for one-time operations
- Don't design for hypothetical future requirements

**Rationale**: Prosody's complexity budget is consumed by its core mission (distributed message processing). Unnecessary abstractions increase cognitive load and maintenance burden without delivering user value.

### VI. Trait-Based Interfaces

When constructing interfaces to external state or mechanisms involving complicated concurrent state transitions, MUST define a trait so the subsystem is easy to mock.

**Examples**:
- `TriggerStore` - timer storage backend (Cassandra/Memory implementations)
- `DeferStore` - deferred message storage
- `DeferLoader` - deferred message retrieval

**When to apply**: Use traits when the subsystem:
- Communicates with external services (databases, message queues, APIs)
- Manages complex concurrent state that is difficult to reason about in tests
- Has multiple implementations (production vs. test, different backends)

**Rationale**: Trait-based abstractions enable deterministic unit tests without external dependencies. Complex concurrent state transitions are notoriously difficult to test; mocking the interface boundary isolates the logic under test from timing-dependent external behavior.

### VII. Documentation Independence (NON-NEGOTIABLE)

All documentation in code MUST stand alone. Code comments, doc strings, and inline documentation MUST NOT reference external planning artifacts such as user stories, tickets, issue numbers, or specification documents.

**Requirements**:
- Code documentation MUST be self-contained and understandable without external context
- Comments MUST explain the "what" and "why" in terms of technical behavior, not business requirements
- References to external systems (ticket IDs, story numbers) MUST NOT appear in code
- Function/module documentation MUST describe the technical contract, not trace to planning documents

**Acceptable**: Technical references (RFC numbers, algorithm names, protocol specifications) that readers can look up independently.

**Unacceptable**: `// Implements US-123`, `// See JIRA-456`, `// Per user story 3`.

**Rationale**: Code outlives the planning artifacts that created it. User stories, tickets, and specs become stale, renumbered, or deleted. Code with external references becomes confusing when those references no longer exist or have changed meaning. Self-contained documentation ensures maintainers can understand code without archaeology through dead project management systems.

## Cassandra Anti-Patterns

**CRITICAL - NEVER USE:**

1. **ALLOW FILTERING**: Full table scans destroy cluster performance
2. **Secondary Indices**: Create coordinator bottlenecks under load
3. **Materialized Views**: Break under sustained write load

**Instead**: Design proper partition keys, use clustering columns for ranges, use `Option<T>` for NULLs and filter in application code.

**Rationale**: Prosody's timer system depends on Cassandra for persistent storage. These anti-patterns cause cascading failures at scale that are difficult to diagnose and expensive to recover from.

## Development Workflow

**Code Organization** (topological by dependencies):
1. Constants → Statics → Types → Implementations → Functions → Errors (at bottom)

**Style Requirements**:
- Prefer `use` statements over fully qualified prefixes
- Methods without `self` SHOULD be functions (except `new` and similar constructors)
- Ask before making large structural changes

**Common Patterns**:
- Use `parking_lot` over `std::sync`
- Use `tokio::sync` primitives (`Notify`, channels, `select!`) for async coordination
- Mark builders with `#[must_use]`
- Use `LazyLock` for expensive static initialization
- Implement `Arbitrary` for QuickCheck property tests

**Research**: Automatically use context7 for code generation and library documentation.

## Governance

This Constitution supersedes all other development practices. Amendments require:
1. Documentation of the proposed change with rationale
2. Review and approval
3. Migration plan for affected code (if applicable)

All PRs and reviews MUST verify compliance with these principles. Complexity MUST be justified against the Simplicity principle. Use CLAUDE.md for runtime development guidance.

**Compliance Review**: Before merging any PR:
- Verify no forbidden error handling patterns
- Confirm Clippy passes with zero warnings
- Check Cassandra queries against anti-patterns
- Validate test synchronization uses approved patterns
- Verify external state interfaces use traits (Principle VI)
- Confirm code documentation contains no external references (Principle VII)

**Version**: 1.2.0 | **Ratified**: 2025-12-07 | **Last Amended**: 2025-12-07
