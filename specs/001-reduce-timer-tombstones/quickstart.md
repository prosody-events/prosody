# Quickstart: Inline/Overflow Timer State

**Feature Branch**: `001-reduce-timer-tombstones`

## Prerequisites

- Rust stable (Edition 2024)
- Cargo + nightly toolchain (for `cargo +nightly fmt`)
- Apache Cassandra (for integration tests only)

## Build & Verify

```bash
# Build
cargo build

# Check lints (must be zero warnings)
cargo clippy
cargo clippy --tests

# Format check
cargo +nightly fmt --check

# Generate docs
cargo doc --no-deps
```

## Run Tests

### Unit + Property Tests

```bash
# Run all non-integration tests (preserving output)
cargo test 2>&1 | tee /tmp/test_output.log
grep FAILED /tmp/test_output.log
```

### Cassandra Integration Tests

Requires a running Cassandra instance. Configure via environment variables:

```bash
# Set Cassandra connection
export CASSANDRA_CONTACT_POINTS="127.0.0.1:9042"
export CASSANDRA_KEYSPACE="prosody_test"

# Run integration tests (slow, output preserved)
cargo test --ignored 2>&1 | tee /tmp/integration_test_output.log
grep FAILED /tmp/integration_test_output.log
```

## Key Files to Understand

| File | What It Contains |
|------|-----------------|
| `src/timers/store/cassandra/mod.rs` | `CassandraTriggerStore`, `CassandraTriggerStoreProvider`, `TimerState`, `InlineTimer`, `RawTimerState`, all mutation implementations |
| `src/timers/store/cassandra/queries.rs` | All prepared CQL statements |
| `src/timers/store/operations.rs` | `TriggerOperations` trait (20 internal primitives) |
| `src/timers/store/mod.rs` | `TriggerStore` trait (public API), `TriggerStoreProvider` trait |
| `src/timers/store/memory.rs` | `InMemoryTriggerStore` + `InMemoryTriggerStoreProvider` |
| `src/timers/store/adapter.rs` | `TableAdapter<T>` dual-table coordinator |
| `src/consumer/storage.rs` | `StorePair` creation with providers |
| `src/cassandra/migrations/20260217_add_singleton_slots.cql` | Schema migration (UDT + static column) |

## Architecture at a Glance

```
StorePair::new()
  └── CassandraTriggerStoreProvider (shared session + queries)
        └── .create_store(topic, partition, group)
              └── CassandraTriggerStore (per-partition state_cache)
                    └── TableAdapter<CassandraTriggerStore>
                          └── implements TriggerStore (public API)
```

**Cache lifecycle**: Created with partition → dropped when partition revoked (Rust ownership).

**State flow**: Every mutation checks cache → falls back to DB read on miss → updates cache on success.

**Tombstone elimination**: Cache-warm `clear_and_schedule_key` on `Inline` state → plain UPDATE (no BATCH, no DELETE, no tombstone).
