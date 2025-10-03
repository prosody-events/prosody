# Multi-Objective Scheduler Implementation Specification

## System Overview

This scheduler manages task execution across 32 parallel slots for up to 1 million keys, divided into two demand
classes (failure and non-failure). It ensures proportional time allocation (30%/70% under contention), fair time
distribution among keys, and bounded scheduling delays.

The algorithm combines hierarchical virtual time scheduling with distribution-aware priority scoring, using
exponentially smoothed percentiles to adapt bounds dynamically.

## Data Type Specifications

### Time Representation

All time values are represented as 64-bit floating-point numbers in milliseconds. This provides microsecond precision
while avoiding overflow for long-running systems.

- **Virtual Time**: Cumulative execution time, monotonically increasing
- **Wall Clock Time**: Real-world timestamps for latency tracking
- **Duration**: Task execution time in milliseconds

### Distribution Tracking

For accurate percentile tracking, use HDR Histogram or similar structure that provides:

- Memory-efficient storage (~2-3KB per histogram)
- O(1) record operation
- O(1) percentile query
- Automatic range adjustment
- Microsecond precision

## Global State

### System-Wide Tracking

**Current Time**: Global simulation/system clock in milliseconds, advances with each task completion.

**Configuration Parameters** (constants):

- `FAILURE_WEIGHT`: 0.3 (target proportion for failure class)
- `NON_FAILURE_WEIGHT`: 0.7 (target proportion for non-failure class)
- `LAG_BOUND_MULTIPLIER`: 10.0 (max VT deviation as multiple of P50)
- `WAIT_BOUND_MULTIPLIER`: 20.0 (max wait time as multiple of P90)
- `MAX_PASSES`: 100 (absolute limit on consecutive scheduling passes)
- `SMOOTHING_ALPHA`: 0.05 (exponential smoothing factor for percentiles)
- `FAIRNESS_WEIGHT`: 1.0 (priority score weight for VT distance)
- `WAIT_WEIGHT`: 1000.0 (priority score weight for wait urgency)
- `PASS_WEIGHT`: 10.0 (priority score weight for pass urgency)

**Scheduling Metrics** (for monitoring):

- Total tasks completed across all keys
- Total urgency activations (when wait or pass urgency triggered)
- Current scheduling interval/epoch

### Slot Management

Each of the 32 slots maintains:

- Current executing key (if any)
- Execution start time
- Expected completion time (for monitoring)

In practice, slots may use work-stealing or batch assignment for efficiency, but logically each slot executes one task
at a time.

## Per-Class State

Each demand class (failure and non-failure) tracks:

### Core Scheduling State

**Weight** (constant): Target resource proportion (0.3 or 0.7)

**Total Execution Time**: Cumulative milliseconds of actual execution. Used for proportional class selection via
`total_execution_time / weight` scoring.

**Virtual Time**: The average virtual time of all active keys in this class. Calculated as
`sum(key.virtual_time) / active_key_count`. Updated before each scheduling decision. Represents where the "typical" key
in this class is in virtual time space.

**Active Key Count**: Number of keys with either pending tasks or non-zero virtual time. Used for average calculations.

### Distribution Parameters

**Smoothed P50**: Exponentially weighted moving average of median task duration. Updated after each task completion
using `(1 - α) * old + α * new`. Initialized conservatively at 10ms.

**Smoothed P90**: Exponentially weighted moving average of 90th percentile duration. Initialized conservatively at
100ms (10x ratio assumption).

**Recent Durations Buffer**: Circular buffer of last 100-1000 task durations for accurate percentile calculation. Used
to compute actual percentiles before smoothing.

**HDR Histogram**: Aggregate distribution of all task durations in class. Provides accurate percentiles for bound
calculations.

### Scheduling Bounds (Derived)

These are recalculated as needed from smoothed percentiles:

- **Max Lag** = `smoothed_p50 * LAG_BOUND_MULTIPLIER`
- **Max Wait** = `smoothed_p90 * WAIT_BOUND_MULTIPLIER`

## Per-Key State

Each key maintains:

### Identity

**Key ID**: Unique identifier (string or integer)
**Class Membership**: Reference to failure or non-failure class

### Virtual Time Accounting

**Virtual Time**: Cumulative execution time in milliseconds. Increases by actual task duration on each completion. Never
weighted or scaled. Represents total resource consumption by this key.

### Task Queue State

**Has Pending Tasks**: Boolean indicating whether key has work ready for scheduling.

**Current Task Enqueue Time**: Wall-clock timestamp when the current task became ready. Set when task arrives/enqueues.
Used for wait time calculation. Reset to current time when task completes and new task is ready.

**Task Duration Distribution**: Either a single expected duration or full distribution parameters (mean, stddev,
percentiles). Used for deadline calculation if known in advance.

### Anti-Starvation Counters

**Pass Count**: Number of consecutive times this key was ready but not selected. Increments each time another ready key
is selected instead. Resets to zero when selected. Triggers urgency when exceeds 50% of MAX_PASSES.

**Consecutive Overruns**: Tracks recent tasks that exceeded P95 duration. Used to detect when a key enters a "heavy"
phase. Resets after normal-duration tasks.

### Distribution Tracking (Optional)

**Key-Specific Histogram**: HDR histogram of this key's task durations. Useful for heterogeneous workloads where keys
have different characteristics.

## Event Processing

### Task Arrival Event

When a task arrives for key K in class C:

1. **Set Task State**:
    - Set K.has_pending_tasks = true
    - Set K.enqueue_time = current_wall_clock_time

2. **Update Class Accounting**:
    - Increment C.active_key_count if K was inactive

3. **Trigger Scheduling**:
    - Add K to ready queue or trigger scheduling decision

### Scheduling Decision Event

The scheduler executes periodically or when triggered:

1. **Phase 1: Class Selection**
    - For each class with ready keys:
        - Calculate score = total_execution_time / weight
    - Select class with minimum score
    - If scores equal (within epsilon), use tiebreaker (queue depth)

2. **Phase 2: Update Class Virtual Time**
    - Calculate sum of virtual times for all active keys
    - Set class.virtual_time = sum / active_key_count
    - This MUST happen before priority calculation

3. **Phase 3: Calculate Bounds**
    - max_lag = class.smoothed_p50 * LAG_BOUND_MULTIPLIER
    - max_wait = class.smoothed_p90 * WAIT_BOUND_MULTIPLIER

4. **Phase 4: Score Ready Keys**
    - For each ready key K:
        - vt_distance = K.virtual_time - class.virtual_time
        - If vt_distance > max_lag: priority = INFINITY (ineligible)
        - Else:
            - priority = vt_distance * FAIRNESS_WEIGHT
            - wait_time = current_time - K.enqueue_time
            - If wait_time > 0:
                - wait_ratio = min(1, wait_time / max_wait)
                - priority -= WAIT_WEIGHT * wait_ratio²
            - If K.pass_count > 0:
                - pass_ratio = min(1, K.pass_count / MAX_PASSES)
                - priority -= PASS_WEIGHT * pass_ratio²

5. **Phase 5: Select Key**
    - If any keys eligible (priority < INFINITY):
        - Select key with minimum priority score
    - Else (all ineligible):
        - Select key with minimum |vt_distance| (least ahead)

6. **Phase 6: Update Counters**
    - For selected key: pass_count = 0
    - For other ready keys: pass_count++
    - If selected key had urgency (wait_ratio > 0.5 or pass_ratio > 0.5):
        - Increment global urgency counter

### Task Completion Event

When key K in class C completes a task with actual_duration:

1. **Update Virtual Times**:
    - K.virtual_time += actual_duration (no weighting)
    - C.total_execution_time += actual_duration

2. **Update Distributions**:
    - Record actual_duration in C.recent_durations buffer
    - Record in C.histogram
    - Update smoothed percentiles:
        - C.smoothed_p50 = (1 - α) * C.smoothed_p50 + α * task_base_duration
        - C.smoothed_p90 = (1 - α) * C.smoothed_p90 + α * observed_p90

3. **Update Key State**:
    - K.has_pending_tasks = false (unless new task ready)
    - If new task immediately ready:
        - K.has_pending_tasks = true
        - K.enqueue_time = current_time

4. **Update Class Average**:
    - Recalculate C.virtual_time as average of active keys

5. **Track Overruns** (optional):
    - If actual_duration > P95:
        - K.consecutive_overruns++
        - Consider penalty: K.virtual_time += (actual_duration - P95) * 0.5

### New Key Addition Event

When a new key K joins class C:

1. **Initialize Virtual Time**:
    - If C has active keys:
        - K.virtual_time = C.virtual_time (class average)
    - Else:
        - K.virtual_time = 0

2. **Initialize State**:
    - K.enqueue_time = current_time
    - K.pass_count = 0
    - K.has_pending_tasks = true

3. **Initialize Distribution** (optional):
    - Copy class-level distribution as prior
    - Or use default conservative estimates

### Key Removal Event

When key K leaves class C:

1. **Update Class Accounting**:
    - Remove K.virtual_time from class sum
    - Decrement C.active_key_count
    - Recalculate C.virtual_time

2. **Preserve History** (optional):
    - Store K's virtual time offset from class average
    - Store K's distribution parameters
    - Useful if key might return

## Quality Metrics

### Fairness Metrics

**Class Proportion Error**:

- Measure: |actual_failure_ratio - 0.3| over sliding window
- Target: < 0.01 (1% error) over 10-second windows
- Calculation: failure_execution_time / total_execution_time

**Key Fairness (VT Spread)**:

- Measure: max(key.vt) - min(key.vt) within each class
- Target: < max_lag (bounded by design)
- Indicates quality of time distribution among keys

**Jain's Fairness Index**:

- Measure: (Σ(vt))² / (n * Σ(vt²)) for keys in class
- Target: > 0.95 (closer to 1 = more fair)
- Single number for overall fairness quality

### Latency Metrics

**Queue Wait Time Distribution**:

- P50, P90, P99 of (start_time - enqueue_time)
- Target: P99 < max_wait bound
- Verify bound enforcement

**Time to First Byte**:

- For new keys, time from arrival to first execution
- Should approximate class average wait time

**Scheduling Latency**:

- Time to make scheduling decision
- Target: < 1ms even with 100K active keys

### Starvation Prevention

**Urgency Activation Rate**:

- Percentage of selections requiring urgency boost
- Target: < 1% in steady state
- High rate indicates bounds too tight

**Maximum Pass Count**:

- Track max observed pass count
- Should never exceed MAX_PASSES (hard bound)

**Maximum Wait Time**:

- Track max observed wait time
- Should never exceed max_wait (hard bound)

### System Health

**Virtual Time Divergence**:

- Track if any key's VT grows unboundedly relative to class
- Indicates potential starvation or fairness violation

**Distribution Drift**:

- KL divergence between recent and historical distributions
- Detects workload changes requiring adaptation

**Bound Stability**:

- Rate of change in smoothed percentiles
- Rapid changes indicate unstable workload

### Efficiency Metrics

**Scheduling Overhead**:

- CPU time spent in scheduler vs executing tasks
- Target: < 1% of total CPU time

**Memory Usage**:

- Per-key state overhead
- Distribution histogram memory
- Target: < 500 bytes per active key

**Cache Efficiency**:

- Cache misses during scheduling decisions
- Important at scale with many keys

## Boundary Conditions

### Startup

- All virtual times begin at zero
- Percentiles initialize to conservative defaults (P50=10ms, P90=100ms)
- First scheduling decisions may be arbitrary until history builds

### All Keys Ineligible

- Occurs when all ready keys exceed max_lag ahead of class average
- Select key with minimum absolute VT distance from average
- Prevents deadlock while maintaining approximate fairness

### Single Key Active

- Virtual time spread = 0 (no fairness issue)
- Key gets 100% of class resources
- Class still maintains proportional share

### Extreme Heterogeneity

- Keys with 100x different durations can coexist
- Bounds adapt via percentile smoothing
- Urgency mechanisms prevent starvation

### Overload

- When arrival rate exceeds service rate
- Queues grow but bounds still enforced
- System remains stable with graceful degradation

## Implementation Notes

### Numerical Stability

- Use double-precision floats for virtual times
- Periodic renormalization may be needed for very long runs
- Consider relative rather than absolute VT comparisons

### Concurrency

- Per-class locks sufficient for most operations
- Consider lock-free structures for hot paths
- Batch scheduling decisions to reduce synchronization

### Optimization Opportunities

- Cache class averages between updates
- Use approximate data structures for large key counts
- Batch percentile updates rather than per-task
- Consider SIMD for priority calculations

### Monitoring Integration

- Export metrics via standard interfaces (Prometheus, StatsD)
- Include scheduling decision traces for debugging
- Track per-key metrics for outlier detection