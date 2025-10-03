# Hierarchical Virtual Time Scheduler: Implementation Design

## 1. Goals and Requirements

### Primary Objectives

**Fair Resource Allocation**: Distribute execution time proportionally across two demand classes:

- Failure demand: 30% of total execution time
- Non-failure demand: 70% of total execution time

**Key-Level Fairness**: Within each class, allocate equal execution time to all active keys, independent of:

- Task arrival rates (some keys may produce more tasks)
- Task duration distributions (some keys may have longer tasks)
- Temporal patterns (burst traffic, flash crowds)

**Bounded Latency**: Provide guarantees that no task waits indefinitely:

- Maximum virtual time lag relative to class average
- Maximum wall-clock wait time
- Maximum number of times a ready task can be passed over

**Work Conservation**: Maintain high utilization of all 32 execution slots when work is available.

### Success Criteria

The scheduler succeeds when:

1. Over any 100-task window, failure class receives 28-32% of execution time
2. Virtual time spread within each class remains bounded
3. No task experiences unbounded waiting
4. All slots remain utilized when tasks are queued

## 2. Theoretical Foundation

### Why Virtual Time Achieves Fairness

**Core Concept**: Instead of tracking "number of tasks executed" (which favors keys with short tasks), we track "total
execution time consumed" (virtual time). Each key accumulates virtual time equal to its actual task durations.

**Fair Scheduling Property**: When we always select the key with the lowest virtual time, all keys converge toward equal
cumulative execution time. This is provably fair because:

1. **Catch-up mechanism**: Keys falling behind get priority
2. **Natural balancing**: No key can run indefinitely ahead
3. **Work-conserving**: We never leave slots idle when work exists

**Mathematical intuition**: For keys K₁ and K₂ with virtual times VT₁ and VT₂:

- If VT₁ < VT₂, then K₁ gets priority
- After K₁ executes a task of duration d, VT₁ += d
- If VT₁ now exceeds VT₂, priority flips
- Over time, VT₁ ≈ VT₂ (bounded deviation)

### Why Hierarchical Selection Achieves Class Proportions

**Two-level hierarchy**:

1. **Class level**: Select between failure/non-failure classes
2. **Key level**: Select specific key within chosen class

**Proportional selection mechanism**: Each class maintains a score:

```
score = total_execution_time / target_weight
```

For our 30/70 split:

- Failure score = total_failure_time / 0.3
- Non-failure score = total_nonfailure_time / 0.7

**Convergence proof sketch**:

- When failure has consumed 300ms and non-failure 700ms:
    - Failure score = 300/0.3 = 1000
    - Non-failure score = 700/0.7 = 1000
    - Scores equal → perfect 30/70 split
- If failure is underserved (say 200ms vs 800ms):
    - Failure score = 200/0.3 = 666
    - Non-failure score = 800/0.7 = 1142
    - Failure gets priority until balance restored

This negative feedback loop automatically maintains target proportions.

### Why Distribution-Aware Bounds Prevent Starvation

**Fixed bounds problem**: If we set "max lag = 100ms" but typical tasks take 200ms, keys become ineligible after one
task, causing deadlock.

**Adaptive solution**: Bounds scale with observed task durations:

- `max_lag = P50 × 10`: Allows 10 median tasks of deviation
- `max_wait = P90 × 20`: Allows 20× worst-case wait time

**Smoothed percentiles**: Use exponential moving average (α=0.05) to avoid sudden bound changes that could cause
oscillation:

```
smoothed_p50 = 0.95 × smoothed_p50 + 0.05 × current_p50
```

This provides ~20-sample half-life, balancing responsiveness with stability.

### Why Priority Scoring Combines Multiple Objectives

**Three competing concerns**:

1. **Fairness**: Keys should have similar cumulative execution time
2. **Timeliness**: Tasks shouldn't wait too long in wall-clock time
3. **Progress**: Keys shouldn't be passed over repeatedly

**Unified priority score** (lower is better):

```
priority = (vt_distance × 1.0) - (wait_urgency × 1000) - (pass_urgency × 10)
```

**Weight rationale**:

- Base fairness weight = 1.0 (priority measured in milliseconds)
- Wait weight = 1000 (allows wait urgency to override ~1000ms of fairness lag)
- Pass weight = 10 (allows pass urgency to override ~10ms of fairness lag)

**Subtractive composition**: Urgency reduces score (increases priority) but within bounded ranges. This prevents runaway
feedback loops while ensuring starvation prevention.

## 3. System Architecture

### Execution Model

**Task lifecycle**:

```
[Enqueued] → [MiddlewareEntered] → [HandlerInvoked] 
   → [HandlerSucceeded/Failed] → [MiddlewareExited] → [Complete]
```

**Scheduler responsibilities**:

- Maintain state for all active keys across all partitions
- Make scheduling decisions when slots become available
- Update state as tasks complete
- Track distribution statistics for adaptive bounds

**Non-preemptive execution**: Once a task starts (HandlerInvoked), it runs to completion. Scheduler only makes decisions
at task boundaries.

### State Components

The scheduler maintains three levels of state:

**1. Global State**

```
- current_time: Instant from quanta
- total_tasks_completed: u64
- urgency_activations: u64
```

**2. Class State** (one for failure, one for non-failure)

```
ClassScheduler {
    weight: f64,                    // 0.3 or 0.7
    total_execution_time: f64,      // Sum of all task durations (ms)
    active_keys: HashMap<Key, KeyState>,
    
    // Distribution tracking
    smoothed_p50: f64,              // EMA of median duration
    smoothed_p90: f64,              // EMA of P90 duration
    recent_durations: VecDeque<f64>, // Last 100 durations for percentile calc
    
    // Virtual time tracking
    virtual_time: f64,              // Average VT of active keys
}
```

**3. Key State** (per unique key in each class)

```
KeyState {
    key: Key,
    virtual_time: f64,              // Cumulative execution time (ms)
    
    // Current task tracking
    enqueue_time: Option<Instant>,  // When current task queued
    started_time: Option<Instant>,  // When MiddlewareEntered
    
    // Anti-starvation
    pass_count: u32,                // Consecutive times passed while ready
    
    // Lifecycle
    has_pending_task: bool,         // Is there a queued task?
    is_executing: bool,             // Is a task currently running?
}
```

### Configuration Parameters

```rust
const FAILURE_WEIGHT: f64 = 0.3;
const NON_FAILURE_WEIGHT: f64 = 0.7;

const LAG_BOUND_MULTIPLIER: f64 = 10.0;
const WAIT_BOUND_MULTIPLIER: f64 = 20.0;
const MAX_PASSES: u32 = 100;

const FAIRNESS_WEIGHT: f64 = 1.0;
const WAIT_WEIGHT: f64 = 1000.0;
const PASS_WEIGHT: f64 = 10.0;

const PERCENTILE_ALPHA: f64 = 0.05;
```

## 4. Algorithm Overview

### Scheduling Decision Flow

When a slot becomes available:

**Phase 1: Class Selection**

```
1. Check which classes have ready tasks (has_pending_task && !is_executing)
2. If only one class has ready tasks → select it
3. If both have ready tasks:
   a. Calculate scores: total_execution_time / weight
   b. Select class with lower score
4. If neither has ready tasks → wait
```

**Phase 2: Class Virtual Time Update**

```
For selected class:
1. Sum virtual_time of all keys where has_pending_task || is_executing
2. Count active keys
3. Update class.virtual_time = sum / count
```

This must happen before priority calculation to use current averages.

**Phase 3: Calculate Dynamic Bounds**

```
max_lag = class.smoothed_p50 × LAG_BOUND_MULTIPLIER
max_wait = class.smoothed_p90 × WAIT_BOUND_MULTIPLIER
```

**Phase 4: Calculate Priority Scores**

```
For each ready key in selected class:
1. vt_distance = key.virtual_time - class.virtual_time
2. Check eligibility:
   if vt_distance > max_lag:
       priority_score = INFINITY
       continue
3. Base priority = vt_distance × FAIRNESS_WEIGHT
4. Wait urgency:
   wait_time = now - key.enqueue_time
   wait_ratio = min(1.0, wait_time / max_wait)
   priority_score -= WAIT_WEIGHT × wait_ratio²
5. Pass urgency:
   pass_ratio = min(1.0, key.pass_count / MAX_PASSES)
   priority_score -= PASS_WEIGHT × pass_ratio²
```

**Phase 5: Select Key**

```
1. Find key with minimum priority_score
2. If all keys have INFINITY score:
   → Select key with minimum absolute |vt_distance|
3. Update pass counts:
   → Selected key: pass_count = 0
   → All other ready keys: pass_count += 1
```

### Completion Processing Flow

When a task completes (MiddlewareExited event):

**Phase 1: Calculate Duration**

```
duration_ms = (exit_time - enter_time).as_millis() as f64
```

**Phase 2: Update Virtual Times**

```
key.virtual_time += duration_ms
class.total_execution_time += duration_ms
```

**Phase 3: Update Distribution Statistics**

```
1. Add duration to class.recent_durations (keep last 100)
2. Calculate current P50 and P90 from recent_durations:
   sorted = recent_durations.sorted()
   current_p50 = sorted[len/2]
   current_p90 = sorted[len*9/10]
3. Update smoothed values:
   smoothed_p50 = (1-α) × smoothed_p50 + α × current_p50
   smoothed_p90 = (1-α) × smoothed_p90 + α × current_p90
```

**Phase 4: Update Class Virtual Time**

```
Recalculate class.virtual_time as average of active key VTs
```

**Phase 5: Reset Key State**

```
key.is_executing = false
key.started_time = None
// Keep enqueue_time if another task is waiting
```

## 5. Telemetry Event Processing

### Event: PartitionAssigned

```
Action:
- Initialize tracking for this partition's keys
- No immediate scheduler impact (passive initialization)
```

### Event: PartitionRevoked

```
Action:
- Remove all keys belonging to this partition
- Subtract their virtual_time from class.total_execution_time
- Update class.virtual_time
```

### Event: KeyState::MiddlewareEntered

```
Required data: key, demand_type, timestamp

Actions:
1. Get or create KeyState for this key
2. Determine class from demand_type:
   DemandType::Failure → failure_class
   DemandType::NonFailure → non_failure_class
3. Update key state:
   key.started_time = Some(timestamp)
   key.is_executing = true
4. If this is a new key:
   key.virtual_time = class.virtual_time  // Start at average
```

### Event: KeyState::HandlerInvoked

```
Action:
- No state changes (informational event)
- Could log for debugging
```

### Event: KeyState::HandlerSucceeded / HandlerFailed

```
Action:
- No immediate state changes
- These events confirm the demand type classification
```

### Event: KeyState::MiddlewareExited

```
Required data: key, demand_type, timestamp

Actions:
1. Calculate task duration:
   duration = timestamp - key.started_time
2. Update virtual times:
   key.virtual_time += duration.as_millis() as f64
   class.total_execution_time += duration.as_millis() as f64
3. Update distribution statistics:
   class.recent_durations.push(duration)
   update_smoothed_percentiles()
4. Update class virtual time:
   class.update_virtual_time()
5. Reset execution state:
   key.is_executing = false
   key.started_time = None
6. Check if more tasks pending:
   if !key.has_pending_task:
       // Could remove from active set after timeout
```

### Event: New Task Arrival (External to telemetry)

```
This happens outside the telemetry stream but must be tracked:

Actions:
1. Get or create KeyState
2. If not currently executing:
   key.enqueue_time = Some(now)
   key.has_pending_task = true
3. Trigger scheduling decision if slots available
```

## 6. Metrics and Monitoring

### Real-Time Health Metrics

**Class Proportion Accuracy**

```
failure_pct = (failure_class.total_execution_time / total_time) × 100
proportion_error = abs(failure_pct - 30.0)

Emit: proportion_error (should be < 1.0 over 100-task windows)
Alert: if proportion_error > 5.0 for extended periods
```

**Virtual Time Spread** (per class)

```
active_vts = [key.virtual_time for key in class where key is active]
max_vt = max(active_vts)
min_vt = min(active_vts)
vt_spread = max_vt - min_vt

Emit: vt_spread_failure_ms, vt_spread_nonfailure_ms
Alert: if vt_spread > (smoothed_p50 × LAG_BOUND_MULTIPLIER × 2)
```

**Urgency Activation Rate**

```
urgency_rate = (urgency_activations / total_tasks) × 100

Emit: urgency_activation_pct
Interpretation:
- < 1%: Bounds are appropriate
- 1-5%: Acceptable, bounds working as intended
- > 5%: Bounds may be too tight
```

**Maximum Wait Time** (per class)

```
For each ready key:
    wait_time = now - key.enqueue_time

Emit: max_wait_time_ms, p99_wait_time_ms
Alert: if max_wait_time > (smoothed_p90 × WAIT_BOUND_MULTIPLIER)
```

**Maximum Pass Count** (per class)

```
max_passes = max(key.pass_count for key in class where key is ready)

Emit: max_pass_count
Alert: if max_pass_count > MAX_PASSES × 0.8
```

### Performance Metrics

**Scheduling Latency**

```
Time from "slot available" to "task assigned"

Emit: scheduling_latency_p50_us, scheduling_latency_p99_us
Target: < 100μs for p99
```

**Slot Utilization**

```
utilization = (busy_slots / total_slots) × 100

Emit: slot_utilization_pct
Target: > 95% when work is available
```

**Active Key Count** (per class)

```
Count of keys with has_pending_task || is_executing

Emit: active_keys_failure, active_keys_nonfailure
```

### Distribution Metrics

**Smoothed Percentiles** (per class)

```
Emit: duration_p50_ms, duration_p90_ms
Track: percentile_drift (rate of change)
```

**Duration Statistics** (per class)

```
From recent_durations:
- mean_duration_ms
- stddev_duration_ms
- min_duration_ms, max_duration_ms
```

### Debugging Metrics

**Eligibility Violations**

```
Count of keys with vt_distance > max_lag

Emit: ineligible_key_count
Debug: List of keys and their vt_distances
```

**Class Score Imbalance**

```
score_diff = abs(failure_score - nonfailure_score)

Emit: class_score_difference
Track: score_history for debugging oscillations
```

**Virtual Time Convergence Rate**

```
Over 100-task window:
    vt_spread_start vs vt_spread_end

Emit: vt_convergence_rate_ms_per_100tasks
Expected: Negative (spread should decrease)
```

## 7. Implementation Considerations

### Concurrency Model

**Central coordinator**: Single-threaded scheduler that:

- Receives telemetry events from all slots
- Maintains canonical state
- Makes scheduling decisions
- Dispatches work to slots

**Lock-free updates**: Telemetry events are queued and processed in batches to minimize contention.

### Memory Management

**Per-key overhead**: ~200 bytes

- Virtual time: 8 bytes
- Timestamps: 24 bytes
- Counters: 8 bytes
- HashMap overhead: ~100 bytes

**Distribution tracking**:

- Recent durations VecDeque: 100 × 8 = 800 bytes per class
- Total: ~2KB for both classes

**Scalability**: 100K active keys = ~20MB memory (acceptable)

### Inactive Key Cleanup

**Timeout policy**: Remove keys that have been idle for > 5 minutes

```
For each key where !has_pending_task && !is_executing:
    if now - last_activity > 5 minutes:
        remove key from active_keys
```

**Virtual time adjustment**: When removing a key, update class state:

```
class.total_execution_time -= key.virtual_time
class.update_virtual_time()
```

### Initialization and Bootstrap

**Cold start**: When first key arrives in a class:

```
class.smoothed_p50 = 10.0  // Conservative 10ms default
class.smoothed_p90 = 100.0 // 10× ratio assumption
key.virtual_time = 0.0     // First key starts at zero
```

**New key arrival**: After class has history:

```
key.virtual_time = class.virtual_time  // Start at current average
```

This prevents new keys from either starving existing keys (if started at 0) or being starved (if started too high).

### Edge Case Handling

**All keys ineligible**: Select least-ahead key to prevent deadlock

```
selected_key = min(keys, |k| abs(k.virtual_time - class.virtual_time))
```

**Empty class with demand**: Immediately assign work to avoid idle slots

**Partition rebalance**: Smoothly transfer state for reassigned partitions

**Extreme duration outliers**: Bounds scale automatically, but may trigger alerts for investigation

## 8. Validation Strategy

### Unit Testing

**Virtual time arithmetic**: Verify VT advances correctly by task duration

**Class selection logic**: Verify convergence to 30/70 split

**Priority calculation**: Test all combinations of fairness/wait/pass urgency

**Percentile tracking**: Verify EMA convergence properties

### Integration Testing

**Traffic regimes** (from simulation):

- Steady state: Uniform arrivals, stable durations
- Burst: Sudden 10× increase in one class
- Heterogeneous: Mix of short and long tasks
- Long tail: Extreme duration variance
- Flash crowd: Simultaneous arrival of many keys
- Phase change: Gradual distribution shift

**Validation criteria per regime**:

- Class proportions converge within 100 tasks
- VT spread stays bounded
- No starvation observed
- Urgency mechanisms activate appropriately

### Observability in Production

**Dashboard metrics**:

- Real-time class proportion tracking
- Virtual time spread evolution
- Priority score distributions
- Wait time CDFs

**Alerting thresholds**:

- proportion_error > 5% for > 1 minute
- vt_spread > 2× max_lag
- max_wait > wait_bound
- max_passes > 80

**Debug traces**: Sample scheduling decisions with full state snapshot

## 9. Why This Works: Intuitive Summary

**Fairness through time accounting**: By tracking execution time rather than task count, we ensure keys with different
task characteristics receive equal resources.

**Hierarchical fairness**: Two-level selection (class, then key) allows independent tuning of inter-class proportions
and intra-class fairness.

**Adaptive bounds**: Using distribution percentiles ensures bounds scale with workload characteristics, preventing both
starvation and excessive unfairness.

**Negative feedback loops**: Score-based selection naturally corrects imbalances—underserved classes/keys get priority
until balance restored.

**Starvation prevention**: Multiple urgency mechanisms (wait time, pass count) provide safety nets when fairness alone
would cause problems.

**Bounded guarantees**: Mathematical bounds on lag, wait time, and passes provide predictable worst-case behavior.

The system is stable because urgency adjustments are subtractive and bounded, preventing runaway feedback, while still
providing strong enough signals to prevent starvation.