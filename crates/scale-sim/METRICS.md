# Laboratory metrics

The laboratory uses one metric vocabulary for all regimes. Each plot uses the same units and percentile definitions.

## Source survey

| Work | Measurements copied into the laboratory | Regimes copied into the laboratory |
| --- | --- | --- |
| [Power of two choices](https://www2.eecs.berkeley.edu/Pubs/TechRpts/1996/7979.html) | Queue mean, queue maximum, and load distribution | Light load, heavy load, and placement skew |
| [Join-Idle-Queue](https://www.microsoft.com/en-us/research/wp-content/uploads/2011/10/idleq.pdf) | Response time, queue delay, utilization, and message overhead | Service-time distributions and very high load |
| [Sparrow](https://cs.stanford.edu/~matei/papers/2013/sosp_sparrow.pdf) | Job latency, queue time, service time, throughput, and probe overhead | Short tasks, parallel jobs, high utilization, and scheduler failure |
| [C3](https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf) | Mean and tail latency, throughput, load distribution, and oscillation | Zipf skew, heterogeneous replicas, stale feedback, and high utilization |
| [Prequal](https://www.usenix.org/system/files/nsdi24-wydrowski.pdf) | Tail latency, requests in flight, CPU distribution, probe rate, and timeouts | Antagonist load, heterogeneous speed, stale probes, and overload |
| [RackSched](https://www.usenix.org/system/files/osdi20-zhu.pdf) | Tail latency, throughput, scale efficiency, and queue depth | Low and high service dispersion, saturation, and server changes |
| [Autopilot](https://john.e-wilkes.com/papers/2020-EuroSys-Autopilot.pdf) | Resource slack, allocation, throttling risk, and failure rate | Short history, drift, and long steady operation |
| [FIRM](https://www.usenix.org/system/files/osdi20-qiu.pdf) | SLO violations, tail latency, utilization, mitigation time, and action count | Resource contention, anomaly intensity, and multiple faults |
| [Sinan](https://people.csail.mit.edu/delimitrou/papers/2021.asplos.sinan.pdf) | Tail latency, QoS violations, utilization, prediction error, and action history | Dependency effects, model drift, and unseen violations |

## Required graph families

Each principal regime produces these graph families:

- Plot offered arrivals, timer releases, backlog, useful throughput, and failures.
- Plot latency percentiles, queue length, requests in flight, and the SLO budget.
- Plot replica count, target, cap, Hold, scale actions, and cumulative replica-seconds.
- Plot handler utilization, dependency utilization, and utilization imbalance.
- Plot live concurrency, useful rate, the capacity posterior, and passive resource windows.
- Plot prediction intervals, observations, expected loss, and model mismatch.
- Plot snapshot age, missing reporters, rebalance time, warm-up time, and recovery time.
- Plot controller time, memory, bytes touched, and allocation counts separately.

Use identical scales when two regimes are compared. Mark missing intervals explicitly.
