# Operator Flags

These flags configure the etcd-operator **manager process** itself (the
controller), as opposed to per-cluster settings expressed on the `EtcdCluster`
custom resource. They are passed as command-line arguments to the operator
binary (see `cmd/main.go`).

## `--max-concurrent-reconciles`

- **Type:** int
- **Default:** `5`

Number of reconcile workers the controller runs in parallel.
controller-runtime's own default is `1`.

Each `EtcdCluster` is reconciled on its own workqueue key — the queue dedups by
namespaced name — so a single cluster is **never** reconciled by two workers at
the same time. Concurrency therefore only ever parallelizes work across
**distinct** clusters; it does not introduce intra-cluster races.

A reconcile in this operator is relatively heavy and long-running: it patches a
StatefulSet, issues member-list and health RPCs against the managed etcd
cluster, and may perform certificate work. With a single worker, one slow
cluster blocks progress on every other cluster. A small pool (default `5`)
meaningfully improves throughput when many clusters need attention at once
(operator restart, mass upgrade, node churn).

The cost of a **larger** pool is more simultaneous load on the apiserver and on
the managed etcd clusters. Wise operators running large fleets should tune this
value for their environment; operators with a handful of clusters can leave it
at the default.

A value `<= 0` falls back to controller-runtime's default of a single worker,
which stays behaviorally safe.

```sh
# Widen to 10 workers for a large fleet.
manager --max-concurrent-reconciles=10
```

## `--etcd-cpu-request`

- **Type:** string (Kubernetes resource quantity)
- **Default:** `50m`

CPU **request** (never a limit) set on the etcd container of every managed
cluster.

Without any request, the etcd pod lands in the **BestEffort** QoS class. That
gives its cgroup the kernel-floor `cpu.shares` of `2` and makes it the first
workload the kubelet evicts under node memory pressure. Setting even a tiny CPU
request lifts the pod to **Burstable**, raises `cpu.shares` to ~`51` (a
scheduling floor that only matters under contention), and — because it is a
request and not a limit — **never throttles** etcd.

`50m` is deliberately tiny: it is a scheduling floor, not a reservation. It is
expressed as a controller-level flag rather than a CRD field because it is an
operator tuning lever that is identical for every cluster, which keeps it out of
the `EtcdCluster` API.

Set the value to an empty string (`""`) or `"0"` to apply **no** request,
restoring the original BestEffort behavior. This makes the effect easy to
A/B-measure and tune per fleet. A malformed quantity is treated the same as
unset (no request) so a typo can never wedge cluster creation.

```sh
# Default: 50m request, Burstable QoS.
manager --etcd-cpu-request=50m

# Opt out: no request, BestEffort QoS (original behavior).
manager --etcd-cpu-request=""

# Larger floor for busy clusters.
manager --etcd-cpu-request=200m
```
