# ih-muse-macos

`ih-muse-macos` sends local Mac metrics to Poet with enough hierarchy for AGS
navigation. It keeps metric families separate: CPU elements receive CPU metrics,
memory elements receive memory metrics, disk volumes receive disk metrics,
network interfaces receive network metrics, and application/process elements
receive their own process counters.

The default tree is:

- host
- resource groups: CPU, Memory, Storage, Network, Power, Thermal, System, and
  Applications
- resource leaves: total CPU, CPU cores, physical memory, swap, mounted
  volumes, network interfaces, internal battery, thermal sensors, and load
  average
- application groups with high-resource process children

The collector records process names, PID, parent PID, and start time so a
process remains stable within a run. It does not collect command lines,
environment variables, or full executable paths. Per-process CPU, memory, and
disk counters are live. Per-process network metric definitions exist, but the
default collector leaves those values empty until a reliable macOS sampler is
available; interface-level network bytes are collected now.

For three local samples after Poet is running:

```sh
cargo run -- --samples 3 --interval-seconds 2 --poet-url http://127.0.0.1:8000
```

Set `IH_MUSE_INTERVAL_SECONDS` to change the continuous collection interval.
Set `IH_MUSE_TOP_PROCESSES` or `--top-processes` to change the process cap.
Set `IH_MUSE_PROCESS_NETWORK=false` to skip the best-effort `nettop` process
network sampler. `--once` sends one sample; `--samples` provides a bounded
collection run for local smoke tests.
