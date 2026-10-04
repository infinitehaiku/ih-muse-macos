pub mod dashboards;
pub mod graph;
pub mod logs;

use ih_muse_proto::{
    metric_id_from_code, ElementKindRegistration, MetricDefinition, MetricPayload,
};

pub const CPU_USAGE_METRIC: &str = "host.cpu.usage_percent";
pub const CPU_CORE_USAGE_METRIC: &str = "host.cpu.core_usage_percent";
pub const MEMORY_USAGE_METRIC: &str = "host.memory.usage_percent";
pub const MEMORY_USED_BYTES_METRIC: &str = "host.memory.used_bytes";
pub const SWAP_USAGE_METRIC: &str = "host.memory.swap_usage_percent";
pub const SWAP_USED_BYTES_METRIC: &str = "host.memory.swap_used_bytes";
pub const LOAD_ONE_METRIC: &str = "host.load.one";
pub const DISK_USAGE_METRIC: &str = "host.disk.usage_percent";
pub const DISK_USED_BYTES_METRIC: &str = "host.disk.used_bytes";
pub const DISK_AVAILABLE_BYTES_METRIC: &str = "host.disk.available_bytes";
pub const NETWORK_RECEIVED_BYTES_METRIC: &str = "host.network.received_bytes_delta";
pub const NETWORK_TRANSMITTED_BYTES_METRIC: &str = "host.network.transmitted_bytes_delta";
pub const NETWORK_TOTAL_RECEIVED_BYTES_METRIC: &str = "host.network.total_received_bytes";
pub const NETWORK_TOTAL_TRANSMITTED_BYTES_METRIC: &str = "host.network.total_transmitted_bytes";
pub const BATTERY_CHARGE_METRIC: &str = "host.battery.charge_percent";
pub const BATTERY_ON_BATTERY_METRIC: &str = "host.battery.on_battery";
pub const BATTERY_CHARGING_METRIC: &str = "host.battery.charging";
pub const BATTERY_DRAIN_RATE_METRIC: &str = "host.battery.drain_percent_per_hour";
pub const THERMAL_TEMPERATURE_METRIC: &str = "host.thermal.temperature_celsius";
pub const PROCESS_CPU_USAGE_METRIC: &str = "process.cpu.usage_percent";
pub const PROCESS_CPU_CAPACITY_METRIC: &str = "process.cpu.capacity_percent";
pub const POET_CPU_SECONDS_METRIC: &str = "process.poet.runtime.cpu_seconds";
pub const POET_UPTIME_SECONDS_METRIC: &str = "process.poet.runtime.uptime_seconds";
pub const POET_CURRENT_RSS_METRIC: &str = "process.poet.runtime.current_rss_bytes";
pub const POET_PEAK_RSS_METRIC: &str = "process.poet.runtime.peak_rss_bytes";
pub const POET_HEALTH_AGE_METRIC: &str = "process.poet.health.sample_age_seconds";
pub const POET_MANAGED_MEMORY_METRIC: &str = "process.poet.storage.managed_memory_bytes";
pub const POET_QUEUE_BYTES_METRIC: &str = "process.poet.storage.queue_bytes";
pub const POET_STORED_BYTES_METRIC: &str = "process.poet.storage.stored_bytes";
pub const POET_SEGMENT_COUNT_METRIC: &str = "process.poet.storage.segment_count";
pub const POET_COVERAGE_METRIC: &str = "process.poet.storage.coverage_ratio";
pub const POET_REQUESTED_RESOLUTION_METRIC: &str =
    "process.poet.storage.requested_resolution_nanoseconds";
pub const POET_EFFECTIVE_RESOLUTION_METRIC: &str =
    "process.poet.storage.effective_resolution_nanoseconds";
pub const POET_REQUESTED_HORIZON_METRIC: &str =
    "process.poet.storage.requested_horizon_nanoseconds";
pub const POET_EFFECTIVE_HORIZON_METRIC: &str =
    "process.poet.storage.effective_horizon_nanoseconds";
pub const POET_DERIVATION_LAG_METRIC: &str = "process.poet.storage.derivation_lag_nanoseconds";
pub const POET_DELIVERY_ACCEPTED_METRIC: &str = "process.poet.delivery.accepted";
pub const POET_DELIVERY_REJECTED_METRIC: &str = "process.poet.delivery.rejected";
pub const POET_DELIVERY_DROPPED_METRIC: &str = "process.poet.delivery.dropped";
pub const POET_DELIVERY_FAILED_METRIC: &str = "process.poet.delivery.failed";
pub const POET_DELIVERY_UNKNOWN_METRIC: &str = "process.poet.delivery.unknown";
/// The Muse's unified log collection (counters since start, on the "Log
/// collection" element; `queued` is a level).
pub const MUSE_LOGS_COLLECTED_METRIC: &str = "muse.logs.collected";
pub const MUSE_LOGS_SENT_METRIC: &str = "muse.logs.sent";
pub const MUSE_LOGS_FILTERED_METRIC: &str = "muse.logs.filtered_at_muse";
pub const MUSE_LOGS_DROPPED_RATE_METRIC: &str = "muse.logs.dropped_rate_limited";
pub const MUSE_LOGS_DROPPED_QUEUE_METRIC: &str = "muse.logs.dropped_queue_full";
pub const MUSE_LOGS_DROPPED_REJECTED_METRIC: &str = "muse.logs.dropped_rejected";
pub const MUSE_LOGS_UNPARSABLE_METRIC: &str = "muse.logs.unparsable";
pub const MUSE_LOGS_TRUNCATED_METRIC: &str = "muse.logs.truncated";
pub const MUSE_LOGS_REDACTED_METRIC: &str = "muse.logs.redacted";
pub const MUSE_LOGS_SEND_FAILURES_METRIC: &str = "muse.logs.send_failures";
pub const MUSE_LOGS_RESTARTS_METRIC: &str = "muse.logs.stream_restarts";
pub const MUSE_LOGS_QUEUED_METRIC: &str = "muse.logs.queued";

/// Converts one-core percentages into a share of the machine's logical CPU capacity.
pub fn cpu_capacity_percent(core_percent: f32, logical_cpus: usize) -> Option<f32> {
    (logical_cpus > 0 && core_percent.is_finite() && core_percent >= 0.0)
        .then(|| (core_percent / logical_cpus as f32).min(100.0))
}
pub const PROCESS_MEMORY_BYTES_METRIC: &str = "process.memory.rss_bytes";
pub const PROCESS_DISK_READ_BYTES_METRIC: &str = "process.disk.read_bytes_delta";
pub const PROCESS_DISK_WRITE_BYTES_METRIC: &str = "process.disk.write_bytes_delta";
pub const PROCESS_NETWORK_RECEIVED_BYTES_METRIC: &str = "process.network.received_bytes_delta";
pub const PROCESS_NETWORK_TRANSMITTED_BYTES_METRIC: &str =
    "process.network.transmitted_bytes_delta";

pub const KIND_MACOS_HOST: &str = "macos_host";
pub const KIND_MACOS_RESOURCE_GROUP: &str = "macos_resource_group";
pub const KIND_MACOS_RESOURCE: &str = "macos_resource";
pub const KIND_MACOS_CPU_CORE: &str = "macos_cpu_core";
pub const KIND_MACOS_DISK_VOLUME: &str = "macos_disk_volume";
pub const KIND_MACOS_NETWORK_INTERFACE: &str = "macos_network_interface";
pub const KIND_MACOS_POWER_SOURCE: &str = "macos_power_source";
pub const KIND_MACOS_THERMAL_SENSOR: &str = "macos_thermal_sensor";
pub const KIND_MACOS_APPLICATION: &str = "macos_application";
pub const KIND_MACOS_PROCESS: &str = "macos_process";

#[derive(Clone, Debug, PartialEq)]
pub struct HostSnapshot {
    pub cpu_usage_percent: f32,
    pub memory_usage_percent: f32,
    pub used_memory_bytes: u64,
    pub total_memory_bytes: u64,
    pub swap_usage_percent: f32,
    pub used_swap_bytes: u64,
    pub total_swap_bytes: u64,
    pub load_one: f32,
    pub disk_usage_percent: f32,
    pub used_disk_bytes: u64,
    pub available_disk_bytes: u64,
    pub total_disk_bytes: u64,
}

impl HostSnapshot {
    pub fn cpu_metrics(&self, timestamp: i64, element_id: u64) -> Option<MetricPayload> {
        metric_payload(
            timestamp,
            element_id,
            &[MetricReading::new(
                CPU_USAGE_METRIC,
                self.cpu_usage_percent,
            )?],
        )
    }

    pub fn memory_metrics(&self, timestamp: i64, element_id: u64) -> Option<MetricPayload> {
        metric_payload(
            timestamp,
            element_id,
            &[
                MetricReading::new(MEMORY_USAGE_METRIC, self.memory_usage_percent)?,
                MetricReading::new(MEMORY_USED_BYTES_METRIC, self.used_memory_bytes as f64)?,
            ],
        )
    }

    pub fn swap_metrics(&self, timestamp: i64, element_id: u64) -> Option<MetricPayload> {
        metric_payload(
            timestamp,
            element_id,
            &[
                MetricReading::new(SWAP_USAGE_METRIC, self.swap_usage_percent)?,
                MetricReading::new(SWAP_USED_BYTES_METRIC, self.used_swap_bytes as f64)?,
            ],
        )
    }

    pub fn load_metrics(&self, timestamp: i64, element_id: u64) -> Option<MetricPayload> {
        metric_payload(
            timestamp,
            element_id,
            &[MetricReading::new(LOAD_ONE_METRIC, self.load_one)?],
        )
    }

    pub fn disk_metrics(&self, timestamp: i64, element_id: u64) -> Option<MetricPayload> {
        metric_payload(
            timestamp,
            element_id,
            &[
                MetricReading::new(DISK_USAGE_METRIC, self.disk_usage_percent)?,
                MetricReading::new(DISK_USED_BYTES_METRIC, self.used_disk_bytes as f64)?,
                MetricReading::new(
                    DISK_AVAILABLE_BYTES_METRIC,
                    self.available_disk_bytes as f64,
                )?,
            ],
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BatterySnapshot {
    pub charge_percent: f32,
    pub on_battery: bool,
    pub charging: bool,
}

/// One named sample. `f64`, so byte counters stay exact up to 2^53.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricReading {
    pub code: &'static str,
    pub value: f64,
}

impl MetricReading {
    pub fn new(code: &'static str, value: impl Into<f64>) -> Option<Self> {
        let value = value.into();
        value.is_finite().then_some(Self { code, value })
    }
}

/// Samples that were collected but not yet acknowledged by Poet.
///
/// A failed send keeps the sample (with its original timestamps) for the next
/// attempt instead of dropping it. The queue is bounded: past `max_samples` the
/// oldest sample is discarded and counted in `dropped_samples`.
#[derive(Debug)]
pub struct PendingSamples<T = Vec<MetricPayload>> {
    queue: std::collections::VecDeque<T>,
    max_samples: usize,
    dropped_samples: u64,
}

impl<T> PendingSamples<T> {
    pub fn new(max_samples: usize) -> Self {
        Self {
            queue: std::collections::VecDeque::new(),
            max_samples: max_samples.max(1),
            dropped_samples: 0,
        }
    }

    /// Queues one sample; returns how many old samples were dropped.
    pub fn push(&mut self, sample: T) -> u64 {
        self.queue.push_back(sample);
        let mut dropped = 0;
        while self.queue.len() > self.max_samples {
            self.queue.pop_front();
            dropped += 1;
        }
        self.dropped_samples += dropped;
        dropped
    }

    /// The oldest unsent sample.
    pub fn oldest(&self) -> Option<&T> {
        self.queue.front()
    }

    /// Marks the oldest sample as acknowledged.
    pub fn acknowledge_oldest(&mut self) {
        self.queue.pop_front();
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn dropped_samples(&self) -> u64 {
        self.dropped_samples
    }
}

pub fn metric_payload(
    timestamp: i64,
    element_id: u64,
    readings: &[MetricReading],
) -> Option<MetricPayload> {
    if readings.is_empty() {
        return None;
    }
    Some(MetricPayload::new(
        timestamp,
        element_id,
        readings
            .iter()
            .map(|reading| metric_id_from_code(reading.code))
            .collect(),
        readings.iter().map(|reading| Some(reading.value)).collect(),
    ))
}

pub fn metric_definitions() -> Vec<MetricDefinition> {
    let mut definitions = vec![
        poet_metric(POET_CPU_SECONDS_METRIC, "Poet CPU time", "Cumulative CPU time used by this Poet process", "seconds"),
        poet_metric(POET_UPTIME_SECONDS_METRIC, "Poet uptime", "Elapsed time in this Poet process lifetime", "seconds"),
        poet_metric(POET_CURRENT_RSS_METRIC, "Poet current memory", "Current resident memory used by this Poet process", "bytes"),
        poet_metric(POET_PEAK_RSS_METRIC, "Poet peak memory", "Peak resident memory in this Poet process lifetime", "bytes"),
        poet_metric(POET_HEALTH_AGE_METRIC, "Poet health freshness", "Age of the sampled Poet health report", "seconds"),
        poet_metric(POET_MANAGED_MEMORY_METRIC, "Poet managed memory", "Memory directly accounted by the native Poet store", "bytes"),
        poet_metric(POET_QUEUE_BYTES_METRIC, "Poet queued storage", "Native storage bytes awaiting derivation or compaction", "bytes"),
        poet_metric(POET_STORED_BYTES_METRIC, "Poet stored data", "Bytes retained across native Poet storage roots", "bytes"),
        poet_metric(POET_SEGMENT_COUNT_METRIC, "Poet segments", "Committed native storage segment count", "number"),
        poet_metric(POET_COVERAGE_METRIC, "Poet coverage", "Fraction of expected retained observations currently available", "ratio"),
        poet_metric(POET_REQUESTED_RESOLUTION_METRIC, "Poet requested resolution", "Configured finest retained resolution", "nanoseconds"),
        poet_metric(POET_EFFECTIVE_RESOLUTION_METRIC, "Poet effective resolution", "Finest resolution currently retained after pressure actions", "nanoseconds"),
        poet_metric(POET_REQUESTED_HORIZON_METRIC, "Poet requested horizon", "Configured duration of retained history", "nanoseconds"),
        poet_metric(POET_EFFECTIVE_HORIZON_METRIC, "Poet effective horizon", "History duration currently retained after pressure actions", "nanoseconds"),
        poet_metric(POET_DERIVATION_LAG_METRIC, "Poet derivation lag", "Time between durable source data and its materialized views", "nanoseconds"),
        poet_metric(POET_DELIVERY_ACCEPTED_METRIC, "Poet accepted deliveries", "Records accepted by native Poet storage", "number"),
        poet_metric(POET_DELIVERY_REJECTED_METRIC, "Poet rejected deliveries", "Records rejected before durable acknowledgement", "number"),
        poet_metric(POET_DELIVERY_DROPPED_METRIC, "Poet dropped deliveries", "Accepted records later reported as dropped", "number"),
        poet_metric(POET_DELIVERY_FAILED_METRIC, "Poet failed deliveries", "Accepted records whose durable processing failed", "number"),
        poet_metric(POET_DELIVERY_UNKNOWN_METRIC, "Poet unknown deliveries", "Records whose final delivery outcome is unknown", "number"),
        MetricDefinition::new(CPU_USAGE_METRIC, "CPU usage", "Host CPU usage percentage"),
        MetricDefinition::new(
            CPU_CORE_USAGE_METRIC,
            "CPU core usage",
            "Per-core CPU usage percentage",
        ),
        MetricDefinition::new(
            MEMORY_USAGE_METRIC,
            "Memory usage",
            "Host memory usage percentage",
        ),
        MetricDefinition::new(
            MEMORY_USED_BYTES_METRIC,
            "Memory used",
            "Host resident memory used in bytes",
        ),
        MetricDefinition::new(
            SWAP_USAGE_METRIC,
            "Swap usage",
            "Host swap usage percentage",
        ),
        MetricDefinition::new(
            SWAP_USED_BYTES_METRIC,
            "Swap used",
            "Host swap used in bytes",
        ),
        MetricDefinition::new(
            LOAD_ONE_METRIC,
            "Load average",
            "One-minute host load average",
        ),
        MetricDefinition::new(
            DISK_USAGE_METRIC,
            "Disk usage",
            "Aggregate local disk usage percentage",
        ),
        MetricDefinition::new(DISK_USED_BYTES_METRIC, "Disk used", "Local disk bytes used"),
        MetricDefinition::new(
            DISK_AVAILABLE_BYTES_METRIC,
            "Disk available",
            "Local disk bytes available",
        ),
        MetricDefinition::new(
            NETWORK_RECEIVED_BYTES_METRIC,
            "Network received",
            "Network bytes received since the previous sample",
        ),
        MetricDefinition::new(
            NETWORK_TRANSMITTED_BYTES_METRIC,
            "Network transmitted",
            "Network bytes transmitted since the previous sample",
        ),
        MetricDefinition::new(
            NETWORK_TOTAL_RECEIVED_BYTES_METRIC,
            "Network received total",
            "Total network bytes received since boot",
        ),
        MetricDefinition::new(
            NETWORK_TOTAL_TRANSMITTED_BYTES_METRIC,
            "Network transmitted total",
            "Total network bytes transmitted since boot",
        ),
        MetricDefinition::new(
            BATTERY_CHARGE_METRIC,
            "Battery charge",
            "Battery charge percentage",
        ),
        MetricDefinition::new(
            BATTERY_ON_BATTERY_METRIC,
            "On battery",
            "One when the Mac is drawing from the battery, zero otherwise",
        ),
        MetricDefinition::new(
            BATTERY_CHARGING_METRIC,
            "Battery charging",
            "One when the battery is charging, zero otherwise",
        ),
        MetricDefinition::new(
            BATTERY_DRAIN_RATE_METRIC,
            "Battery drain",
            "Estimated battery drain percentage per hour",
        ),
        MetricDefinition::new(
            THERMAL_TEMPERATURE_METRIC,
            "Temperature",
            "Thermal sensor temperature in degrees Celsius",
        ),
        MetricDefinition::new(
            PROCESS_CPU_USAGE_METRIC,
            "Process CPU (core %)",
            "100 percent is one logical CPU; this can exceed 100 on multicore hosts",
        ),
        MetricDefinition::new(
            PROCESS_CPU_CAPACITY_METRIC,
            "Process CPU",
            "Share of machine logical CPU capacity; collected processes may not cover all host activity",
        ),
        MetricDefinition::new(
            PROCESS_MEMORY_BYTES_METRIC,
            "Process memory",
            "Process resident memory in bytes",
        ),
        MetricDefinition::new(
            PROCESS_DISK_READ_BYTES_METRIC,
            "Process disk read",
            "Process disk bytes read since the previous sample",
        ),
        MetricDefinition::new(
            PROCESS_DISK_WRITE_BYTES_METRIC,
            "Process disk write",
            "Process disk bytes written since the previous sample",
        ),
        MetricDefinition::new(
            PROCESS_NETWORK_RECEIVED_BYTES_METRIC,
            "Process network received",
            "Process network bytes received since the previous sample when available",
        ),
        MetricDefinition::new(
            PROCESS_NETWORK_TRANSMITTED_BYTES_METRIC,
            "Process network transmitted",
            "Process network bytes transmitted since the previous sample when available",
        ),
    ];
    for definition in &mut definitions {
        if let Some(display) = &mut definition.display {
            let kind = if definition.code.starts_with("process.") {
                KIND_MACOS_PROCESS
            } else if definition.code.contains(".core_") {
                KIND_MACOS_CPU_CORE
            } else if definition.code.contains(".disk.") {
                KIND_MACOS_DISK_VOLUME
            } else if definition.code.contains(".network.") {
                KIND_MACOS_NETWORK_INTERFACE
            } else if definition.code.contains(".battery.") {
                KIND_MACOS_POWER_SOURCE
            } else if definition.code.contains(".thermal.") {
                KIND_MACOS_THERMAL_SENSOR
            } else {
                KIND_MACOS_RESOURCE
            };
            display.element_kinds = if definition.code.starts_with("process.") {
                // Process metrics are also published as additive application
                // summaries, so the same metric supports parent/child zoom.
                vec![KIND_MACOS_APPLICATION.into(), KIND_MACOS_PROCESS.into()]
            } else {
                vec![kind.into()]
            };
            display.direction = if definition.code.ends_with("charge_percent")
                || definition.code.ends_with("available_bytes")
            {
                "lower_is_worse"
            } else if definition.code.ends_with("celsius")
                || definition.code.ends_with("usage_percent")
            {
                "higher_is_worse"
            } else {
                "neutral"
            }
            .into();
        }
    }
    definitions.extend(muse_log_metric_definitions());
    definitions
}

/// Definitions of the unified log collection counters ([`logs::LogStats`]).
fn muse_log_metric_definitions() -> Vec<MetricDefinition> {
    [
        (MUSE_LOGS_COLLECTED_METRIC, "Logs collected", "Unified log records kept by the Muse"),
        (MUSE_LOGS_SENT_METRIC, "Logs sent", "Log records a Poet acknowledged"),
        (MUSE_LOGS_FILTERED_METRIC, "Logs filtered at Muse", "Lines the Muse read but did not keep"),
        (MUSE_LOGS_DROPPED_RATE_METRIC, "Logs over rate", "Records dropped by the rate limit"),
        (MUSE_LOGS_DROPPED_QUEUE_METRIC, "Logs over queue", "Oldest records dropped while no Poet accepted them"),
        (MUSE_LOGS_DROPPED_REJECTED_METRIC, "Logs refused", "Records a Poet refused"),
        (MUSE_LOGS_UNPARSABLE_METRIC, "Logs unreadable", "Lines skipped as unreadable or too long"),
        (MUSE_LOGS_TRUNCATED_METRIC, "Logs cut", "Records whose text was cut to the size limit"),
        (MUSE_LOGS_REDACTED_METRIC, "Logs redacted", "Records with user paths or addresses masked"),
        (MUSE_LOGS_SEND_FAILURES_METRIC, "Log send failures", "Log requests no Poet answered"),
        (MUSE_LOGS_RESTARTS_METRIC, "Log stream restarts", "Times the unified log stream was restarted"),
        (MUSE_LOGS_QUEUED_METRIC, "Logs queued", "Records waiting for a Poet"),
    ]
    .into_iter()
    .map(|(code, name, description)| {
        let mut definition = MetricDefinition::new(code, name, description);
        if let Some(display) = &mut definition.display {
            display.unit = "number".into();
            display.aggregation = "none".into();
            display.kind = if code == MUSE_LOGS_QUEUED_METRIC { "gauge" } else { "counter" }.into();
            display.element_kinds = vec![KIND_MACOS_RESOURCE.into()];
            display.direction = "neutral".into();
        }
        definition
    })
    .collect()
}

/// The readings of the log collection element for one sample.
pub fn log_stats_readings(stats: &logs::LogStats) -> Vec<MetricReading> {
    [
        (MUSE_LOGS_COLLECTED_METRIC, stats.collected),
        (MUSE_LOGS_SENT_METRIC, stats.sent),
        (MUSE_LOGS_FILTERED_METRIC, stats.filtered),
        (MUSE_LOGS_DROPPED_RATE_METRIC, stats.dropped_rate_limited),
        (MUSE_LOGS_DROPPED_QUEUE_METRIC, stats.dropped_queue_full),
        (MUSE_LOGS_DROPPED_REJECTED_METRIC, stats.dropped_rejected),
        (MUSE_LOGS_UNPARSABLE_METRIC, stats.unparsable),
        (MUSE_LOGS_TRUNCATED_METRIC, stats.truncated),
        (MUSE_LOGS_REDACTED_METRIC, stats.redacted),
        (MUSE_LOGS_SEND_FAILURES_METRIC, stats.send_failures),
        (MUSE_LOGS_RESTARTS_METRIC, stats.stream_restarts),
        (MUSE_LOGS_QUEUED_METRIC, stats.queued),
    ]
    .into_iter()
    .filter_map(|(code, value)| MetricReading::new(code, value as f64))
    .collect()
}

fn poet_metric(code: &str, name: &str, description: &str, unit: &str) -> MetricDefinition {
    let mut definition = MetricDefinition::new(code, name, description);
    if let Some(display) = &mut definition.display {
        display.unit = unit.into();
        display.aggregation = "none".into();
        display.kind = if code == POET_CPU_SECONDS_METRIC || code.contains(".delivery.") {
            "counter"
        } else {
            "gauge"
        }
        .into();
        display.element_kinds = vec![KIND_MACOS_PROCESS.into()];
    }
    definition
}

pub fn element_kind_definitions() -> Vec<ElementKindRegistration> {
    vec![
        ElementKindRegistration::new(
            KIND_MACOS_HOST,
            None,
            "macOS host",
            "The local macOS machine",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_RESOURCE_GROUP,
            Some(KIND_MACOS_HOST),
            "macOS resource group",
            "A local host resource group",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_RESOURCE,
            Some(KIND_MACOS_RESOURCE_GROUP),
            "macOS resource",
            "A measured local host resource",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_CPU_CORE,
            Some(KIND_MACOS_RESOURCE_GROUP),
            "macOS CPU core",
            "A logical CPU core",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_DISK_VOLUME,
            Some(KIND_MACOS_RESOURCE_GROUP),
            "macOS disk volume",
            "A mounted local disk volume",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_NETWORK_INTERFACE,
            Some(KIND_MACOS_RESOURCE_GROUP),
            "macOS network interface",
            "A network interface on the local Mac",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_POWER_SOURCE,
            Some(KIND_MACOS_RESOURCE_GROUP),
            "macOS power source",
            "A local power source such as the internal battery",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_THERMAL_SENSOR,
            Some(KIND_MACOS_RESOURCE_GROUP),
            "macOS thermal sensor",
            "A local thermal sensor exposed by macOS",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_APPLICATION,
            Some(KIND_MACOS_RESOURCE_GROUP),
            "macOS application",
            "A running application or executable group",
        ),
        ElementKindRegistration::new(
            KIND_MACOS_PROCESS,
            Some(KIND_MACOS_APPLICATION),
            "macOS process",
            "A running process instance",
        ),
    ]
}

pub fn parse_pmset_battery(output: &str) -> Option<BatterySnapshot> {
    let lower = output.to_ascii_lowercase();
    let percent_index = output.find('%')?;
    let charge_start = output[..percent_index]
        .rfind(|c: char| !(c.is_ascii_digit() || c == '.'))
        .map_or(0, |idx| idx + 1);
    let charge_percent = output[charge_start..percent_index].trim().parse().ok()?;
    let on_battery =
        lower.contains("battery power") || lower.contains("discharging") && !lower.contains("ac ");
    let charging = !lower.contains("discharging")
        && !lower.contains("not charging")
        && lower.contains("charging");
    Some(BatterySnapshot {
        charge_percent,
        on_battery,
        charging,
    })
}

pub fn snapshot_from_values(
    cpu_usage_percent: f32,
    used_memory: u64,
    total_memory: u64,
    load_one: f64,
    available_disk: u64,
    total_disk: u64,
) -> HostSnapshot {
    let used_disk = total_disk.saturating_sub(available_disk);
    HostSnapshot {
        cpu_usage_percent: cpu_usage_percent.clamp(0.0, 100.0),
        memory_usage_percent: percentage(used_memory, total_memory),
        used_memory_bytes: used_memory,
        total_memory_bytes: total_memory,
        swap_usage_percent: 0.0,
        used_swap_bytes: 0,
        total_swap_bytes: 0,
        load_one: load_one.max(0.0) as f32,
        disk_usage_percent: percentage(used_disk, total_disk),
        used_disk_bytes: used_disk,
        available_disk_bytes: available_disk,
        total_disk_bytes: total_disk,
    }
}

fn percentage(numerator: u64, denominator: u64) -> f32 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 * 100.0 / denominator as f64) as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_log_collection_reading_has_a_counter_definition() {
        let definitions = metric_definitions();
        let stats = logs::LogStats { collected: 3, dropped_rate_limited: 2, queued: 1, ..Default::default() };
        let readings = log_stats_readings(&stats);
        assert_eq!(readings.len(), 12);
        for reading in &readings {
            let definition = definitions.iter().find(|definition| definition.code == reading.code).expect(reading.code);
            let display = definition.display.as_ref().unwrap();
            assert_eq!(display.element_kinds, vec![KIND_MACOS_RESOURCE.to_string()]);
            let expected = if reading.code == MUSE_LOGS_QUEUED_METRIC { "gauge" } else { "counter" };
            assert_eq!(display.kind, expected, "{}", reading.code);
        }
        assert!(readings.iter().any(|reading| reading.code == MUSE_LOGS_DROPPED_RATE_METRIC && reading.value == 2.0));
        let codes = definitions.iter().map(|definition| definition.code.as_str()).collect::<std::collections::HashSet<_>>();
        assert_eq!(codes.len(), definitions.len(), "metric codes stay unique");
    }

    #[test]
    fn pending_samples_keep_unsent_data_in_order_and_bound_memory() {
        let payload = |time| vec![MetricPayload::new(time, 1, vec![1], vec![Some(1.0)])];
        let mut pending = PendingSamples::new(2);
        assert_eq!(pending.push(payload(1)), 0);
        assert_eq!(pending.push(payload(2)), 0);
        assert_eq!(pending.push(payload(3)), 1);
        assert_eq!(pending.dropped_samples(), 1);
        assert_eq!(pending.oldest().unwrap()[0].time, 2);
        pending.acknowledge_oldest();
        assert_eq!(pending.oldest().unwrap()[0].time, 3);
        pending.acknowledge_oldest();
        assert!(pending.is_empty());
    }

    #[test]
    fn readings_keep_byte_counters_exact() {
        let bytes = 16_777_217_u64; // 2^24 + 1 is not representable in f32
        let reading = MetricReading::new("memory_used_bytes", bytes as f64).unwrap();
        assert_eq!(reading.value, 16_777_217.0);
    }

    #[test]
    fn every_metric_declares_units_and_measured_element_kind() {
        for metric in metric_definitions() {
            let display = metric.display.unwrap();
            assert!(!display.unit.is_empty());
            if metric.code.starts_with("process.") {
                assert_eq!(
                    display.element_kinds,
                    vec![KIND_MACOS_APPLICATION, KIND_MACOS_PROCESS]
                );
            } else {
                assert_eq!(display.element_kinds.len(), 1);
            }
        }
    }
    use ih_muse_proto::metric_id_from_code;

    #[test]
    fn maps_fixed_host_values_to_independent_metric_payloads() {
        let snapshot = snapshot_from_values(42.0, 3, 4, 1.5, 25, 100);
        let cpu_payload = snapshot.cpu_metrics(10, 7).unwrap();
        let memory_payload = snapshot.memory_metrics(10, 8).unwrap();
        let disk_payload = snapshot.disk_metrics(10, 9).unwrap();

        assert_eq!(cpu_payload.time, 10);
        assert_eq!(cpu_payload.element_id, 7);
        assert_eq!(
            cpu_payload.metric_ids,
            vec![metric_id_from_code(CPU_USAGE_METRIC)]
        );
        assert_eq!(cpu_payload.values, vec![Some(42.0)]);

        assert_eq!(
            memory_payload.metric_ids,
            vec![
                metric_id_from_code(MEMORY_USAGE_METRIC),
                metric_id_from_code(MEMORY_USED_BYTES_METRIC)
            ]
        );
        assert_eq!(memory_payload.values, vec![Some(75.0), Some(3.0)]);

        assert_eq!(
            disk_payload.metric_ids,
            vec![
                metric_id_from_code(DISK_USAGE_METRIC),
                metric_id_from_code(DISK_USED_BYTES_METRIC),
                metric_id_from_code(DISK_AVAILABLE_BYTES_METRIC)
            ]
        );
        assert_eq!(
            disk_payload.values,
            vec![Some(75.0), Some(75.0), Some(25.0)]
        );
    }

    #[test]
    fn parses_pmset_battery_state() {
        let output = "Now drawing from 'AC Power'\n -InternalBattery-0\t80%; AC attached; not charging present: true\n";
        assert_eq!(
            parse_pmset_battery(output),
            Some(BatterySnapshot {
                charge_percent: 80.0,
                on_battery: false,
                charging: false,
            })
        );

        let output = "Now drawing from 'Battery Power'\n -InternalBattery-0\t79%; discharging; 4:00 remaining present: true\n";
        assert_eq!(
            parse_pmset_battery(output),
            Some(BatterySnapshot {
                charge_percent: 79.0,
                on_battery: true,
                charging: false,
            })
        );
    }

    #[test]
    fn process_cpu_capacity_preserves_core_units_and_rejects_unknown_capacity() {
        assert_eq!(cpu_capacity_percent(400.0, 12), Some(400.0 / 12.0));
        assert_eq!(cpu_capacity_percent(0.0, 12), Some(0.0));
        assert_eq!(cpu_capacity_percent(400.0, 0), None);
        assert_eq!(cpu_capacity_percent(f32::NAN, 12), None);
        assert_eq!(cpu_capacity_percent(-1.0, 12), None);
    }
}
