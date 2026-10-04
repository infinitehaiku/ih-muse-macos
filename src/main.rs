use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use ih_muse_client::GraphPoetClient;
use ih_muse_macos::graph::GraphRegistry;
use ih_muse_proto::{GraphIntakeRequest, MetricPayload};
use sysinfo::{Components, Disks, Networks, ProcessesToUpdate, System};

use ih_muse_macos::{
    metric_definitions, metric_payload, parse_pmset_battery,
    snapshot_from_values, BatterySnapshot, MetricReading, PendingSamples, BATTERY_CHARGE_METRIC,
    BATTERY_CHARGING_METRIC, BATTERY_DRAIN_RATE_METRIC, BATTERY_ON_BATTERY_METRIC,
    CPU_CORE_USAGE_METRIC, DISK_AVAILABLE_BYTES_METRIC, DISK_USAGE_METRIC, DISK_USED_BYTES_METRIC,
    KIND_MACOS_APPLICATION, KIND_MACOS_CPU_CORE, KIND_MACOS_DISK_VOLUME, KIND_MACOS_HOST,
    KIND_MACOS_NETWORK_INTERFACE, KIND_MACOS_POWER_SOURCE, KIND_MACOS_PROCESS, KIND_MACOS_RESOURCE,
    KIND_MACOS_RESOURCE_GROUP, KIND_MACOS_THERMAL_SENSOR, NETWORK_RECEIVED_BYTES_METRIC,
    NETWORK_TOTAL_RECEIVED_BYTES_METRIC, NETWORK_TOTAL_TRANSMITTED_BYTES_METRIC,
    NETWORK_TRANSMITTED_BYTES_METRIC, POET_COVERAGE_METRIC, POET_CPU_SECONDS_METRIC,
    POET_CURRENT_RSS_METRIC, POET_DELIVERY_ACCEPTED_METRIC, POET_DELIVERY_DROPPED_METRIC,
    POET_DELIVERY_FAILED_METRIC, POET_DELIVERY_REJECTED_METRIC, POET_DELIVERY_UNKNOWN_METRIC,
    POET_DERIVATION_LAG_METRIC, POET_EFFECTIVE_HORIZON_METRIC, POET_EFFECTIVE_RESOLUTION_METRIC,
    POET_HEALTH_AGE_METRIC, POET_MANAGED_MEMORY_METRIC, POET_PEAK_RSS_METRIC,
    POET_QUEUE_BYTES_METRIC, POET_REQUESTED_HORIZON_METRIC, POET_REQUESTED_RESOLUTION_METRIC,
    POET_SEGMENT_COUNT_METRIC, POET_STORED_BYTES_METRIC, POET_UPTIME_SECONDS_METRIC,
    PROCESS_CPU_USAGE_METRIC, PROCESS_DISK_READ_BYTES_METRIC, PROCESS_DISK_WRITE_BYTES_METRIC,
    PROCESS_MEMORY_BYTES_METRIC, THERMAL_TEMPERATURE_METRIC,
};

const DEFAULT_TOP_PROCESSES: usize = 40;
const DEFAULT_PROCESS_HOLD_SAMPLES: u32 = 30;

/// Collector endpoint, sampling cadence, and optional sampling features.
#[derive(Debug, Parser)]
#[command(about = "Send local macOS host, resource, and process metrics to Infinite Haiku Poet")]
struct Args {
    /// Every Poet of the cluster, comma-separated. The Muse sends to one and
    /// fails over to the next; the Poets replicate among themselves.
    #[arg(
        long,
        env = "IH_MUSE_POET_URL",
        default_value = "http://127.0.0.1:8000",
        value_delimiter = ','
    )]
    poet_url: Vec<String>,
    /// Tenant (organization) the Poets' intake token is scoped to.
    #[arg(long, env = "IH_MUSE_ORGANIZATION", default_value = "local")]
    organization: String,
    #[arg(long, env = "IH_MUSE_INTERVAL_SECONDS", default_value_t = 5)]
    interval_seconds: u64,
    #[arg(
        long,
        env = "IH_MUSE_TOP_PROCESSES",
        default_value_t = DEFAULT_TOP_PROCESSES
    )]
    top_processes: usize,
    /// Samples a process stays emitted after it leaves the top ranking, so
    /// its series does not flicker while it still exists.
    #[arg(
        long,
        env = "IH_MUSE_PROCESS_HOLD_SAMPLES",
        default_value_t = DEFAULT_PROCESS_HOLD_SAMPLES
    )]
    process_hold_samples: u32,
    #[arg(
        long,
        env = "IH_MUSE_PROCESS_NETWORK",
        default_value = "true",
        value_parser = clap::value_parser!(bool)
    )]
    process_network: bool,
    #[arg(long)]
    once: bool,
    #[arg(long, env = "IH_MUSE_SAMPLE_COUNT")]
    samples: Option<u32>,
    #[arg(long, env = "IH_MUSE_POET_TOKEN_PATH")]
    poet_token_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.once && args.samples.is_some() {
        anyhow::bail!("--once and --samples cannot be used together");
    }

    let poet_token = args
        .poet_token_path
        .as_deref()
        .map(read_private_token)
        .transpose()?
        .context("--poet-token-path is required: graph intake is authenticated")?;
    let client = GraphPoetClient::cluster(args.poet_url.clone(), poet_token.clone())?;

    let mut registry = ElementRegistry::new(&args.organization, hostname(), &metric_definitions());
    let static_elements = register_static_elements(&mut registry);
    let mut sampler = Sampler::new(ProcessSelector::new(
        args.top_processes,
        args.process_hold_samples,
    ));
    let sample_limit = args.samples.or_else(|| args.once.then_some(1));
    let mut samples_sent = 0;
    let mut pending = PendingSamples::<GraphIntakeRequest>::new(MAX_PENDING_SAMPLES);
    let telemetry_client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()?;
    let poet_token = Some(poet_token);

    println!(
        "macOS Muse sending to {} (failover in that order) every {}s; top process cap: {}",
        args.poet_url.join(", "),
        args.interval_seconds.max(1),
        args.top_processes
    );
    println!(
        "Hierarchy: host -> resource groups -> cores, memory areas, volumes, interfaces, battery, sensors, applications -> processes"
    );

    // Keep sampling cadence independent of collection and upload duration.
    let mut cadence = tokio::time::interval(Duration::from_secs(args.interval_seconds.max(1)));
    cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        cadence.tick().await;
        let snapshot_time = timestamp();
        let mut snapshot = sampler.collect(snapshot_time, args.process_network);
        snapshot.poet_health =
            collect_poet_health(&telemetry_client, client.preferred_endpoint(), poet_token.as_deref()).await;
        // A Poet restart or network error must not stop the Muse or lose the
        // sample: unsent samples stay queued and are retried in order, on any Poet.
        let published = publish_snapshot(&mut registry, &static_elements, &snapshot);
        let observed_at = u64::try_from(timestamp()).unwrap_or(0).saturating_mul(1_000);
        let request = registry.intake(published.timestamp, &published.payloads, observed_at);
        let dropped = pending.push(request);
        if dropped > 0 {
            eprintln!(
                "Poet unreachable for over {MAX_PENDING_SAMPLES} samples; dropped {dropped} oldest (total {})",
                pending.dropped_samples()
            );
        }
        if let Err(error) = send_pending(&client, &mut registry, &mut pending).await {
            eprintln!(
                "Poet send failed; {} sample(s) queued for retry: {error:#}",
                pending.len()
            );
        }

        samples_sent += 1;
        println!(
            "sample {} at {}: {} payloads, {} processes ({} emitted), {} disks, {} interfaces, {} sensors, battery {}",
            samples_sent,
            published.timestamp,
            published.payload_count,
            snapshot.processes.len(),
            snapshot.emitted_processes.len(),
            snapshot.disks.len(),
            snapshot.network_interfaces.len(),
            snapshot.thermal_sensors.len(),
            snapshot
                .battery
                .as_ref()
                .map(|battery| format!("{:.0}%", battery.snapshot.charge_percent))
                .unwrap_or_else(|| "unavailable".to_string())
        );

        if sample_limit.is_some_and(|limit| samples_sent >= limit) {
            send_pending(&client, &mut registry, &mut pending).await?;
            return Ok(());
        }
    }
}

/// Collector keys mapped to deterministic graph identities (no Poet round trip).
type ElementRegistry = GraphRegistry;

/// Poet IDs for the fixed host resource hierarchy.
struct StaticElements {
    cpu_group_id: u64,
    storage_group_id: u64,
    network_group_id: u64,
    power_group_id: u64,
    thermal_group_id: u64,
    applications_group_id: u64,
    total_cpu_id: u64,
    physical_memory_id: u64,
    swap_id: u64,
    load_average_id: u64,
}

fn register_static_elements(registry: &mut ElementRegistry) -> StaticElements {
    let host_id = registry
        .ensure(
            "host".to_string(),
            KIND_MACOS_HOST,
            hostname(),
            None,
            metadata([("scope", "host")]),
        );
    let cpu_group_id = ensure_group(registry, host_id, "CPU", "cpu");
    let memory_group_id = ensure_group(registry, host_id, "Memory", "memory");
    let storage_group_id = ensure_group(registry, host_id, "Storage", "storage");
    let network_group_id = ensure_group(registry, host_id, "Network", "network");
    let power_group_id = ensure_group(registry, host_id, "Power", "power");
    let thermal_group_id = ensure_group(registry, host_id, "Thermal", "thermal");
    let applications_group_id =
        ensure_group(registry, host_id, "Applications", "applications");
    let system_group_id = ensure_group(registry, host_id, "System", "system");

    let total_cpu_id = registry
        .ensure(
            "resource:cpu:total".to_string(),
            KIND_MACOS_RESOURCE,
            "Total CPU".to_string(),
            Some(cpu_group_id),
            metadata([("resource", "cpu"), ("level", "host")]),
        );
    let physical_memory_id = registry
        .ensure(
            "resource:memory:physical".to_string(),
            KIND_MACOS_RESOURCE,
            "Physical memory".to_string(),
            Some(memory_group_id),
            metadata([("resource", "memory"), ("level", "host")]),
        );
    let swap_id = registry
        .ensure(
            "resource:memory:swap".to_string(),
            KIND_MACOS_RESOURCE,
            "Swap".to_string(),
            Some(memory_group_id),
            metadata([("resource", "swap"), ("level", "host")]),
        );
    let load_average_id = registry
        .ensure(
            "resource:system:load".to_string(),
            KIND_MACOS_RESOURCE,
            "Load average".to_string(),
            Some(system_group_id),
            metadata([("resource", "load"), ("level", "host")]),
        );

    StaticElements {
        cpu_group_id,
        storage_group_id,
        network_group_id,
        power_group_id,
        thermal_group_id,
        applications_group_id,
        total_cpu_id,
        physical_memory_id,
        swap_id,
        load_average_id,
    }
}

fn ensure_group(registry: &mut ElementRegistry, host_id: u64, name: &str, key: &str) -> u64 {
    registry
        .ensure(
            format!("group:{key}"),
            KIND_MACOS_RESOURCE_GROUP,
            name.to_string(),
            Some(host_id),
            metadata([("resource_group", key)]),
        )
}

/// Retains OS sampling state needed to compute deltas between observations,
/// plus the process selector whose hold state spans samples.
struct Sampler {
    process_selector: ProcessSelector,
    system: System,
    disks: Disks,
    networks: Networks,
    components: Components,
    previous_battery: Option<(i64, BatterySnapshot)>,
    process_network_available: bool,
}

impl Sampler {
    fn new(process_selector: ProcessSelector) -> Self {
        let mut system = System::new_all();
        system.refresh_cpu_all();
        system.refresh_memory();
        system.refresh_processes(ProcessesToUpdate::All, true);

        let mut disks = Disks::new_with_refreshed_list();
        disks.refresh();
        let mut networks = Networks::new_with_refreshed_list();
        networks.refresh();
        let mut components = Components::new_with_refreshed_list();
        components.refresh();

        Self {
            process_selector,
            system,
            disks,
            networks,
            components,
            previous_battery: None,
            process_network_available: true,
        }
    }

    fn collect(
        &mut self,
        now: i64,
        process_network_enabled: bool,
    ) -> CollectedSnapshot {
        self.system.refresh_cpu_all();
        self.system.refresh_memory();
        self.system.refresh_processes(ProcessesToUpdate::All, true);
        self.disks.refresh();
        self.networks.refresh();
        self.components.refresh();

        let disks = collect_disks(&self.disks);
        let total_disk = disks.iter().map(|disk| disk.total_bytes).sum();
        let available_disk = disks.iter().map(|disk| disk.available_bytes).sum();
        let network_interfaces = collect_network_interfaces(&self.networks);
        let thermal_sensors = collect_thermal_sensors(&self.components);
        let battery = self.battery_reading(now);
        let process_network = self.collect_process_network(process_network_enabled);
        let processes = collect_processes(&self.system, &process_network);
        let emitted_processes = self.process_selector.select(&processes);

        CollectedSnapshot {
            poet_health: None,
            host: snapshot_from_values(
                self.system.global_cpu_usage(),
                self.system.used_memory(),
                self.system.total_memory(),
                System::load_average().one,
                available_disk,
                total_disk,
            ),
            cpu_cores: self
                .system
                .cpus()
                .iter()
                .enumerate()
                .map(|(index, cpu)| CpuCoreSample {
                    index,
                    name: if cpu.name().is_empty() {
                        format!("CPU core {index}")
                    } else {
                        format!("{} {index}", cpu.name())
                    },
                    usage_percent: cpu.cpu_usage().clamp(0.0, 100.0),
                })
                .collect(),
            disks,
            network_interfaces,
            battery,
            thermal_sensors,
            processes,
            emitted_processes,
        }
    }

    fn battery_reading(&mut self, now: i64) -> Option<BatteryReading> {
        let snapshot = read_battery_snapshot()?;
        let drain_percent_per_hour = match &self.previous_battery {
            Some((previous_time, previous_snapshot))
                if snapshot.on_battery && previous_snapshot.on_battery && now > *previous_time =>
            {
                let elapsed_hours = (now - *previous_time) as f32 / 3_600_000_000.0;
                let drained_percent =
                    (previous_snapshot.charge_percent - snapshot.charge_percent).max(0.0);
                Some(drained_percent / elapsed_hours.max(f32::EPSILON))
            }
            Some(_) => Some(0.0),
            None => None,
        };
        self.previous_battery = Some((now, snapshot.clone()));
        Some(BatteryReading {
            snapshot,
            drain_percent_per_hour,
        })
    }

    fn collect_process_network(&mut self, enabled: bool) -> HashMap<String, ProcessNetworkSample> {
        if !enabled || !self.process_network_available {
            return HashMap::new();
        }
        match read_nettop_process_network() {
            Ok(samples) => samples,
            Err(error) => {
                eprintln!("process network sampling unavailable for this run: {error}");
                self.process_network_available = false;
                HashMap::new()
            }
        }
    }
}

/// One collection pass before dynamic element registration and publication.
/// `processes` holds every sampled process (application totals are summed
/// over all of them); `emitted_processes` holds the keys published as leaves.
struct CollectedSnapshot {
    poet_health: Option<PoetHealthSample>,
    host: ih_muse_macos::HostSnapshot,
    cpu_cores: Vec<CpuCoreSample>,
    disks: Vec<DiskSample>,
    network_interfaces: Vec<NetworkInterfaceSample>,
    battery: Option<BatteryReading>,
    thermal_sensors: Vec<ThermalSensorSample>,
    processes: Vec<ProcessSample>,
    emitted_processes: HashSet<String>,
}

/// Bounded, authenticated self-health returned by the observed Poet process.
#[derive(serde::Deserialize)]
struct PoetHealthSample {
    observed_unix_nano: u64,
    process: PoetProcessHealth,
    native_storage: Option<PoetNativeStorageHealth>,
}

#[derive(serde::Deserialize)]
struct PoetProcessHealth {
    pid: u32,
    cpu_seconds: Option<f64>,
    uptime_seconds: f64,
    rss_bytes: Option<u64>,
    peak_rss_bytes: Option<u64>,
}

#[derive(serde::Deserialize)]
struct PoetNativeStorageHealth {
    managed_memory_bytes: u64,
    queue_bytes: u64,
    segment_count: u64,
    coverage_fraction: f64,
    requested_resolution_unix_nano: Option<u64>,
    effective_resolution_unix_nano: Option<u64>,
    requested_horizon_unix_nano: Option<u64>,
    effective_horizon_unix_nano: Option<u64>,
    derivation_lag_unix_nano: u64,
    roots: Vec<PoetStorageRootHealth>,
    outcomes: PoetDeliveryOutcomes,
}

#[derive(serde::Deserialize)]
struct PoetStorageRootHealth {
    stored_bytes: u64,
    available: bool,
}

#[derive(serde::Deserialize)]
struct PoetDeliveryOutcomes {
    accepted: u64,
    rejected: u64,
    dropped: u64,
    failed: u64,
    unknown: u64,
}

impl PoetHealthSample {
    fn readings_for(&self, pid: u32, collected_unix_micro: i64) -> Vec<Option<MetricReading>> {
        if pid != self.process.pid {
            return Vec::new();
        }
        let age_seconds = (u64::try_from(collected_unix_micro)
            .unwrap_or_default()
            .saturating_mul(1_000)
            .saturating_sub(self.observed_unix_nano) as f64
            / 1_000_000_000.0) as f32;
        let mut readings = vec![
            self.process
                .cpu_seconds
                .and_then(|value| MetricReading::new(POET_CPU_SECONDS_METRIC, value)),
            MetricReading::new(
                POET_UPTIME_SECONDS_METRIC,
                self.process.uptime_seconds,
            ),
            self.process
                .rss_bytes
                .and_then(|value| MetricReading::new(POET_CURRENT_RSS_METRIC, value as f64)),
            self.process
                .peak_rss_bytes
                .and_then(|value| MetricReading::new(POET_PEAK_RSS_METRIC, value as f64)),
            MetricReading::new(POET_HEALTH_AGE_METRIC, age_seconds),
        ];
        if let Some(storage) = &self.native_storage {
            let stored_bytes = storage
                .roots
                .iter()
                .filter(|root| root.available)
                .map(|root| root.stored_bytes)
                .sum::<u64>();
            readings.extend([
                MetricReading::new(
                    POET_MANAGED_MEMORY_METRIC,
                    storage.managed_memory_bytes as f64,
                ),
                MetricReading::new(POET_QUEUE_BYTES_METRIC, storage.queue_bytes as f64),
                MetricReading::new(POET_STORED_BYTES_METRIC, stored_bytes as f64),
                MetricReading::new(POET_SEGMENT_COUNT_METRIC, storage.segment_count as f64),
                MetricReading::new(POET_COVERAGE_METRIC, storage.coverage_fraction),
                storage.requested_resolution_unix_nano.and_then(|value| {
                    MetricReading::new(POET_REQUESTED_RESOLUTION_METRIC, value as f64)
                }),
                storage.effective_resolution_unix_nano.and_then(|value| {
                    MetricReading::new(POET_EFFECTIVE_RESOLUTION_METRIC, value as f64)
                }),
                storage.requested_horizon_unix_nano.and_then(|value| {
                    MetricReading::new(POET_REQUESTED_HORIZON_METRIC, value as f64)
                }),
                storage.effective_horizon_unix_nano.and_then(|value| {
                    MetricReading::new(POET_EFFECTIVE_HORIZON_METRIC, value as f64)
                }),
                MetricReading::new(
                    POET_DERIVATION_LAG_METRIC,
                    storage.derivation_lag_unix_nano as f64,
                ),
                MetricReading::new(
                    POET_DELIVERY_ACCEPTED_METRIC,
                    storage.outcomes.accepted as f64,
                ),
                MetricReading::new(
                    POET_DELIVERY_REJECTED_METRIC,
                    storage.outcomes.rejected as f64,
                ),
                MetricReading::new(
                    POET_DELIVERY_DROPPED_METRIC,
                    storage.outcomes.dropped as f64,
                ),
                MetricReading::new(POET_DELIVERY_FAILED_METRIC, storage.outcomes.failed as f64),
                MetricReading::new(
                    POET_DELIVERY_UNKNOWN_METRIC,
                    storage.outcomes.unknown as f64,
                ),
            ]);
        }
        readings
    }
}

#[test]
fn native_health_is_scoped_to_exact_process_with_honest_optional_values() {
    let sample = PoetHealthSample {
        observed_unix_nano: 10_000_000_000,
        process: PoetProcessHealth {
            pid: 42,
            cpu_seconds: Some(2.5),
            uptime_seconds: 9.0,
            rss_bytes: Some(8192),
            peak_rss_bytes: Some(16384),
        },
        native_storage: Some(PoetNativeStorageHealth {
            managed_memory_bytes: 1024,
            queue_bytes: 512,
            segment_count: 3,
            coverage_fraction: 0.75,
            requested_resolution_unix_nano: Some(1_000_000),
            effective_resolution_unix_nano: None,
            requested_horizon_unix_nano: Some(60_000_000_000),
            effective_horizon_unix_nano: None,
            derivation_lag_unix_nano: 7,
            roots: vec![PoetStorageRootHealth {
                stored_bytes: 2048,
                available: true,
            }],
            outcomes: PoetDeliveryOutcomes {
                accepted: 4,
                rejected: 1,
                dropped: 0,
                failed: 0,
                unknown: 0,
            },
        }),
    };
    assert!(sample.readings_for(43, 10_000_000).is_empty());
    let readings = sample.readings_for(42, 10_000_000);
    assert!(readings
        .iter()
        .flatten()
        .any(|reading| reading.code == POET_CURRENT_RSS_METRIC && reading.value == 8192.0));
    assert!(readings
        .iter()
        .flatten()
        .any(|reading| reading.code == POET_COVERAGE_METRIC && reading.value == 0.75));
    assert!(!readings
        .iter()
        .flatten()
        .any(|reading| reading.code == POET_EFFECTIVE_RESOLUTION_METRIC));
}

fn read_private_token(path: &Path) -> Result<String> {
    let token = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read Poet credential from {}", path.display()))?;
    let token = token.trim().to_owned();
    anyhow::ensure!(
        !token.is_empty() && token.len() <= 4096,
        "Poet credential is empty or too large"
    );
    Ok(token)
}

async fn collect_poet_health(
    client: &reqwest::Client,
    poet_url: &str,
    token: Option<&str>,
) -> Option<PoetHealthSample> {
    let token = token?;
    client
        .get(format!(
            "{}/api/v1/telemetry/stats",
            poet_url.trim_end_matches('/')
        ))
        .bearer_auth(token)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()
}

/// Utilization of one logical CPU core in percent.
struct CpuCoreSample {
    index: usize,
    name: String,
    usage_percent: f32,
}

/// Capacity and occupied space for one mounted volume.
struct DiskSample {
    key: String,
    name: String,
    usage_percent: f32,
    used_bytes: u64,
    available_bytes: u64,
    total_bytes: u64,
}

/// Per-interface byte deltas and lifetime counters.
struct NetworkInterfaceSample {
    key: String,
    name: String,
    received_bytes: u64,
    transmitted_bytes: u64,
    total_received_bytes: u64,
    total_transmitted_bytes: u64,
}

/// Battery state plus an optional observed discharge rate.
struct BatteryReading {
    snapshot: BatterySnapshot,
    drain_percent_per_hour: Option<f32>,
}

/// Named thermal sensor observation in degrees Celsius.
struct ThermalSensorSample {
    key: String,
    name: String,
    temperature_celsius: f32,
}

#[derive(Clone)]
/// Process identity and resource observations, including optional network bytes.
struct ProcessSample {
    key: String,
    app_key: String,
    app_name: String,
    name: String,
    pid: u32,
    parent_pid: Option<u32>,
    start_time: u64,
    cpu_usage_percent: f32,
    memory_bytes: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
    network_received_bytes: Option<u64>,
    network_transmitted_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Network byte observations attributed to a process by nettop.
struct ProcessNetworkSample {
    received_bytes: u64,
    transmitted_bytes: u64,
}

/// Timestamp and payload count of a successfully published collection.
struct PublishedSample {
    timestamp: i64,
    payload_count: usize,
    payloads: Vec<MetricPayload>,
}

/// Samples kept while Poet is unreachable: 10 minutes at the default 5 s cadence.
const MAX_PENDING_SAMPLES: usize = 120;

/// Dynamic Poet element IDs aligned with a collected snapshot's resource lists.
struct RegisteredSnapshotElements {
    cpu_core_ids: Vec<u64>,
    disk_ids: Vec<u64>,
    network_interface_ids: Vec<u64>,
    battery_id: Option<u64>,
    thermal_sensor_ids: Vec<u64>,
    applications: Vec<RegisteredApplication>,
}

/// Poet IDs for one application, its emitted process leaves (aligned with
/// [`ApplicationPlan::emitted`]) and its optional "other processes" child.
struct RegisteredApplication {
    application_id: u64,
    process_ids: Vec<u64>,
    other_id: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
/// Additive resource observations for a set of processes (an application,
/// or the un-emitted remainder of one). Network sums are present only when
/// at least one summed process had a network observation.
struct ApplicationTotals {
    cpu_usage_percent: f32,
    memory_bytes: u64,
    disk_read_bytes: u64,
    disk_write_bytes: u64,
    network_received_bytes: u64,
    network_transmitted_bytes: u64,
    has_network_received: bool,
    has_network_transmitted: bool,
}

impl ApplicationTotals {
    fn add(&mut self, process: &ProcessSample) {
        self.cpu_usage_percent += process.cpu_usage_percent;
        self.memory_bytes = self.memory_bytes.saturating_add(process.memory_bytes);
        self.disk_read_bytes = self.disk_read_bytes.saturating_add(process.disk_read_bytes);
        self.disk_write_bytes = self
            .disk_write_bytes
            .saturating_add(process.disk_write_bytes);
        if let Some(value) = process.network_received_bytes {
            self.network_received_bytes = self.network_received_bytes.saturating_add(value);
            self.has_network_received = true;
        }
        if let Some(value) = process.network_transmitted_bytes {
            self.network_transmitted_bytes = self.network_transmitted_bytes.saturating_add(value);
            self.has_network_transmitted = true;
        }
    }

    /// The totals of a single process.
    fn of(process: &ProcessSample) -> Self {
        let mut totals = Self::default();
        totals.add(process);
        totals
    }

    /// Process metric readings for these totals; absent network stays absent.
    fn readings(&self, logical_cpus: usize) -> [Option<MetricReading>; 7] {
        [
            MetricReading::new(PROCESS_CPU_USAGE_METRIC, self.cpu_usage_percent),
            ih_muse_macos::cpu_capacity_percent(self.cpu_usage_percent, logical_cpus)
                .and_then(|value| MetricReading::new(ih_muse_macos::PROCESS_CPU_CAPACITY_METRIC, value)),
            MetricReading::new(PROCESS_MEMORY_BYTES_METRIC, self.memory_bytes as f64),
            MetricReading::new(PROCESS_DISK_READ_BYTES_METRIC, self.disk_read_bytes as f64),
            MetricReading::new(PROCESS_DISK_WRITE_BYTES_METRIC, self.disk_write_bytes as f64),
            self.has_network_received
                .then(|| {
                    MetricReading::new(
                        ih_muse_macos::PROCESS_NETWORK_RECEIVED_BYTES_METRIC,
                        self.network_received_bytes as f64,
                    )
                })
                .flatten(),
            self.has_network_transmitted
                .then(|| {
                    MetricReading::new(
                        ih_muse_macos::PROCESS_NETWORK_TRANSMITTED_BYTES_METRIC,
                        self.network_transmitted_bytes as f64,
                    )
                })
                .flatten(),
        ]
    }
}

/// One application for one sample: totals over ALL of its sampled processes,
/// the processes emitted as leaves (in rank order), and the remainder of the
/// un-emitted ones, so the children always sum to the application total.
struct ApplicationPlan<'a> {
    app_key: &'a str,
    app_name: &'a str,
    totals: ApplicationTotals,
    emitted: Vec<&'a ProcessSample>,
    other: Option<OtherProcesses>,
}

/// Resource sums of an application's processes that are not emitted as leaves.
#[derive(Debug, PartialEq)]
struct OtherProcesses {
    count: usize,
    totals: ApplicationTotals,
}

/// Stable graph key of an application's "other processes" child.
fn other_processes_key(app_key: &str) -> String {
    format!("{app_key}:other")
}

/// Groups every sampled process by application (ordered by key). An
/// application exists in the plan whenever any of its processes exists.
fn plan_applications<'a>(
    processes: &'a [ProcessSample],
    emitted: &HashSet<String>,
) -> Vec<ApplicationPlan<'a>> {
    let mut plans = BTreeMap::<&str, ApplicationPlan<'a>>::new();
    for process in processes {
        let plan = plans
            .entry(process.app_key.as_str())
            .or_insert_with(|| ApplicationPlan {
                app_key: &process.app_key,
                app_name: &process.app_name,
                totals: ApplicationTotals::default(),
                emitted: Vec::new(),
                other: None,
            });
        plan.totals.add(process);
        if emitted.contains(&process.key) {
            plan.emitted.push(process);
        } else {
            let other = plan.other.get_or_insert_with(|| OtherProcesses {
                count: 0,
                totals: ApplicationTotals::default(),
            });
            other.count += 1;
            other.totals.add(process);
        }
    }
    plans.into_values().collect()
}

/// Chooses which processes are emitted as individual leaves: the top `top`
/// by [`process_score`], plus any process that was in the top within the last
/// `hold_samples` samples and still exists (bounded to `2 × top` overall),
/// plus the always-observed `poet` and `ih-muse-macos`. State is keyed by the
/// process key (pid + start time), so a reused pid is a new process.
struct ProcessSelector {
    top: usize,
    hold_samples: u32,
    /// Remaining hold samples per process key, refreshed while in the top.
    held: HashMap<String, u32>,
}

impl ProcessSelector {
    fn new(top: usize, hold_samples: u32) -> Self {
        Self {
            top: top.max(1),
            hold_samples,
            held: HashMap::new(),
        }
    }

    /// Selects the emitted process keys for this sample and advances holds.
    fn select(&mut self, processes: &[ProcessSample]) -> HashSet<String> {
        let mut ranked = processes.iter().collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            process_score(right)
                .partial_cmp(&process_score(left))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.pid.cmp(&right.pid))
        });
        let bound = self.top.saturating_mul(2);
        let mut emitted = HashSet::new();
        let mut held = HashMap::new();
        for (rank, process) in ranked.iter().enumerate() {
            if rank < self.top {
                emitted.insert(process.key.clone());
                if self.hold_samples > 0 {
                    held.insert(process.key.clone(), self.hold_samples);
                }
            } else if let Some(remaining) = self.held.get(&process.key).copied() {
                if remaining > 0 && emitted.len() < bound {
                    emitted.insert(process.key.clone());
                    if remaining > 1 {
                        held.insert(process.key.clone(), remaining - 1);
                    }
                }
            }
            if is_self_observed(&process.name) {
                emitted.insert(process.key.clone());
            }
        }
        self.held = held;
        emitted
    }
}

fn is_self_observed(name: &str) -> bool {
    matches!(name, "poet" | "ih-muse-macos")
}

#[test]
fn application_totals_preserve_sampled_process_resource_sums() {
    let first = ProcessSample {
        key: "process:1:1".into(),
        app_key: "app:demo".into(),
        app_name: "Demo".into(),
        name: "demo".into(),
        pid: 1,
        parent_pid: None,
        start_time: 1,
        cpu_usage_percent: 12.5,
        memory_bytes: 10,
        disk_read_bytes: 2,
        disk_write_bytes: 3,
        network_received_bytes: Some(4),
        network_transmitted_bytes: None,
    };
    let second = ProcessSample {
        pid: 2,
        cpu_usage_percent: 7.5,
        memory_bytes: 20,
        disk_read_bytes: 5,
        disk_write_bytes: 7,
        network_received_bytes: None,
        network_transmitted_bytes: Some(11),
        ..first.clone()
    };
    let mut total = ApplicationTotals::default();
    total.add(&first);
    total.add(&second);
    assert_eq!(total.cpu_usage_percent, 20.0);
    assert_eq!(total.memory_bytes, 30);
    assert_eq!((total.disk_read_bytes, total.disk_write_bytes), (7, 10));
    assert_eq!(
        (
            total.network_received_bytes,
            total.network_transmitted_bytes
        ),
        (4, 11)
    );
    assert!(total.has_network_received && total.has_network_transmitted);
}

fn publish_snapshot(
    registry: &mut ElementRegistry,
    static_elements: &StaticElements,
    snapshot: &CollectedSnapshot,
) -> PublishedSample {
    let applications = plan_applications(&snapshot.processes, &snapshot.emitted_processes);
    let registered = ensure_snapshot_elements(registry, static_elements, snapshot, &applications);
    let now = timestamp();
    let mut payloads = Vec::new();
    if let Some(payload) = snapshot.host.cpu_metrics(now, static_elements.total_cpu_id) {
        payloads.push(payload);
    }
    if let Some(payload) = snapshot
        .host
        .memory_metrics(now, static_elements.physical_memory_id)
    {
        payloads.push(payload);
    }
    if let Some(payload) = snapshot.host.swap_metrics(now, static_elements.swap_id) {
        payloads.push(payload);
    }
    if let Some(payload) = snapshot
        .host
        .load_metrics(now, static_elements.load_average_id)
    {
        payloads.push(payload);
    }

    for (core, element_id) in snapshot.cpu_cores.iter().zip(registered.cpu_core_ids) {
        push_payload(
            &mut payloads,
            now,
            element_id,
            [MetricReading::new(
                CPU_CORE_USAGE_METRIC,
                core.usage_percent,
            )],
        );
    }

    for (disk, element_id) in snapshot.disks.iter().zip(registered.disk_ids) {
        push_payload(
            &mut payloads,
            now,
            element_id,
            [
                MetricReading::new(DISK_USAGE_METRIC, disk.usage_percent),
                MetricReading::new(DISK_USED_BYTES_METRIC, disk.used_bytes as f64),
                MetricReading::new(DISK_AVAILABLE_BYTES_METRIC, disk.available_bytes as f64),
            ],
        );
    }

    for (interface, element_id) in snapshot
        .network_interfaces
        .iter()
        .zip(registered.network_interface_ids)
    {
        push_payload(
            &mut payloads,
            now,
            element_id,
            [
                MetricReading::new(
                    NETWORK_RECEIVED_BYTES_METRIC,
                    interface.received_bytes as f64,
                ),
                MetricReading::new(
                    NETWORK_TRANSMITTED_BYTES_METRIC,
                    interface.transmitted_bytes as f64,
                ),
                MetricReading::new(
                    NETWORK_TOTAL_RECEIVED_BYTES_METRIC,
                    interface.total_received_bytes as f64,
                ),
                MetricReading::new(
                    NETWORK_TOTAL_TRANSMITTED_BYTES_METRIC,
                    interface.total_transmitted_bytes as f64,
                ),
            ],
        );
    }

    if let (Some(battery), Some(element_id)) = (&snapshot.battery, registered.battery_id) {
        push_payload(
            &mut payloads,
            now,
            element_id,
            [
                MetricReading::new(BATTERY_CHARGE_METRIC, battery.snapshot.charge_percent),
                MetricReading::new(
                    BATTERY_ON_BATTERY_METRIC,
                    bool_metric(battery.snapshot.on_battery),
                ),
                MetricReading::new(
                    BATTERY_CHARGING_METRIC,
                    bool_metric(battery.snapshot.charging),
                ),
                battery
                    .drain_percent_per_hour
                    .and_then(|value| MetricReading::new(BATTERY_DRAIN_RATE_METRIC, value)),
            ],
        );
    }

    for (sensor, element_id) in snapshot
        .thermal_sensors
        .iter()
        .zip(registered.thermal_sensor_ids)
    {
        push_payload(
            &mut payloads,
            now,
            element_id,
            [MetricReading::new(
                THERMAL_TEMPERATURE_METRIC,
                sensor.temperature_celsius,
            )],
        );
    }

    let logical_cpus = snapshot.cpu_cores.len();
    for (application, registered_application) in applications.iter().zip(&registered.applications) {
        for (process, process_id) in application
            .emitted
            .iter()
            .zip(&registered_application.process_ids)
        {
            push_payload(
                &mut payloads,
                now,
                *process_id,
                ApplicationTotals::of(process)
                    .readings(logical_cpus)
                    .into_iter()
                    .chain(
                        snapshot
                            .poet_health
                            .iter()
                            .flat_map(|health| health.readings_for(process.pid, now)),
                    ),
            );
        }
        if let (Some(other), Some(other_id)) = (&application.other, registered_application.other_id) {
            push_payload(&mut payloads, now, other_id, other.totals.readings(logical_cpus));
        }
        push_payload(
            &mut payloads,
            now,
            registered_application.application_id,
            application.totals.readings(logical_cpus),
        );
    }

    PublishedSample {
        timestamp: now,
        payload_count: payloads.len(),
        payloads,
    }
}

/// Sends queued samples oldest first and stops at the first failure, which
/// stays queued for the next tick. Returns how many samples were sent.
async fn send_pending(
    client: &GraphPoetClient,
    registry: &mut ElementRegistry,
    pending: &mut PendingSamples<GraphIntakeRequest>,
) -> Result<usize> {
    let mut sent = 0;
    while let Some(request) = pending.oldest() {
        client.publish(request).await?;
        registry.acknowledge(request);
        pending.acknowledge_oldest();
        sent += 1;
    }
    Ok(sent)
}

fn ensure_snapshot_elements(
    registry: &mut ElementRegistry,
    static_elements: &StaticElements,
    snapshot: &CollectedSnapshot,
    applications: &[ApplicationPlan<'_>],
) -> RegisteredSnapshotElements {
    let mut cpu_core_ids = Vec::with_capacity(snapshot.cpu_cores.len());
    for core in &snapshot.cpu_cores {
        cpu_core_ids.push(
            registry
                .ensure(
                    format!("cpu-core:{}", core.index),
                    KIND_MACOS_CPU_CORE,
                    core.name.clone(),
                    Some(static_elements.cpu_group_id),
                    metadata([
                        ("resource", "cpu"),
                        ("level", "core"),
                        ("core_index", &core.index.to_string()),
                    ]),
                ),
        );
    }

    let mut disk_ids = Vec::with_capacity(snapshot.disks.len());
    for disk in &snapshot.disks {
        disk_ids.push(
            registry
                .ensure(
                    disk.key.clone(),
                    KIND_MACOS_DISK_VOLUME,
                    disk.name.clone(),
                    Some(static_elements.storage_group_id),
                    metadata([("resource", "disk"), ("level", "volume")]),
                ),
        );
    }

    let mut network_interface_ids = Vec::with_capacity(snapshot.network_interfaces.len());
    for interface in &snapshot.network_interfaces {
        network_interface_ids.push(
            registry
                .ensure(
                    interface.key.clone(),
                    KIND_MACOS_NETWORK_INTERFACE,
                    interface.name.clone(),
                    Some(static_elements.network_group_id),
                    metadata([("resource", "network"), ("level", "interface")]),
                ),
        );
    }

    let battery_id = if snapshot.battery.is_some() {
        Some(
            registry
                .ensure(
                    "power:battery:internal".to_string(),
                    KIND_MACOS_POWER_SOURCE,
                    "Internal battery".to_string(),
                    Some(static_elements.power_group_id),
                    metadata([("resource", "battery"), ("level", "power_source")]),
                ),
        )
    } else {
        None
    };

    let mut thermal_sensor_ids = Vec::with_capacity(snapshot.thermal_sensors.len());
    for sensor in &snapshot.thermal_sensors {
        thermal_sensor_ids.push(
            registry
                .ensure(
                    sensor.key.clone(),
                    KIND_MACOS_THERMAL_SENSOR,
                    sensor.name.clone(),
                    Some(static_elements.thermal_group_id),
                    metadata([("resource", "thermal"), ("level", "sensor")]),
                ),
        );
    }

    let mut registered_applications = Vec::with_capacity(applications.len());
    for application in applications {
        let application_id = registry.ensure(
            application.app_key.to_string(),
            KIND_MACOS_APPLICATION,
            application.app_name.to_string(),
            Some(static_elements.applications_group_id),
            metadata([("resource", "process"), ("level", "application")]),
        );
        let process_ids = application
            .emitted
            .iter()
            .map(|process| {
                registry.ensure(
                    process.key.clone(),
                    KIND_MACOS_PROCESS,
                    format!("{} ({})", process.name, process.pid),
                    Some(application_id),
                    metadata([
                        ("resource", "process"),
                        ("level", "process"),
                        ("pid", &process.pid.to_string()),
                        ("parent_pid", &optional_u32(process.parent_pid)),
                        ("start_time", &process.start_time.to_string()),
                    ]),
                )
            })
            .collect();
        // No pid/start_time metadata: the graph identity falls back to the
        // stable per-application key rather than a process identity.
        let other_id = application.other.as_ref().map(|_| {
            registry.ensure(
                other_processes_key(application.app_key),
                KIND_MACOS_PROCESS,
                "Other processes".to_string(),
                Some(application_id),
                metadata([
                    ("resource", "process"),
                    ("level", "process_group"),
                    ("process_group", "other"),
                ]),
            )
        });
        registered_applications.push(RegisteredApplication {
            application_id,
            process_ids,
            other_id,
        });
    }

    RegisteredSnapshotElements {
        cpu_core_ids,
        disk_ids,
        network_interface_ids,
        battery_id,
        thermal_sensor_ids,
        applications: registered_applications,
    }
}

fn push_payload(
    payloads: &mut Vec<MetricPayload>,
    timestamp: i64,
    element_id: u64,
    readings: impl IntoIterator<Item = Option<MetricReading>>,
) {
    let readings = readings.into_iter().flatten().collect::<Vec<_>>();
    if let Some(payload) = metric_payload(timestamp, element_id, &readings) {
        payloads.push(payload);
    }
}

fn collect_disks(disks: &Disks) -> Vec<DiskSample> {
    disks
        .list()
        .iter()
        .map(|disk| {
            let total_bytes = disk.total_space();
            let available_bytes = disk.available_space();
            let used_bytes = total_bytes.saturating_sub(available_bytes);
            let mount = disk.mount_point().display().to_string();
            let name = os_str_to_display(disk.name()).unwrap_or_else(|| mount.clone());
            DiskSample {
                key: format!("disk:{mount}"),
                name: format!("{name} {mount}"),
                usage_percent: percentage(used_bytes, total_bytes),
                used_bytes,
                available_bytes,
                total_bytes,
            }
        })
        .collect()
}

fn collect_network_interfaces(networks: &Networks) -> Vec<NetworkInterfaceSample> {
    let mut interfaces = networks
        .iter()
        .map(|(name, data)| NetworkInterfaceSample {
            key: format!("network:{name}"),
            name: name.clone(),
            received_bytes: data.received(),
            transmitted_bytes: data.transmitted(),
            total_received_bytes: data.total_received(),
            total_transmitted_bytes: data.total_transmitted(),
        })
        .collect::<Vec<_>>();
    interfaces.sort_by(|left, right| left.name.cmp(&right.name));
    interfaces
}

fn collect_thermal_sensors(components: &Components) -> Vec<ThermalSensorSample> {
    components
        .iter()
        .filter_map(|component| {
            let temperature = component.temperature();
            (temperature.is_finite() && temperature > 0.0).then(|| {
                let name = component.label().to_string();
                ThermalSensorSample {
                    key: format!("thermal:{name}"),
                    name,
                    temperature_celsius: temperature,
                }
            })
        })
        .collect()
}

/// Samples every process; each is attributed to its application through
/// [`resolve_application`] over the full process table.
fn collect_processes(
    system: &System,
    process_network: &HashMap<String, ProcessNetworkSample>,
) -> Vec<ProcessSample> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let table = system
        .processes()
        .iter()
        .map(|(pid, process)| {
            let name = os_str_to_display(process.name())
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| format!("process-{}", pid.as_u32()));
            (
                pid.as_u32(),
                ProcessIdentity::new(
                    name,
                    process.parent().map(|parent| parent.as_u32()),
                    process.exe(),
                    home.as_deref(),
                ),
            )
        })
        .collect::<HashMap<_, _>>();
    system
        .processes()
        .iter()
        .map(|(pid, process)| {
            let name = table[&pid.as_u32()].name.clone();
            let ApplicationRef {
                key: app_key,
                name: app_name,
            } = resolve_application(pid.as_u32(), &table);
            let disk_usage = process.disk_usage();
            let network = process_network.get(&name).copied();
            ProcessSample {
                key: format!("process:{}:{}", pid.as_u32(), process.start_time()),
                app_key,
                app_name,
                name,
                pid: pid.as_u32(),
                parent_pid: process.parent().map(|parent| parent.as_u32()),
                start_time: process.start_time(),
                cpu_usage_percent: process.cpu_usage().max(0.0),
                memory_bytes: process.memory(),
                disk_read_bytes: disk_usage.read_bytes,
                disk_write_bytes: disk_usage.written_bytes,
                network_received_bytes: network.map(|sample| sample.received_bytes),
                network_transmitted_bytes: network.map(|sample| sample.transmitted_bytes),
            }
        })
        .collect()
}

fn read_nettop_process_network() -> Result<HashMap<String, ProcessNetworkSample>> {
    let output = Command::new("nettop")
        .args(["-P", "-d", "-L", "1", "-x", "-n"])
        .output()
        .context("failed to execute nettop")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "nettop exited with {}; {}",
            output.status,
            stderr.lines().next().unwrap_or("no stderr")
        );
    }
    parse_nettop_process_network(&String::from_utf8_lossy(&output.stdout))
        .context("nettop output did not contain process network columns")
}

fn parse_nettop_process_network(output: &str) -> Option<HashMap<String, ProcessNetworkSample>> {
    let mut lines = output.lines();
    let header = lines.find(|line| {
        let lower = line.to_ascii_lowercase();
        lower.contains("process") && lower.contains("bytes_in") && lower.contains("bytes_out")
    })?;
    let headers = split_csv_line(header)
        .into_iter()
        .map(|part| part.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    let process_index = headers.iter().position(|header| header == "process")?;
    let bytes_in_index = headers.iter().position(|header| header == "bytes_in")?;
    let bytes_out_index = headers.iter().position(|header| header == "bytes_out")?;
    let max_index = process_index.max(bytes_in_index).max(bytes_out_index);
    let mut samples = HashMap::new();

    for line in lines {
        let columns = split_csv_line(line);
        if columns.len() <= max_index {
            continue;
        }
        let process_name = columns[process_index].trim();
        if process_name.is_empty() {
            continue;
        }
        let received_bytes = parse_u64_cell(&columns[bytes_in_index]).unwrap_or(0);
        let transmitted_bytes = parse_u64_cell(&columns[bytes_out_index]).unwrap_or(0);
        if received_bytes == 0 && transmitted_bytes == 0 {
            continue;
        }
        let entry = samples
            .entry(process_name.to_string())
            .or_insert_with(ProcessNetworkSample::default);
        entry.received_bytes = entry.received_bytes.saturating_add(received_bytes);
        entry.transmitted_bytes = entry.transmitted_bytes.saturating_add(transmitted_bytes);
    }

    Some(samples)
}

fn split_csv_line(line: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for character in line.chars() {
        match character {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                columns.push(current.trim_matches('"').to_string());
                current.clear();
            }
            _ => current.push(character),
        }
    }
    columns.push(current.trim_matches('"').to_string());
    columns
}

fn parse_u64_cell(value: &str) -> Option<u64> {
    value.trim().parse().ok()
}

fn process_score(process: &ProcessSample) -> f32 {
    let memory_mib = process.memory_bytes as f32 / 1_048_576.0;
    let disk_mib = process
        .disk_read_bytes
        .saturating_add(process.disk_write_bytes) as f32
        / 1_048_576.0;
    process.cpu_usage_percent * 1000.0 + memory_mib + disk_mib * 10.0
}

fn read_battery_snapshot() -> Option<BatterySnapshot> {
    let output = Command::new("pmset").args(["-g", "batt"]).output().ok()?;
    if !output.status.success() {
        return None;
    }
    parse_pmset_battery(&String::from_utf8_lossy(&output.stdout))
}

/// Name, parent, own `.app` bundle and developer-tool location of one
/// process, for attribution.
struct ProcessIdentity {
    name: String,
    parent_pid: Option<u32>,
    bundle: Option<String>,
    /// Executable lives under the user's home, `/opt/homebrew` or `/usr/local`.
    developer_tool: bool,
}

impl ProcessIdentity {
    fn new(name: String, parent_pid: Option<u32>, executable: Option<&Path>, home: Option<&Path>) -> Self {
        Self {
            name,
            parent_pid,
            bundle: executable.and_then(outermost_app_bundle),
            developer_tool: executable.is_some_and(|path| is_developer_tool_path(path, home)),
        }
    }
}

/// The application a process is attributed to: stable graph key and name.
#[derive(Debug, PartialEq, Eq)]
struct ApplicationRef {
    key: String,
    name: String,
}

impl ApplicationRef {
    fn named(name: &str) -> Self {
        Self {
            key: format!("app:{name}"),
            name: name.to_string(),
        }
    }

    /// The single application for bundle-less system daemons.
    fn system_services() -> Self {
        Self {
            key: "app:system-services".to_string(),
            name: "System services".to_string(),
        }
    }
}

/// Whether a bundle-less executable is a developer tool that keeps its own
/// application (grouped by process name) instead of joining System services.
fn is_developer_tool_path(executable: &Path, home: Option<&Path>) -> bool {
    home.is_some_and(|home| home.components().count() > 1 && executable.starts_with(home))
        || executable.starts_with("/opt/homebrew")
        || executable.starts_with("/usr/local")
}

/// The OUTERMOST `.app` bundle containing `executable`, so helper bundles
/// nested inside an application (`Code Helper (Renderer).app` inside
/// `Visual Studio Code.app`) belong to that application.
fn outermost_app_bundle(executable: &Path) -> Option<String> {
    executable.components().find_map(|component| {
        let name = component.as_os_str().to_string_lossy();
        name.strip_suffix(".app")
            .filter(|name| !name.is_empty())
            .map(str::to_string)
    })
}

/// Maximum parent hops when attributing a bundle-less process.
const MAX_ANCESTOR_HOPS: usize = 64;

/// Application of `pid`: its own outermost bundle, else the nearest
/// ancestor's bundle (never through launchd/pid 1 or kernel_task), else its
/// process name for developer tools, else the shared System services.
fn resolve_application(pid: u32, table: &HashMap<u32, ProcessIdentity>) -> ApplicationRef {
    let Some(process) = table.get(&pid) else {
        return ApplicationRef::system_services();
    };
    if let Some(bundle) = &process.bundle {
        return ApplicationRef::named(bundle);
    }
    let mut next = process.parent_pid;
    for _ in 0..MAX_ANCESTOR_HOPS {
        let Some(parent_pid) = next.filter(|parent| *parent > 1 && *parent != pid) else {
            break;
        };
        let Some(parent) = table.get(&parent_pid) else {
            break;
        };
        if matches!(parent.name.as_str(), "launchd" | "kernel_task") {
            break;
        }
        if let Some(bundle) = &parent.bundle {
            return ApplicationRef::named(bundle);
        }
        next = parent.parent_pid;
    }
    if process.developer_tool {
        ApplicationRef::named(&process.name)
    } else {
        ApplicationRef::system_services()
    }
}

fn os_str_to_display(value: &std::ffi::OsStr) -> Option<String> {
    let display = value.to_string_lossy().trim().to_string();
    (!display.is_empty()).then_some(display)
}

fn metadata<const N: usize>(pairs: [(&str, &str); N]) -> HashMap<String, String> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn optional_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "none".to_string(), |value| value.to_string())
}

fn bool_metric(value: bool) -> f32 {
    if value {
        1.0
    } else {
        0.0
    }
}

fn percentage(numerator: u64, denominator: u64) -> f32 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 * 100.0 / denominator as f64) as f32
    }
}

fn timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_micros() as i64
}

fn hostname() -> String {
    System::host_name().unwrap_or_else(|| "macos-host".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOME: &str = "/Users/dev";

    fn identity(name: &str, parent_pid: Option<u32>, exe: Option<&str>) -> ProcessIdentity {
        ProcessIdentity::new(
            name.to_string(),
            parent_pid,
            exe.map(Path::new),
            Some(Path::new(HOME)),
        )
    }

    fn app(pid: u32, table: &HashMap<u32, ProcessIdentity>) -> String {
        resolve_application(pid, table).name
    }

    /// Samples every process of `table` as `collect_processes` would.
    fn samples_from(table: &HashMap<u32, ProcessIdentity>) -> Vec<ProcessSample> {
        table
            .iter()
            .map(|(pid, identity)| {
                let application = resolve_application(*pid, table);
                ProcessSample {
                    key: format!("process:{pid}:1"),
                    app_key: application.key,
                    app_name: application.name,
                    name: identity.name.clone(),
                    pid: *pid,
                    parent_pid: identity.parent_pid,
                    start_time: 1,
                    cpu_usage_percent: (*pid % 7) as f32,
                    memory_bytes: u64::from(*pid) * 1_048_576,
                    disk_read_bytes: 0,
                    disk_write_bytes: 0,
                    network_received_bytes: None,
                    network_transmitted_bytes: None,
                }
            })
            .collect()
    }

    fn base_table() -> HashMap<u32, ProcessIdentity> {
        HashMap::from([
            (0, identity("kernel_task", None, None)),
            (1, identity("launchd", Some(0), Some("/sbin/launchd"))),
        ])
    }

    fn sample(pid: u32, app: &str, cpu: f32, memory_bytes: u64) -> ProcessSample {
        ProcessSample {
            key: format!("process:{pid}:1"),
            app_key: format!("app:{app}"),
            app_name: app.to_string(),
            name: format!("proc-{pid}"),
            pid,
            parent_pid: Some(1),
            start_time: 1,
            cpu_usage_percent: cpu,
            memory_bytes,
            disk_read_bytes: u64::from(pid),
            disk_write_bytes: 2 * u64::from(pid),
            network_received_bytes: pid.is_multiple_of(2).then_some(10),
            network_transmitted_bytes: None,
        }
    }

    fn keys(pids: &[u32]) -> HashSet<String> {
        pids.iter().map(|pid| format!("process:{pid}:1")).collect()
    }

    #[test]
    fn outermost_bundle_groups_nested_helpers_under_their_application() {
        let cases = [
            (
                "/Applications/Visual Studio Code.app/Contents/Frameworks/Code Helper (Renderer).app/Contents/MacOS/Code Helper (Renderer)",
                "Visual Studio Code",
            ),
            (
                "/Applications/Brave Browser.app/Contents/Frameworks/Brave Browser Framework.framework/Versions/1.2.3/Helpers/Brave Browser Helper (GPU).app/Contents/MacOS/Brave Browser Helper (GPU)",
                "Brave Browser",
            ),
            (
                "/Applications/Google Chrome.app/Contents/Frameworks/Google Chrome Framework.framework/Versions/1/Helpers/Google Chrome Helper (Renderer).app/Contents/MacOS/Google Chrome Helper (Renderer)",
                "Google Chrome",
            ),
            ("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "Google Chrome"),
        ];
        for (path, expected) in cases {
            assert_eq!(outermost_app_bundle(Path::new(path)).as_deref(), Some(expected), "{path}");
        }
        assert_eq!(outermost_app_bundle(Path::new("/usr/bin/zsh")), None);
        assert_eq!(outermost_app_bundle(Path::new("/tmp/.app/x")), None);
    }

    #[test]
    fn bundle_less_processes_inherit_the_nearest_bundled_ancestor() {
        let table = HashMap::from([
            (0, identity("kernel_task", None, None)),
            (1, identity("launchd", Some(0), None)),
            (
                100,
                identity(
                    "Code",
                    Some(1),
                    Some("/Applications/Visual Studio Code.app/Contents/MacOS/Code"),
                ),
            ),
            (200, identity("zsh", Some(100), Some("/bin/zsh"))),
            (300, identity("cargo", Some(200), None)),
            (400, identity("mds", Some(1), Some("/System/Library/mds"))),
            (500, identity("orphan", Some(9999), None)),
            (600, identity("child-of-kernel", Some(0), None)),
        ]);
        assert_eq!(app(300, &table), "Visual Studio Code");
        assert_eq!(app(200, &table), "Visual Studio Code");
        assert_eq!(app(100, &table), "Visual Studio Code");
        // Bundle-less processes with no bundled ancestor share System services.
        for pid in [400, 1, 0, 500, 600] {
            assert_eq!(resolve_application(pid, &table), ApplicationRef::system_services(), "{pid}");
        }
    }

    #[test]
    fn bundle_less_developer_tools_keep_their_own_application() {
        let mut table = base_table();
        table.insert(10, identity("cargo", Some(1), Some("/Users/dev/.cargo/bin/cargo")));
        table.insert(11, identity("python3", Some(1), Some("/opt/homebrew/bin/python3")));
        table.insert(12, identity("node", Some(1), Some("/usr/local/bin/node")));
        table.insert(13, identity("poet", Some(1), Some("/Users/dev/git/ih/target/release/poet")));
        table.insert(14, identity("syslogd", Some(1), Some("/usr/sbin/syslogd")));
        table.insert(15, identity("hidden", Some(1), None));
        assert_eq!(resolve_application(10, &table), ApplicationRef::named("cargo"));
        assert_eq!(app(11, &table), "python3");
        assert_eq!(app(12, &table), "node");
        assert_eq!(resolve_application(13, &table).key, "app:poet");
        assert_eq!(resolve_application(14, &table).key, "app:system-services");
        assert_eq!(resolve_application(15, &table).key, "app:system-services");
        // A root home never makes every path a developer tool.
        assert!(!is_developer_tool_path(Path::new("/usr/sbin/syslogd"), Some(Path::new("/"))));
    }

    #[test]
    fn launchd_daemons_collapse_into_one_system_services_application() {
        let mut table = base_table();
        for pid in 100..400 {
            table.insert(pid, identity(&format!("daemon{pid}"), Some(1), Some("/usr/libexec/daemon")));
        }
        table.insert(500, identity("cargo", Some(1), Some("/Users/dev/.cargo/bin/cargo")));
        table.insert(501, identity("node", Some(1), Some("/opt/homebrew/bin/node")));
        let processes = samples_from(&table);
        let emitted = ProcessSelector::new(12, 30).select(&processes);
        let plans = plan_applications(&processes, &emitted);
        let keys = plans.iter().map(|plan| plan.app_key).collect::<Vec<_>>();
        assert_eq!(keys, ["app:cargo", "app:node", "app:system-services"]);
        let system = &plans[2];
        assert_eq!(system.app_name, "System services");
        assert_eq!(system.emitted.len() + system.other.as_ref().map_or(0, |o| o.count), 302);
    }

    #[test]
    fn realistic_process_table_bounds_application_payloads() {
        let mut table = base_table();
        let mut pid = 100;
        for index in 0..40 {
            let main = pid;
            let bundle = format!("/Applications/App{index}.app");
            table.insert(main, identity(&format!("App{index}"), Some(1), Some(&format!("{bundle}/Contents/MacOS/App{index}"))));
            for helper in 0..2 {
                pid += 1;
                let exe = format!("{bundle}/Contents/Frameworks/Helper {helper}.app/Contents/MacOS/Helper {helper}");
                table.insert(pid, identity(&format!("Helper {helper}"), Some(main), Some(&exe)));
            }
            pid += 1;
        }
        for _ in 0..458 {
            table.insert(pid, identity(&format!("daemon{pid}"), Some(1), Some("/usr/libexec/daemon")));
            pid += 1;
        }
        for index in 0..20 {
            let exe = format!("/opt/homebrew/bin/tool{index}");
            table.insert(pid, identity(&format!("tool{index}"), Some(1), Some(&exe)));
            pid += 1;
        }
        assert_eq!(table.len(), 600);
        let processes = samples_from(&table);
        let emitted = ProcessSelector::new(DEFAULT_TOP_PROCESSES, DEFAULT_PROCESS_HOLD_SAMPLES).select(&processes);
        let plans = plan_applications(&processes, &emitted);
        let applications = plans.len();
        let process_payloads = plans.iter().map(|plan| plan.emitted.len()).sum::<usize>();
        let other_payloads = plans.iter().filter(|plan| plan.other.is_some()).count();
        println!(
            "600 processes: {applications} application payloads, {process_payloads} process payloads, {other_payloads} other-processes payloads, {} total",
            applications + process_payloads + other_payloads
        );
        assert_eq!(applications, 40 + 20 + 1);
        assert_eq!(process_payloads, DEFAULT_TOP_PROCESSES);
        assert!(other_payloads <= applications);
    }

    #[test]
    fn ancestor_walk_terminates_on_parent_cycles() {
        let table = HashMap::from([
            (10, identity("a", Some(11), None)),
            (11, identity("b", Some(10), None)),
        ]);
        assert_eq!(resolve_application(10, &table), ApplicationRef::system_services());
    }

    #[test]
    fn application_totals_cover_every_process_and_children_sum_to_them() {
        let processes = vec![
            sample(2, "Brave", 40.0, 100),
            sample(3, "Brave", 5.0, 50),
            sample(4, "Brave", 1.0, 25),
            sample(5, "mds", 2.0, 7),
        ];
        let plans = plan_applications(&processes, &keys(&[2]));
        assert_eq!(plans.len(), 2, "every application with a process appears");

        let brave = &plans[0];
        assert_eq!(brave.app_key, "app:Brave");
        assert_eq!(brave.totals.cpu_usage_percent, 46.0);
        assert_eq!(brave.totals.memory_bytes, 175);
        assert_eq!(brave.emitted.iter().map(|p| p.pid).collect::<Vec<_>>(), [2]);
        let other = brave.other.as_ref().expect("un-emitted Brave processes");
        assert_eq!(other.count, 2);
        let mut children = ApplicationTotals::of(brave.emitted[0]);
        children.cpu_usage_percent += other.totals.cpu_usage_percent;
        children.memory_bytes += other.totals.memory_bytes;
        children.disk_read_bytes += other.totals.disk_read_bytes;
        children.disk_write_bytes += other.totals.disk_write_bytes;
        children.network_received_bytes += other.totals.network_received_bytes;
        assert_eq!(children.cpu_usage_percent, brave.totals.cpu_usage_percent);
        assert_eq!(children.memory_bytes, brave.totals.memory_bytes);
        assert_eq!(children.disk_read_bytes, brave.totals.disk_read_bytes);
        assert_eq!(children.disk_write_bytes, brave.totals.disk_write_bytes);
        assert_eq!(children.network_received_bytes, brave.totals.network_received_bytes);
        // pid 4 had network, pid 3 did not: the remainder reports it, while
        // transmitted stays absent (missing, not a measured zero).
        assert!(other.totals.has_network_received);
        assert!(!other.totals.has_network_transmitted);
        assert!(other.totals.readings(8)[6].is_none());

        let mds = &plans[1];
        assert!(mds.emitted.is_empty());
        assert_eq!(mds.other.as_ref().map(|other| other.count), Some(1));
        assert_eq!(other_processes_key(mds.app_key), "app:mds:other");
    }

    #[test]
    fn fully_emitted_application_has_no_other_child() {
        let processes = vec![sample(2, "Brave", 1.0, 1), sample(3, "Brave", 1.0, 1)];
        let plans = plan_applications(&processes, &keys(&[2, 3]));
        assert!(plans[0].other.is_none());
        assert_eq!(plans[0].emitted.len(), 2);
    }

    #[test]
    fn selector_holds_processes_that_leave_the_top_while_they_exist() {
        let mut selector = ProcessSelector::new(1, 2);
        let mut processes = vec![sample(2, "a", 50.0, 0), sample(3, "b", 10.0, 0)];
        assert_eq!(selector.select(&processes), keys(&[2]));
        // pid 3 overtakes pid 2, which is held for two more samples.
        processes[1].cpu_usage_percent = 90.0;
        assert_eq!(selector.select(&processes), keys(&[2, 3]));
        assert_eq!(selector.select(&processes), keys(&[2, 3]));
        assert_eq!(selector.select(&processes), keys(&[3]));
        // pid 2 retakes the top; pid 3 is now the held one.
        processes[0].cpu_usage_percent = 99.0;
        assert_eq!(selector.select(&processes), keys(&[2, 3]));
        // A held process that exits loses its hold and is not resurrected.
        let only_two = vec![processes[0].clone()];
        assert_eq!(selector.select(&only_two), keys(&[2]));
        assert_eq!(selector.select(&processes), keys(&[2]));
    }

    #[test]
    fn selector_bounds_held_processes_to_twice_the_top() {
        let mut selector = ProcessSelector::new(2, 30);
        let mut processes = (2..=9)
            .map(|pid| sample(pid, "a", pid as f32, 0))
            .collect::<Vec<_>>();
        // Rotate the leaders so every process is ranked top at some point.
        for round in 0..4 {
            for process in &mut processes {
                process.cpu_usage_percent = if (process.pid as usize - 2) / 2 == round {
                    100.0
                } else {
                    process.pid as f32
                };
            }
            let emitted = selector.select(&processes);
            assert!(emitted.len() <= 4, "round {round}: {emitted:?}");
        }
    }

    #[test]
    fn selector_always_emits_self_observed_processes() {
        let mut selector = ProcessSelector::new(1, 0);
        let mut poet = sample(7, "poet", 0.0, 0);
        poet.name = "poet".into();
        let mut muse = sample(8, "muse", 0.0, 0);
        muse.name = "ih-muse-macos".into();
        let processes = vec![sample(2, "a", 50.0, 0), sample(3, "b", 40.0, 0), poet, muse];
        assert_eq!(selector.select(&processes), keys(&[2, 7, 8]));
    }

    #[test]
    fn process_score_keeps_resource_consumers_visible() {
        let low_cpu_high_memory = ProcessSample {
            key: "process:1:1".to_string(),
            app_key: "app:memory".to_string(),
            app_name: "memory".to_string(),
            name: "memory".to_string(),
            pid: 1,
            parent_pid: None,
            start_time: 1,
            cpu_usage_percent: 0.0,
            memory_bytes: 8 * 1_048_576 * 1024,
            disk_read_bytes: 0,
            disk_write_bytes: 0,
            network_received_bytes: None,
            network_transmitted_bytes: None,
        };
        let small_process = ProcessSample {
            memory_bytes: 1,
            ..low_cpu_high_memory.clone()
        };

        assert!(process_score(&low_cpu_high_memory) > process_score(&small_process));
    }

    #[test]
    fn parses_nettop_process_network_csv() {
        let output = "time,process,bytes_in,bytes_out\n1,Foo,10,20\n1,Foo,2,3\n1,Bar,0,5\n";
        let samples = parse_nettop_process_network(output).unwrap();

        assert_eq!(
            samples.get("Foo"),
            Some(&ProcessNetworkSample {
                received_bytes: 12,
                transmitted_bytes: 23,
            })
        );
        assert_eq!(
            samples.get("Bar"),
            Some(&ProcessNetworkSample {
                received_bytes: 0,
                transmitted_bytes: 5,
            })
        );
    }
}
