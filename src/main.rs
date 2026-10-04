use std::collections::{BTreeMap, HashMap};
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
    let mut sampler = Sampler::new();
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
        let mut snapshot = sampler.collect(snapshot_time, args.top_processes, args.process_network);
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
        if let Err(error) = send_pending(&client, &mut pending).await {
            eprintln!(
                "Poet send failed; {} sample(s) queued for retry: {error:#}",
                pending.len()
            );
        }

        samples_sent += 1;
        println!(
            "sample {} at {}: {} payloads, {} processes, {} disks, {} interfaces, {} sensors, battery {}",
            samples_sent,
            published.timestamp,
            published.payload_count,
            snapshot.processes.len(),
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
            send_pending(&client, &mut pending).await?;
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

/// Retains OS sampling state needed to compute deltas between observations.
struct Sampler {
    system: System,
    disks: Disks,
    networks: Networks,
    components: Components,
    previous_battery: Option<(i64, BatterySnapshot)>,
    process_network_available: bool,
}

impl Sampler {
    fn new() -> Self {
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
        top_processes: usize,
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
            processes: collect_processes(&self.system, top_processes, &process_network),
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
struct CollectedSnapshot {
    poet_health: Option<PoetHealthSample>,
    host: ih_muse_macos::HostSnapshot,
    cpu_cores: Vec<CpuCoreSample>,
    disks: Vec<DiskSample>,
    network_interfaces: Vec<NetworkInterfaceSample>,
    battery: Option<BatteryReading>,
    thermal_sensors: Vec<ThermalSensorSample>,
    processes: Vec<ProcessSample>,
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
    processes: Vec<RegisteredProcess>,
}

/// Poet IDs for a process leaf and its stable application parent.
struct RegisteredProcess {
    application_id: u64,
    process_id: u64,
}

#[derive(Default)]
/// Additive resource observations for the sampled processes of one application.
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
    let registered = ensure_snapshot_elements(registry, static_elements, snapshot);
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

    let mut applications = BTreeMap::<u64, ApplicationTotals>::new();
    for (process, registered_process) in snapshot.processes.iter().zip(&registered.processes) {
        push_payload(
            &mut payloads,
            now,
            registered_process.process_id,
            [
                MetricReading::new(PROCESS_CPU_USAGE_METRIC, process.cpu_usage_percent),
                ih_muse_macos::cpu_capacity_percent(
                    process.cpu_usage_percent,
                    snapshot.cpu_cores.len(),
                )
                .and_then(|value| {
                    MetricReading::new(ih_muse_macos::PROCESS_CPU_CAPACITY_METRIC, value)
                }),
                MetricReading::new(PROCESS_MEMORY_BYTES_METRIC, process.memory_bytes as f64),
                MetricReading::new(
                    PROCESS_DISK_READ_BYTES_METRIC,
                    process.disk_read_bytes as f64,
                ),
                MetricReading::new(
                    PROCESS_DISK_WRITE_BYTES_METRIC,
                    process.disk_write_bytes as f64,
                ),
                process.network_received_bytes.and_then(|value| {
                    MetricReading::new(
                        ih_muse_macos::PROCESS_NETWORK_RECEIVED_BYTES_METRIC,
                        value as f64,
                    )
                }),
                process.network_transmitted_bytes.and_then(|value| {
                    MetricReading::new(
                        ih_muse_macos::PROCESS_NETWORK_TRANSMITTED_BYTES_METRIC,
                        value as f64,
                    )
                }),
            ]
            .into_iter()
            .chain(
                snapshot
                    .poet_health
                    .iter()
                    .flat_map(|health| health.readings_for(process.pid, now)),
            ),
        );
        applications
            .entry(registered_process.application_id)
            .or_default()
            .add(process);
    }
    for (application_id, totals) in applications {
        push_payload(
            &mut payloads,
            now,
            application_id,
            [
                MetricReading::new(PROCESS_CPU_USAGE_METRIC, totals.cpu_usage_percent),
                ih_muse_macos::cpu_capacity_percent(
                    totals.cpu_usage_percent,
                    snapshot.cpu_cores.len(),
                )
                .and_then(|value| {
                    MetricReading::new(ih_muse_macos::PROCESS_CPU_CAPACITY_METRIC, value)
                }),
                MetricReading::new(PROCESS_MEMORY_BYTES_METRIC, totals.memory_bytes as f64),
                MetricReading::new(
                    PROCESS_DISK_READ_BYTES_METRIC,
                    totals.disk_read_bytes as f64,
                ),
                MetricReading::new(
                    PROCESS_DISK_WRITE_BYTES_METRIC,
                    totals.disk_write_bytes as f64,
                ),
                totals
                    .has_network_received
                    .then(|| {
                        MetricReading::new(
                            ih_muse_macos::PROCESS_NETWORK_RECEIVED_BYTES_METRIC,
                            totals.network_received_bytes as f64,
                        )
                    })
                    .flatten(),
                totals
                    .has_network_transmitted
                    .then(|| {
                        MetricReading::new(
                            ih_muse_macos::PROCESS_NETWORK_TRANSMITTED_BYTES_METRIC,
                            totals.network_transmitted_bytes as f64,
                        )
                    })
                    .flatten(),
            ],
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
    pending: &mut PendingSamples<GraphIntakeRequest>,
) -> Result<usize> {
    let mut sent = 0;
    while let Some(request) = pending.oldest() {
        client.publish(request).await?;
        pending.acknowledge_oldest();
        sent += 1;
    }
    Ok(sent)
}

fn ensure_snapshot_elements(
    registry: &mut ElementRegistry,
    static_elements: &StaticElements,
    snapshot: &CollectedSnapshot,
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

    let mut processes = Vec::with_capacity(snapshot.processes.len());
    for process in &snapshot.processes {
        let app_id = registry
            .ensure(
                process.app_key.clone(),
                KIND_MACOS_APPLICATION,
                process.app_name.clone(),
                Some(static_elements.applications_group_id),
                metadata([("resource", "process"), ("level", "application")]),
            );
        let process_id = registry
            .ensure(
                process.key.clone(),
                KIND_MACOS_PROCESS,
                format!("{} ({})", process.name, process.pid),
                Some(app_id),
                metadata([
                    ("resource", "process"),
                    ("level", "process"),
                    ("pid", &process.pid.to_string()),
                    ("parent_pid", &optional_u32(process.parent_pid)),
                    ("start_time", &process.start_time.to_string()),
                ]),
            );
        processes.push(RegisteredProcess {
            application_id: app_id,
            process_id,
        });
    }

    RegisteredSnapshotElements {
        cpu_core_ids,
        disk_ids,
        network_interface_ids,
        battery_id,
        thermal_sensor_ids,
        processes,
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

fn collect_processes(
    system: &System,
    top_processes: usize,
    process_network: &HashMap<String, ProcessNetworkSample>,
) -> Vec<ProcessSample> {
    let mut processes = system
        .processes()
        .iter()
        .map(|(pid, process)| {
            let name = os_str_to_display(process.name())
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| format!("process-{}", pid.as_u32()));
            let app_name = application_name(&name, process.exe());
            let app_key = format!("app:{app_name}");
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
        .collect::<Vec<_>>();

    processes.sort_by(|left, right| {
        process_score(right)
            .partial_cmp(&process_score(left))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.pid.cmp(&right.pid))
    });
    let mut rank = 0;
    processes.retain(|process| {
        let keep = retain_process(rank, top_processes, &process.name);
        rank += 1;
        keep
    });
    processes
}

fn retain_process(rank: usize, top_processes: usize, name: &str) -> bool {
    rank < top_processes.max(1) || matches!(name, "poet" | "ih-muse-macos")
}

#[test]
fn self_observation_is_independent_of_process_ranking() {
    assert!(retain_process(100, 12, "poet"));
    assert!(retain_process(100, 12, "ih-muse-macos"));
    assert!(!retain_process(100, 12, "other"));
    assert!(retain_process(0, 12, "other"));
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

fn application_name(process_name: &str, executable: Option<&Path>) -> String {
    executable
        .and_then(|path| {
            path.ancestors().find_map(|ancestor| {
                let file_name = ancestor.file_name()?.to_string_lossy();
                file_name
                    .strip_suffix(".app")
                    .map(|name| name.to_string())
                    .filter(|name| !name.is_empty())
            })
        })
        .unwrap_or_else(|| process_name.to_string())
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

    #[test]
    fn application_name_prefers_app_bundle_without_exposing_full_path() {
        assert_eq!(
            application_name(
                "Google Chrome Helper",
                Some(Path::new(
                    "/Applications/Google Chrome.app/Contents/Frameworks/helper"
                ))
            ),
            "Google Chrome"
        );
        assert_eq!(application_name("launchd", None), "launchd");
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
