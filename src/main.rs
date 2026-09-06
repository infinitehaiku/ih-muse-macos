use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use ih_muse_client::PoetClient;
use ih_muse_core::Transport;
use ih_muse_proto::{ElementRegistration, MetricPayload};
use sysinfo::{Components, Disks, Networks, ProcessesToUpdate, System};

use ih_muse_macos::{
    element_kind_definitions, metric_definitions, metric_payload, parse_pmset_battery,
    snapshot_from_values, BatterySnapshot, MetricReading, BATTERY_CHARGE_METRIC,
    BATTERY_CHARGING_METRIC, BATTERY_DRAIN_RATE_METRIC, BATTERY_ON_BATTERY_METRIC,
    CPU_CORE_USAGE_METRIC, DISK_AVAILABLE_BYTES_METRIC, DISK_USAGE_METRIC, DISK_USED_BYTES_METRIC,
    KIND_MACOS_APPLICATION, KIND_MACOS_CPU_CORE, KIND_MACOS_DISK_VOLUME, KIND_MACOS_HOST,
    KIND_MACOS_NETWORK_INTERFACE, KIND_MACOS_POWER_SOURCE, KIND_MACOS_PROCESS, KIND_MACOS_RESOURCE,
    KIND_MACOS_RESOURCE_GROUP, KIND_MACOS_THERMAL_SENSOR, NETWORK_RECEIVED_BYTES_METRIC,
    NETWORK_TOTAL_RECEIVED_BYTES_METRIC, NETWORK_TOTAL_TRANSMITTED_BYTES_METRIC,
    NETWORK_TRANSMITTED_BYTES_METRIC, PROCESS_CPU_USAGE_METRIC, PROCESS_DISK_READ_BYTES_METRIC,
    PROCESS_DISK_WRITE_BYTES_METRIC, PROCESS_MEMORY_BYTES_METRIC, THERMAL_TEMPERATURE_METRIC,
};

const DEFAULT_TOP_PROCESSES: usize = 40;

#[derive(Debug, Parser)]
#[command(about = "Send local macOS host, resource, and process metrics to Infinite Haiku Poet")]
struct Args {
    #[arg(
        long,
        env = "IH_MUSE_POET_URL",
        default_value = "http://127.0.0.1:8000"
    )]
    poet_url: String,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.once && args.samples.is_some() {
        anyhow::bail!("--once and --samples cannot be used together");
    }

    let client = PoetClient::new(&[args.poet_url.clone()]);
    client.register_metrics(&metric_definitions()).await?;
    client
        .register_element_kinds(&element_kind_definitions())
        .await?;

    let mut registry = ElementRegistry::default();
    let static_elements = register_static_elements(&client, &mut registry).await?;
    let mut sampler = Sampler::new();
    let sample_limit = args.samples.or_else(|| args.once.then_some(1));
    let mut samples_sent = 0;

    println!(
        "macOS Muse sending to {} every {}s; top process cap: {}",
        args.poet_url,
        args.interval_seconds.max(1),
        args.top_processes
    );
    println!(
        "Hierarchy: host -> resource groups -> cores, memory areas, volumes, interfaces, battery, sensors, applications -> processes"
    );

    loop {
        let snapshot_time = timestamp();
        let snapshot = sampler.collect(snapshot_time, args.top_processes, args.process_network);
        let published =
            publish_snapshot(&client, &mut registry, &static_elements, &snapshot).await?;

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
            return client.shutdown().await.map_err(Into::into);
        }
        tokio::time::sleep(Duration::from_secs(args.interval_seconds.max(1))).await;
    }
}

#[derive(Default)]
struct ElementRegistry {
    ids: HashMap<String, u64>,
}

impl ElementRegistry {
    async fn ensure(
        &mut self,
        client: &PoetClient,
        key: String,
        kind_code: &str,
        name: String,
        parent_id: Option<u64>,
        metadata: HashMap<String, String>,
    ) -> Result<u64> {
        if let Some(id) = self.ids.get(&key) {
            return Ok(*id);
        }
        let mut results = client
            .register_elements(&[ElementRegistration::new(
                kind_code, name, metadata, parent_id,
            )])
            .await?;
        let id = results
            .pop()
            .context("Poet did not return an element registration result")?
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        self.ids.insert(key, id);
        Ok(id)
    }
}

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

async fn register_static_elements(
    client: &PoetClient,
    registry: &mut ElementRegistry,
) -> Result<StaticElements> {
    let host_id = registry
        .ensure(
            client,
            "host".to_string(),
            KIND_MACOS_HOST,
            hostname(),
            None,
            metadata([("scope", "host")]),
        )
        .await?;
    let cpu_group_id = ensure_group(client, registry, host_id, "CPU", "cpu").await?;
    let memory_group_id = ensure_group(client, registry, host_id, "Memory", "memory").await?;
    let storage_group_id = ensure_group(client, registry, host_id, "Storage", "storage").await?;
    let network_group_id = ensure_group(client, registry, host_id, "Network", "network").await?;
    let power_group_id = ensure_group(client, registry, host_id, "Power", "power").await?;
    let thermal_group_id = ensure_group(client, registry, host_id, "Thermal", "thermal").await?;
    let applications_group_id =
        ensure_group(client, registry, host_id, "Applications", "applications").await?;
    let system_group_id = ensure_group(client, registry, host_id, "System", "system").await?;

    let total_cpu_id = registry
        .ensure(
            client,
            "resource:cpu:total".to_string(),
            KIND_MACOS_RESOURCE,
            "Total CPU".to_string(),
            Some(cpu_group_id),
            metadata([("resource", "cpu"), ("level", "host")]),
        )
        .await?;
    let physical_memory_id = registry
        .ensure(
            client,
            "resource:memory:physical".to_string(),
            KIND_MACOS_RESOURCE,
            "Physical memory".to_string(),
            Some(memory_group_id),
            metadata([("resource", "memory"), ("level", "host")]),
        )
        .await?;
    let swap_id = registry
        .ensure(
            client,
            "resource:memory:swap".to_string(),
            KIND_MACOS_RESOURCE,
            "Swap".to_string(),
            Some(memory_group_id),
            metadata([("resource", "swap"), ("level", "host")]),
        )
        .await?;
    let load_average_id = registry
        .ensure(
            client,
            "resource:system:load".to_string(),
            KIND_MACOS_RESOURCE,
            "Load average".to_string(),
            Some(system_group_id),
            metadata([("resource", "load"), ("level", "host")]),
        )
        .await?;

    Ok(StaticElements {
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
    })
}

async fn ensure_group(
    client: &PoetClient,
    registry: &mut ElementRegistry,
    host_id: u64,
    name: &str,
    key: &str,
) -> Result<u64> {
    registry
        .ensure(
            client,
            format!("group:{key}"),
            KIND_MACOS_RESOURCE_GROUP,
            name.to_string(),
            Some(host_id),
            metadata([("resource_group", key)]),
        )
        .await
}

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

struct CollectedSnapshot {
    host: ih_muse_macos::HostSnapshot,
    cpu_cores: Vec<CpuCoreSample>,
    disks: Vec<DiskSample>,
    network_interfaces: Vec<NetworkInterfaceSample>,
    battery: Option<BatteryReading>,
    thermal_sensors: Vec<ThermalSensorSample>,
    processes: Vec<ProcessSample>,
}

struct CpuCoreSample {
    index: usize,
    name: String,
    usage_percent: f32,
}

struct DiskSample {
    key: String,
    name: String,
    usage_percent: f32,
    used_bytes: u64,
    available_bytes: u64,
    total_bytes: u64,
}

struct NetworkInterfaceSample {
    key: String,
    name: String,
    received_bytes: u64,
    transmitted_bytes: u64,
    total_received_bytes: u64,
    total_transmitted_bytes: u64,
}

struct BatteryReading {
    snapshot: BatterySnapshot,
    drain_percent_per_hour: Option<f32>,
}

struct ThermalSensorSample {
    key: String,
    name: String,
    temperature_celsius: f32,
}

#[derive(Clone)]
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
struct ProcessNetworkSample {
    received_bytes: u64,
    transmitted_bytes: u64,
}

struct PublishedSample {
    timestamp: i64,
    payload_count: usize,
}

struct RegisteredSnapshotElements {
    cpu_core_ids: Vec<u64>,
    disk_ids: Vec<u64>,
    network_interface_ids: Vec<u64>,
    battery_id: Option<u64>,
    thermal_sensor_ids: Vec<u64>,
    process_ids: Vec<u64>,
}

async fn publish_snapshot(
    client: &PoetClient,
    registry: &mut ElementRegistry,
    static_elements: &StaticElements,
    snapshot: &CollectedSnapshot,
) -> Result<PublishedSample> {
    let registered = ensure_snapshot_elements(client, registry, static_elements, snapshot).await?;
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
                MetricReading::new(DISK_USED_BYTES_METRIC, disk.used_bytes as f32),
                MetricReading::new(DISK_AVAILABLE_BYTES_METRIC, disk.available_bytes as f32),
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
                    interface.received_bytes as f32,
                ),
                MetricReading::new(
                    NETWORK_TRANSMITTED_BYTES_METRIC,
                    interface.transmitted_bytes as f32,
                ),
                MetricReading::new(
                    NETWORK_TOTAL_RECEIVED_BYTES_METRIC,
                    interface.total_received_bytes as f32,
                ),
                MetricReading::new(
                    NETWORK_TOTAL_TRANSMITTED_BYTES_METRIC,
                    interface.total_transmitted_bytes as f32,
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

    for (process, process_id) in snapshot.processes.iter().zip(registered.process_ids) {
        push_payload(
            &mut payloads,
            now,
            process_id,
            [
                MetricReading::new(PROCESS_CPU_USAGE_METRIC, process.cpu_usage_percent),
                MetricReading::new(PROCESS_MEMORY_BYTES_METRIC, process.memory_bytes as f32),
                MetricReading::new(
                    PROCESS_DISK_READ_BYTES_METRIC,
                    process.disk_read_bytes as f32,
                ),
                MetricReading::new(
                    PROCESS_DISK_WRITE_BYTES_METRIC,
                    process.disk_write_bytes as f32,
                ),
                process.network_received_bytes.and_then(|value| {
                    MetricReading::new(
                        ih_muse_macos::PROCESS_NETWORK_RECEIVED_BYTES_METRIC,
                        value as f32,
                    )
                }),
                process.network_transmitted_bytes.and_then(|value| {
                    MetricReading::new(
                        ih_muse_macos::PROCESS_NETWORK_TRANSMITTED_BYTES_METRIC,
                        value as f32,
                    )
                }),
            ],
        );
    }

    let payload_count = payloads.len();
    if payload_count > 0 {
        client.send_metrics(payloads, None).await?;
    }
    Ok(PublishedSample {
        timestamp: now,
        payload_count,
    })
}

async fn ensure_snapshot_elements(
    client: &PoetClient,
    registry: &mut ElementRegistry,
    static_elements: &StaticElements,
    snapshot: &CollectedSnapshot,
) -> Result<RegisteredSnapshotElements> {
    let mut cpu_core_ids = Vec::with_capacity(snapshot.cpu_cores.len());
    for core in &snapshot.cpu_cores {
        cpu_core_ids.push(
            registry
                .ensure(
                    client,
                    format!("cpu-core:{}", core.index),
                    KIND_MACOS_CPU_CORE,
                    core.name.clone(),
                    Some(static_elements.cpu_group_id),
                    metadata([
                        ("resource", "cpu"),
                        ("level", "core"),
                        ("core_index", &core.index.to_string()),
                    ]),
                )
                .await?,
        );
    }

    let mut disk_ids = Vec::with_capacity(snapshot.disks.len());
    for disk in &snapshot.disks {
        disk_ids.push(
            registry
                .ensure(
                    client,
                    disk.key.clone(),
                    KIND_MACOS_DISK_VOLUME,
                    disk.name.clone(),
                    Some(static_elements.storage_group_id),
                    metadata([("resource", "disk"), ("level", "volume")]),
                )
                .await?,
        );
    }

    let mut network_interface_ids = Vec::with_capacity(snapshot.network_interfaces.len());
    for interface in &snapshot.network_interfaces {
        network_interface_ids.push(
            registry
                .ensure(
                    client,
                    interface.key.clone(),
                    KIND_MACOS_NETWORK_INTERFACE,
                    interface.name.clone(),
                    Some(static_elements.network_group_id),
                    metadata([("resource", "network"), ("level", "interface")]),
                )
                .await?,
        );
    }

    let battery_id = if snapshot.battery.is_some() {
        Some(
            registry
                .ensure(
                    client,
                    "power:battery:internal".to_string(),
                    KIND_MACOS_POWER_SOURCE,
                    "Internal battery".to_string(),
                    Some(static_elements.power_group_id),
                    metadata([("resource", "battery"), ("level", "power_source")]),
                )
                .await?,
        )
    } else {
        None
    };

    let mut thermal_sensor_ids = Vec::with_capacity(snapshot.thermal_sensors.len());
    for sensor in &snapshot.thermal_sensors {
        thermal_sensor_ids.push(
            registry
                .ensure(
                    client,
                    sensor.key.clone(),
                    KIND_MACOS_THERMAL_SENSOR,
                    sensor.name.clone(),
                    Some(static_elements.thermal_group_id),
                    metadata([("resource", "thermal"), ("level", "sensor")]),
                )
                .await?,
        );
    }

    let mut process_ids = Vec::with_capacity(snapshot.processes.len());
    for process in &snapshot.processes {
        let app_id = registry
            .ensure(
                client,
                process.app_key.clone(),
                KIND_MACOS_APPLICATION,
                process.app_name.clone(),
                Some(static_elements.applications_group_id),
                metadata([("resource", "process"), ("level", "application")]),
            )
            .await?;
        process_ids.push(
            registry
                .ensure(
                    client,
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
                )
                .await?,
        );
    }

    Ok(RegisteredSnapshotElements {
        cpu_core_ids,
        disk_ids,
        network_interface_ids,
        battery_id,
        thermal_sensor_ids,
        process_ids,
    })
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
    processes.truncate(top_processes.max(1));
    processes
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
