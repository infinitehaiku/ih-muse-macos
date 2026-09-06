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
                MetricReading::new(MEMORY_USED_BYTES_METRIC, self.used_memory_bytes as f32)?,
            ],
        )
    }

    pub fn swap_metrics(&self, timestamp: i64, element_id: u64) -> Option<MetricPayload> {
        metric_payload(
            timestamp,
            element_id,
            &[
                MetricReading::new(SWAP_USAGE_METRIC, self.swap_usage_percent)?,
                MetricReading::new(SWAP_USED_BYTES_METRIC, self.used_swap_bytes as f32)?,
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
                MetricReading::new(DISK_USED_BYTES_METRIC, self.used_disk_bytes as f32)?,
                MetricReading::new(
                    DISK_AVAILABLE_BYTES_METRIC,
                    self.available_disk_bytes as f32,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricReading {
    pub code: &'static str,
    pub value: f32,
}

impl MetricReading {
    pub fn new(code: &'static str, value: f32) -> Option<Self> {
        value.is_finite().then_some(Self { code, value })
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
    vec![
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
            "Process CPU",
            "Process CPU usage percentage",
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
    ]
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
}
