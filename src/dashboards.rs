//! The macOS Muse's default dashboards, defined in code.
//!
//! The Muse's author defines what matters in its data; the definitions version
//! with this crate and travel to Poet in `GraphBatch.dashboards` (see
//! [`crate::graph::GraphRegistry::intake`]). Poet stores them as data and Kabuki
//! renders them; neither has macOS-specific dashboard code.
//!
//! Every panel names a metric this Muse emits (the `*_METRIC` constants), so a
//! renamed metric breaks the build or the tests, not the dashboard.

use ih_muse_proto::dashboard::{
    FilterOp, GoldenSignal, PanelAggregation, PanelFilter, PanelGroupBy, PanelSpec,
    PanelThresholds,
};
use ih_muse_proto::{DashboardAppliesTo, DashboardBlock, DashboardDefinition};

use crate::{
    BATTERY_CHARGE_METRIC, CPU_USAGE_METRIC, DISK_USAGE_METRIC, KIND_MACOS_APPLICATION,
    KIND_MACOS_HOST, KIND_MACOS_PROCESS, LOAD_ONE_METRIC, MEMORY_USAGE_METRIC,
    NETWORK_RECEIVED_BYTES_METRIC, NETWORK_TRANSMITTED_BYTES_METRIC, PROCESS_CPU_USAGE_METRIC,
    PROCESS_MEMORY_BYTES_METRIC, SWAP_USAGE_METRIC,
};

/// The Muse kind this Muse declares: the prefix of its element kinds
/// (`macos_host`, `macos_process`, ...) and of its dashboard ids.
pub const MUSE_KIND: &str = "macos";

/// Id of the host dashboard.
pub const HOST_DASHBOARD_ID: &str = "macos.host";

/// Every default dashboard this Muse defines.
pub fn dashboard_definitions() -> Vec<DashboardDefinition> {
    vec![host_dashboard()]
}

/// The Mac host: golden-signal measurements (CPU, memory, network), host
/// resources over time, and the processes and applications that use it.
pub fn host_dashboard() -> DashboardDefinition {
    DashboardDefinition {
        id: HOST_DASHBOARD_ID.into(),
        revision: 1,
        title: "macOS host".into(),
        description: "CPU, memory, disk, network and the busiest processes of a Mac observed by the macOS Muse.".into(),
        muse_kind: MUSE_KIND.into(),
        muse_versions: Some(concat!(">=", env!("CARGO_PKG_VERSION")).into()),
        applies_to: DashboardAppliesTo::MuseRoots {
            entity_kinds: vec![KIND_MACOS_HOST.into()],
            identity_kinds: Vec::new(),
        },
        panels: vec![
            panel("cpu", "CPU usage", CPU_USAGE_METRIC, PanelAggregation::Avg)
                .signal(GoldenSignal::Saturation)
                .thresholds(80.0, 95.0, true),
            panel("memory", "Memory usage", MEMORY_USAGE_METRIC, PanelAggregation::Avg)
                .signal(GoldenSignal::Saturation)
                .thresholds(85.0, 95.0, true),
            panel("network_rx", "Network received", NETWORK_RECEIVED_BYTES_METRIC, PanelAggregation::Rate)
                .signal(GoldenSignal::Traffic),
            panel("network_tx", "Network sent", NETWORK_TRANSMITTED_BYTES_METRIC, PanelAggregation::Rate)
                .signal(GoldenSignal::Traffic),
            panel("load", "Load average (1 min)", LOAD_ONE_METRIC, PanelAggregation::Avg),
            panel("disk", "Disk usage", DISK_USAGE_METRIC, PanelAggregation::Max)
                .thresholds(85.0, 95.0, true),
            panel("swap", "Swap usage", SWAP_USAGE_METRIC, PanelAggregation::Avg),
            panel("top_cpu", "Top processes by CPU", PROCESS_CPU_USAGE_METRIC, PanelAggregation::Avg)
                .top_entities(KIND_MACOS_PROCESS, 8),
            panel("top_memory", "Top processes by memory", PROCESS_MEMORY_BYTES_METRIC, PanelAggregation::Avg)
                .top_entities(KIND_MACOS_PROCESS, 8),
            panel("top_apps_cpu", "Top applications by CPU", PROCESS_CPU_USAGE_METRIC, PanelAggregation::Avg)
                .top_entities(KIND_MACOS_APPLICATION, 8),
            panel("battery", "Battery charge", BATTERY_CHARGE_METRIC, PanelAggregation::Last)
                .thresholds(20.0, 10.0, false),
        ],
        blocks: vec![
            DashboardBlock {
                label: "Host: over time".into(),
                text: None,
                panels: ids(&["load", "disk", "swap", "battery"]),
                ..Default::default()
            },
            DashboardBlock {
                label: "Host: what uses the machine".into(),
                text: Some("The busiest processes and applications, top 8 each.".into()),
                panels: ids(&["top_cpu", "top_memory", "top_apps_cpu"]),
                ..Default::default()
            },
        ],
        columns: Some(3),
    }
}

fn ids(panels: &[&str]) -> Vec<String> {
    panels.iter().map(|id| (*id).to_string()).collect()
}

fn panel(id: &str, title: &str, metric: &str, aggregation: PanelAggregation) -> PanelSpec {
    PanelSpec {
        id: id.into(),
        title: title.into(),
        metric: metric.into(),
        aggregation,
        // Time series at the kind's default size; new model fields keep
        // their defaults so they never break this Muse again.
        ..Default::default()
    }
}

/// Small builder steps that keep the panel list above readable.
trait PanelSpecExt {
    fn signal(self, signal: GoldenSignal) -> Self;
    fn thresholds(self, warning: f64, critical: f64, higher_is_worse: bool) -> Self;
    fn top_entities(self, entity_kind: &str, top_n: usize) -> Self;
}

impl PanelSpecExt for PanelSpec {
    fn signal(mut self, signal: GoldenSignal) -> Self {
        self.signal = Some(signal);
        self
    }

    fn thresholds(mut self, warning: f64, critical: f64, higher_is_worse: bool) -> Self {
        self.thresholds = Some(PanelThresholds { warning, critical, higher_is_worse });
        self
    }

    fn top_entities(mut self, entity_kind: &str, top_n: usize) -> Self {
        self.filters = vec![PanelFilter {
            key: "entity.kind".into(),
            op: FilterOp::Eq,
            value: serde_json::Value::String(entity_kind.into()),
        }];
        self.group_by = Some(PanelGroupBy::Entity);
        self.top_n = Some(top_n);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::metric_definitions;

    fn example_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../ih-muse/examples/dashboards/macos-host.json")
    }

    #[test]
    fn every_definition_validates_and_ids_are_namespaced() {
        let definitions = dashboard_definitions();
        assert!(!definitions.is_empty());
        let mut ids = BTreeSet::new();
        for definition in &definitions {
            definition.validate().unwrap_or_else(|error| panic!("{}: {error}", definition.id));
            assert_eq!(definition.muse_kind, MUSE_KIND);
            assert!(definition.id.starts_with(&format!("{MUSE_KIND}.")));
            assert!(ids.insert(definition.id.clone()), "duplicate id {}", definition.id);
        }
    }

    #[test]
    fn every_panel_metric_is_a_metric_this_muse_emits() {
        let emitted: BTreeSet<String> =
            metric_definitions().into_iter().map(|definition| definition.code).collect();
        for definition in dashboard_definitions() {
            for panel in &definition.panels {
                assert!(
                    emitted.contains(&panel.metric),
                    "{} panel {} names {}, which the Muse does not emit",
                    definition.id,
                    panel.id,
                    panel.metric
                );
            }
        }
    }

    /// The code-built definition equals the reference JSON shipped with the
    /// SDK (converted from Poet's former built-in profile), apart from the
    /// version requirement only the Muse knows. Compared as typed values,
    /// since serde defaults (`filters: []`, `higher_is_worse: true`) may be
    /// written or omitted.
    #[test]
    fn host_dashboard_matches_the_sdk_example() {
        let mut example: DashboardDefinition =
            serde_json::from_str(&std::fs::read_to_string(example_path()).unwrap()).unwrap();
        assert_eq!(example.muse_versions, None);
        example.muse_versions = Some(concat!(">=", env!("CARGO_PKG_VERSION")).into());
        assert_eq!(host_dashboard(), example);
        let round_trip: DashboardDefinition =
            serde_json::from_str(&serde_json::to_string(&host_dashboard()).unwrap()).unwrap();
        assert_eq!(round_trip, host_dashboard());
    }

    /// Runs `ih-muse-cli dashboard check` on the serialized definitions when
    /// `IH_MUSE_CLI` names the binary (built from ../ih-muse). The scratch
    /// directory is removed on every outcome.
    #[test]
    fn serialized_definitions_pass_the_cli_checker() {
        let Some(cli) = std::env::var_os("IH_MUSE_CLI") else {
            eprintln!("IH_MUSE_CLI not set; skipping the ih-muse-cli check (structure is checked against the example)");
            return;
        };
        let scratch = Scratch::new();
        let files: Vec<PathBuf> = dashboard_definitions()
            .iter()
            .map(|definition| {
                let path = scratch.0.join(format!("{}.json", definition.id));
                std::fs::write(&path, serde_json::to_vec_pretty(definition).unwrap()).unwrap();
                path
            })
            .collect();
        let output = std::process::Command::new(cli)
            .args(["dashboard", "check"])
            .args(&files)
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "checker failed: {stdout}{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(stdout.matches("OK").count(), files.len(), "{stdout}");
    }

    /// A per-test directory removed on drop, so pass, fail and panic clean up.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "ih-muse-macos-dashboards-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
