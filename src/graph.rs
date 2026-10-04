//! The macOS Muse's graph identity and batch mapping (`ih.graph.v1`).
//!
//! Elements get deterministic identities from their collector keys, not
//! Poet-assigned ids, so the same Mac is the same entity on every Poet of a
//! cluster and Poets can replicate it. The host is a `Host`, a process a
//! `Process` (host, pid, start time), everything else `Other` in the host's
//! namespace. Each entity carries `entity.kind` and `display.name`, the
//! attributes dashboards and AGS already read.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use ih_muse_proto::{
    metric_id_from_code, AggregationTemporality, AttributeValue, Entity, EntityIdentity,
    DashboardDefinition, EntityKey, GraphBatch, GraphIntakeRequest, InstrumentationScope, JoinStatus, MetricDefinition,
    MetricDescriptor, MetricDisplay, MetricId, MetricInstrument, MetricObservation, MetricPayload,
    MetricUnit, Number, Observation, OrganizationId, Provenance, RelationKind, SpatialAggregation,
    TemporalRelation, TimeRange, TypedMetricValue, UnitDisplay, ValueDomain,
    GRAPH_CONTRACT_REVISION, GRAPH_INTAKE_CONTRACT_REVISION, GRAPH_INTAKE_SCHEMA_VERSION,
    GRAPH_SCHEMA_VERSION,
};

use crate::{KIND_MACOS_HOST, KIND_MACOS_PROCESS};

const SOURCE_ID: &str = "ih-muse-macos";

/// One collector element: its stable key, kind, name, parent and metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct ElementInfo {
    pub key: String,
    pub kind: String,
    pub name: String,
    pub parent: Option<u64>,
    pub metadata: HashMap<String, String>,
}

/// Local element handles (session-scoped numbers used by the payload code)
/// mapped to deterministic graph identities.
pub struct GraphRegistry {
    organization: String,
    host_id: String,
    ids: HashMap<String, u64>,
    elements: HashMap<u64, ElementInfo>,
    descriptors: HashMap<MetricId, MetricDescriptor>,
    /// This Muse's dashboard definitions, attached to every intake until a
    /// Poet acknowledges one batch that carried them (then left out).
    dashboards: Vec<DashboardDefinition>,
    dashboards_delivered: bool,
}

impl GraphRegistry {
    pub fn new(organization: impl Into<String>, host_id: impl Into<String>, definitions: &[MetricDefinition]) -> Self {
        Self {
            organization: organization.into(),
            host_id: host_id.into(),
            ids: HashMap::new(),
            elements: HashMap::new(),
            descriptors: definitions
                .iter()
                .map(|definition| (metric_id_from_code(&definition.code), descriptor(definition)))
                .collect(),
            dashboards: crate::dashboards::dashboard_definitions(),
            dashboards_delivered: false,
        }
    }

    /// Records that a Poet acknowledged `request`. Once a batch that carried
    /// the dashboard definitions is acknowledged, later intakes leave them out
    /// until the process restarts (they are static per Muse version, and the
    /// Poets of a cluster replicate them among themselves).
    pub fn acknowledge(&mut self, request: &GraphIntakeRequest) {
        if !request.batch.dashboards.is_empty() {
            self.dashboards_delivered = true;
        }
    }

    /// The handle for `key`, registering the element on first use (no network).
    pub fn ensure(
        &mut self,
        key: String,
        kind: &str,
        name: String,
        parent: Option<u64>,
        metadata: HashMap<String, String>,
    ) -> u64 {
        if let Some(id) = self.ids.get(&key) {
            return *id;
        }
        let id = self.ids.len() as u64 + 1;
        self.ids.insert(key.clone(), id);
        self.elements.insert(id, ElementInfo { key, kind: kind.into(), name, parent, metadata });
        id
    }

    fn entity_key(&self, element: &ElementInfo) -> EntityKey {
        let environment_id = format!("macos:{}", self.host_id);
        let identity = match element.kind.as_str() {
            KIND_MACOS_HOST => EntityIdentity::Host { environment_id, host_id: self.host_id.clone() },
            KIND_MACOS_PROCESS => {
                let pid = element.metadata.get("pid").and_then(|pid| pid.parse::<u32>().ok());
                let started = element.metadata.get("start_time").and_then(|time| time.parse::<u64>().ok());
                match (pid, started) {
                    (Some(pid), Some(started)) if pid > 0 && started > 0 => EntityIdentity::Process {
                        environment_id,
                        host_id: self.host_id.clone(),
                        pid,
                        start_time_unix_nano: started.saturating_mul(1_000_000_000),
                    },
                    _ => EntityIdentity::Other { namespace: environment_id, id: element.key.clone() },
                }
            }
            _ => EntityIdentity::Other { namespace: environment_id, id: element.key.clone() },
        };
        EntityKey { organization: OrganizationId(self.organization.clone()), identity }
    }

    fn entity(&self, element: &ElementInfo) -> Entity {
        let mut attributes = element
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), AttributeValue::String(value.clone())))
            .collect::<BTreeMap<_, _>>();
        attributes.insert("entity.kind".into(), AttributeValue::String(element.kind.clone()));
        attributes.insert("display.name".into(), AttributeValue::String(element.name.clone()));
        Entity {
            key: self.entity_key(element),
            lifetime: TimeRange { from_unix_nano: 0, to_unix_nano: u64::MAX },
            attributes,
        }
    }

    /// One graph batch for one sample: the measured elements with their
    /// ancestors, `Contains` relations, and one observation per value.
    pub fn batch(&self, payloads: &[MetricPayload], observed_at_unix_nano: u64) -> GraphBatch {
        let provenance = Provenance {
            source_id: format!("{SOURCE_ID}:{}", self.host_id),
            source_revision: env!("CARGO_PKG_VERSION").into(),
            observed_at_unix_nano,
            join_status: JoinStatus::Resolved,
        };
        let mut included = BTreeSet::new();
        for payload in payloads {
            let mut next = Some(payload.element_id);
            while let Some(id) = next {
                if !included.insert(id) {
                    break;
                }
                next = self.elements.get(&id).and_then(|element| element.parent);
            }
        }
        let keys = included
            .iter()
            .filter_map(|id| self.elements.get(id).map(|element| (*id, self.entity_key(element))))
            .collect::<HashMap<_, _>>();
        let entities = included.iter().filter_map(|id| self.elements.get(id)).map(|element| self.entity(element)).collect();
        let relations = included
            .iter()
            .filter_map(|id| {
                let element = self.elements.get(id)?;
                Some(TemporalRelation {
                    subject: keys.get(&element.parent?)?.clone(),
                    object: keys.get(id)?.clone(),
                    kind: RelationKind::Contains,
                    valid_time: TimeRange { from_unix_nano: 0, to_unix_nano: u64::MAX },
                    provenance: provenance.clone(),
                    attributes: BTreeMap::new(),
                })
            })
            .collect();
        let scope = InstrumentationScope {
            name: SOURCE_ID.into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
            schema_url: None,
            attributes: BTreeMap::new(),
        };
        let mut observations = Vec::new();
        for payload in payloads {
            let Some(entity) = keys.get(&payload.element_id) else { continue };
            let time = (payload.time.max(1) as u64).saturating_mul(1_000);
            for (metric_id, value) in payload.metric_ids.iter().zip(&payload.values) {
                let (Some(descriptor), Some(value)) = (self.descriptors.get(metric_id), value) else { continue };
                if !value.is_finite() {
                    continue;
                }
                let number = Number::F64(*value);
                let measured = match descriptor.instrument {
                    MetricInstrument::Sum { .. } => TypedMetricValue::Sum(number),
                    _ => TypedMetricValue::Gauge(number),
                };
                observations.push(Observation {
                    entity: entity.clone(),
                    scope: scope.clone(),
                    descriptor: descriptor.clone(),
                    ratio_denominator: None,
                    attributes: BTreeMap::new(),
                    start_time_unix_nano: None,
                    time_unix_nano: time,
                    value: MetricObservation::Measured { value: measured },
                    provenance: provenance.clone(),
                });
            }
        }
        GraphBatch {
            schema_version: GRAPH_SCHEMA_VERSION,
            contract_revision: GRAPH_CONTRACT_REVISION.into(),
            entities,
            relations,
            observations,
            events: Vec::new(),
            derivations: Vec::new(),
            availability: Vec::new(),
            dashboards: Vec::new(),
        }
    }

    /// The intake request for one sample. Its delivery id depends only on the
    /// host and the sample time, so a retry (to any Poet) is stored once.
    /// It carries the dashboard definitions until one such batch is
    /// acknowledged ([`Self::acknowledge`]): normally only the first batch
    /// after start, and every queued batch while no Poet is reachable, so a
    /// dropped or failed first batch cannot lose them.
    pub fn intake(&self, sample_time_micros: i64, payloads: &[MetricPayload], observed_at_unix_nano: u64) -> GraphIntakeRequest {
        GraphIntakeRequest {
            schema_version: GRAPH_INTAKE_SCHEMA_VERSION,
            contract_revision: GRAPH_INTAKE_CONTRACT_REVISION.into(),
            delivery_id: format!("{SOURCE_ID}:{}:{sample_time_micros}", self.host_id),
            organization: self.organization.clone(),
            owner_id: self.organization.clone(),
            batch: GraphBatch {
                dashboards: if self.dashboards_delivered { Vec::new() } else { self.dashboards.clone() },
                ..self.batch(payloads, observed_at_unix_nano)
            },
        }
    }
}

/// The graph descriptor for a Muse metric definition. It matches Poet's import
/// of the same definitions from the legacy path, so dashboards, views and AGS
/// see identical metrics whichever path delivered them.
pub fn descriptor(definition: &MetricDefinition) -> MetricDescriptor {
    let display = definition.display.clone().unwrap_or_else(|| MetricDisplay::infer(&definition.code));
    let (ucum, unit_display) = match display.unit.as_str() {
        "bytes" => ("By", UnitDisplay::Bytes),
        "seconds" => ("s", UnitDisplay::Seconds),
        "percent" => ("%", UnitDisplay::Percent),
        "celsius" => ("Cel", UnitDisplay::Celsius),
        "boolean" => ("1", UnitDisplay::Boolean),
        _ => ("1", UnitDisplay::Number),
    };
    let instrument = match display.kind.as_str() {
        "counter" => MetricInstrument::Sum { monotonic: true, temporality: AggregationTemporality::Cumulative },
        "delta" => MetricInstrument::Sum { monotonic: false, temporality: AggregationTemporality::Delta },
        _ => MetricInstrument::Gauge,
    };
    let spatial_aggregation = match display.aggregation.as_str() {
        "sum" => SpatialAggregation::Sum,
        "mean" => SpatialAggregation::Mean,
        "min" => SpatialAggregation::Min,
        "max" => SpatialAggregation::Max,
        "last" => SpatialAggregation::Last,
        _ => SpatialAggregation::None,
    };
    MetricDescriptor {
        name: definition.code.clone(),
        description: definition.description.clone(),
        unit: MetricUnit { ucum: ucum.into(), display: unit_display },
        value_domain: if display.unit == "boolean" { ValueDomain::Boolean } else { ValueDomain::Unbounded },
        instrument,
        spatial_aggregation,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{metric_definitions, CPU_USAGE_METRIC, KIND_MACOS_RESOURCE, NETWORK_RECEIVED_BYTES_METRIC, PROCESS_CPU_USAGE_METRIC};

    fn metadata(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect()
    }

    fn registry() -> (GraphRegistry, u64, u64, u64) {
        let mut registry = GraphRegistry::new("local", "macbook", &metric_definitions());
        let host = registry.ensure("host".into(), KIND_MACOS_HOST, "macbook".into(), None, metadata(&[]));
        let cpu = registry.ensure("resource:cpu:total".into(), KIND_MACOS_RESOURCE, "Total CPU".into(), Some(host), metadata(&[]));
        let process = registry.ensure(
            "process:42".into(),
            KIND_MACOS_PROCESS,
            "rustc (42)".into(),
            Some(host),
            metadata(&[("pid", "42"), ("start_time", "1700000000")]),
        );
        (registry, host, cpu, process)
    }

    #[test]
    fn identities_are_deterministic_so_every_poet_sees_one_mac() {
        let (first, ..) = registry();
        let (second, ..) = registry();
        let payload = |element| vec![MetricPayload::new(1_000_000, element, vec![metric_id_from_code(CPU_USAGE_METRIC)], vec![Some(12.5)])];
        let a = first.intake(1_000_000, &payload(2), 1);
        let b = second.intake(1_000_000, &payload(2), 1);
        assert_eq!(a.batch.entities, b.batch.entities, "same keys give the same entities");
        assert_eq!(a.delivery_id, b.delivery_id, "a resend is the same delivery");
        assert_eq!(a.delivery_id, "ih-muse-macos:macbook:1000000");
    }

    #[test]
    fn a_sample_becomes_a_valid_graph_batch_with_its_ancestors() {
        let (registry, _, cpu, process) = registry();
        let payloads = vec![
            MetricPayload::new(2_000_000, cpu, vec![metric_id_from_code(CPU_USAGE_METRIC)], vec![Some(40.0)]),
            MetricPayload::new(2_000_000, process, vec![metric_id_from_code(PROCESS_CPU_USAGE_METRIC)], vec![Some(88.0)]),
        ];
        let request = registry.intake(2_000_000, &payloads, 5);
        request.validate().expect("graph intake request is valid");
        let batch = &request.batch;
        assert_eq!(batch.entities.len(), 3, "host, CPU and process");
        assert_eq!(batch.relations.len(), 2, "host contains CPU and process");
        assert!(batch.entities.iter().any(|entity| matches!(
            &entity.key.identity,
            EntityIdentity::Process { pid: 42, start_time_unix_nano: 1_700_000_000_000_000_000, .. }
        )));
        assert!(batch.entities.iter().any(|entity| matches!(&entity.key.identity, EntityIdentity::Host { host_id, .. } if host_id == "macbook")));
        assert_eq!(batch.observations.len(), 2);
        assert_eq!(batch.observations[0].time_unix_nano, 2_000_000_000, "microseconds become nanoseconds");
        assert_eq!(
            batch.entities.iter().find(|entity| matches!(entity.key.identity, EntityIdentity::Process { .. })).unwrap().attributes["entity.kind"],
            AttributeValue::String(KIND_MACOS_PROCESS.into())
        );
    }

    #[test]
    fn delta_metrics_are_sums_and_levels_are_gauges() {
        let definitions = metric_definitions();
        let find = |code: &str| definitions.iter().find(|definition| definition.code == code).unwrap();
        assert!(matches!(
            descriptor(find(NETWORK_RECEIVED_BYTES_METRIC)).instrument,
            MetricInstrument::Sum { temporality: AggregationTemporality::Delta, .. }
        ));
        assert_eq!(descriptor(find(CPU_USAGE_METRIC)).instrument, MetricInstrument::Gauge);
        assert_eq!(descriptor(find(CPU_USAGE_METRIC)).unit.ucum, "%");
    }

    #[test]
    fn dashboards_ride_the_first_batch_until_a_poet_acknowledges_one() {
        let (mut registry, _, cpu, _) = registry();
        let payload = |time| vec![MetricPayload::new(time, cpu, vec![metric_id_from_code(CPU_USAGE_METRIC)], vec![Some(1.0)])];
        let first = registry.intake(1_000_000, &payload(1_000_000), 1);
        first.validate().expect("a batch with definitions is valid");
        assert_eq!(first.batch.dashboards, crate::dashboards::dashboard_definitions());
        // No Poet reachable yet: the next queued batch carries them too, so a
        // dropped first batch cannot lose them.
        let queued = registry.intake(2_000_000, &payload(2_000_000), 2);
        assert_eq!(queued.batch.dashboards, first.batch.dashboards);
        // A batch without definitions being acknowledged changes nothing.
        let mut plain = queued.clone();
        plain.batch.dashboards.clear();
        registry.acknowledge(&plain);
        assert!(!registry.intake(3_000_000, &payload(3_000_000), 3).batch.dashboards.is_empty());
        registry.acknowledge(&first);
        let later = registry.intake(4_000_000, &payload(4_000_000), 4);
        assert!(later.batch.dashboards.is_empty(), "sent once per start");
        assert!(!serde_json::to_string(&later).unwrap().contains("dashboards"), "later batches keep their old bytes");
    }
}
