//! A bounded, privacy-safe subset of the macOS unified log, sent to Poet as
//! OTLP logs (`/v1/logs`).
//!
//! `log stream --style ndjson` runs with a predicate that only lets through
//! the events this Muse can link to what it already shows: sleep and wake,
//! battery level, process crashes (ReportCrash), disk mount and unmount,
//! network link and configuration changes, Wi-Fi association and thermal
//! pressure. Each line is parsed, classified (anything the classifier does
//! not recognise is dropped and counted as filtered at the Muse), redacted
//! (user paths, e-mail and network addresses, on by default), cut to a size
//! limit and rate limited, then queued for the sender.
//!
//! Every record's resource carries `host.name` (the Muse's host id), which
//! Poet's element mapping resolves to the Mac host element. A crash record
//! also carries `process.pid` and `process.executable.name` of the crashed
//! process, so Poet maps it to that process element while the Muse still
//! reports it. Volume, interface, battery and sensor elements are named in
//! `macos.element.key` (the Muse's element key) for readers; Poet maps such
//! records to the host.

use std::collections::{HashMap, VecDeque};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

/// Records kept while no Poet accepts them; past it the oldest are dropped.
pub const MAX_QUEUED_RECORDS: usize = 2_000;
/// Body bytes kept per record; longer bodies are cut and marked.
pub const MAX_BODY_BYTES: usize = 2_048;
/// Input lines longer than this are skipped (counted as oversized).
pub const MAX_LINE_BYTES: usize = 64 * 1024;
/// Records per OTLP request.
pub const MAX_BATCH_RECORDS: usize = 500;
/// Encoded bytes per OTLP request (about; one record may exceed it alone).
pub const MAX_BATCH_BYTES: usize = 512 * 1024;
/// Burst a single event category may use before its own rate applies, so
/// one noisy source cannot starve the others.
const CATEGORY_BURST: f64 = 50.0;
const CATEGORY_RATE_PER_SECOND: f64 = 5.0;

/// Prefix a test line must start with to be collected (test hook only).
pub const TEST_LINE_PREFIX: &str = "ih-muse-test";

/// What the collector keeps and how fast (command-line settings).
#[derive(Clone, Debug)]
pub struct LogSettings {
    /// Mask user paths, e-mail and network addresses (default on).
    pub redact: bool,
    /// Also collect `logger "ih-muse-test ..."` lines (for e2e checks).
    pub test_hook: bool,
    /// Records per second over all categories, and the burst above it.
    pub rate_per_second: f64,
    pub burst: f64,
}

impl Default for LogSettings {
    fn default() -> Self {
        Self { redact: true, test_hook: false, rate_per_second: 20.0, burst: 200.0 }
    }
}

/// One unified log line as `log stream --style ndjson` prints it (the
/// fields this Muse reads).
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedLogEntry {
    #[serde(default)]
    pub message_type: String,
    #[serde(default)]
    pub process_image_path: String,
    #[serde(default)]
    pub subsystem: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub event_message: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default, rename = "processID")]
    pub process_id: i64,
    #[serde(default)]
    pub event_type: String,
}

impl UnifiedLogEntry {
    /// The logging process's name (last path component).
    pub fn process(&self) -> &str {
        self.process_image_path.rsplit('/').next().unwrap_or_default()
    }
}

/// Parses one ndjson line; `None` for the banner, blank or invalid lines
/// and non-log events (activities, signposts).
pub fn parse_line(line: &str) -> Option<UnifiedLogEntry> {
    let line = line.trim();
    if !line.starts_with('{') {
        return None;
    }
    let entry: UnifiedLogEntry = serde_json::from_str(line).ok()?;
    (entry.event_type.is_empty() || entry.event_type == "logEvent").then_some(entry)
}

/// `2026-10-04 17:50:33.803491+0200` as ns since the epoch.
pub fn parse_timestamp(text: &str) -> Option<u64> {
    let (date, rest) = text.split_once(' ')?;
    let mut date_parts = date.split('-');
    let year: i32 = date_parts.next()?.parse().ok()?;
    let month: u8 = date_parts.next()?.parse().ok()?;
    let day: u8 = date_parts.next()?.parse().ok()?;
    let sign_at = rest.rfind(['+', '-'])?;
    let (clock, offset) = rest.split_at(sign_at);
    let (hms, fraction) = clock.split_once('.').unwrap_or((clock, "0"));
    let mut hms_parts = hms.split(':');
    let hour: u8 = hms_parts.next()?.parse().ok()?;
    let minute: u8 = hms_parts.next()?.parse().ok()?;
    let second: u8 = hms_parts.next()?.parse().ok()?;
    let digits = fraction.get(..fraction.len().min(9))?;
    let nanos: u32 = format!("{digits:0<9}").parse().ok()?;
    let sign: i64 = if offset.starts_with('-') { -1 } else { 1 };
    let offset = &offset[1..];
    if offset.len() != 4 {
        return None;
    }
    let offset_seconds = sign * (offset[..2].parse::<i64>().ok()? * 3600 + offset[2..].parse::<i64>().ok()? * 60);
    let month = time::Month::try_from(month).ok()?;
    let date = time::Date::from_calendar_date(year, month, day).ok()?;
    let clock = time::Time::from_hms_nano(hour, minute, second, nanos).ok()?;
    let utc = time::PrimitiveDateTime::new(date, clock).assume_utc().unix_timestamp_nanos();
    u64::try_from(utc - offset_seconds as i128 * 1_000_000_000).ok()
}

/// OTLP severity number for a unified log level (`messageType`); the level
/// name itself is kept as the severity text.
pub fn severity_number(message_type: &str) -> u8 {
    match message_type {
        "Debug" => 5,
        "Info" => 9,
        "Default" => 10,
        "Error" => 17,
        "Fault" => 18,
        _ => 9,
    }
}

/// Event categories, each rate limited on its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Category {
    Power,
    Crash,
    Disk,
    Network,
    Thermal,
    Test,
}

/// An attribute value of an outgoing record.
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Bool(bool),
}

/// What a recognised line means: its event name, category, lowest severity
/// (a crash is an error whatever its log level), the process it is about
/// and extra attributes.
#[derive(Clone, Debug, PartialEq)]
pub struct Classified {
    pub event_name: &'static str,
    pub category: Category,
    pub severity_floor: u8,
    /// pid and executable name of the process the event is about.
    pub process: Option<(i64, String)>,
    /// The Muse element key the event concerns (`network:en0`, ...).
    pub element_key: Option<String>,
    pub attributes: Vec<(String, AttrValue)>,
}

impl Classified {
    fn new(event_name: &'static str, category: Category) -> Self {
        Self { event_name, category, severity_floor: 0, process: None, element_key: None, attributes: Vec::new() }
    }
    fn floor(mut self, severity: u8) -> Self {
        self.severity_floor = severity;
        self
    }
    fn element(mut self, key: impl Into<String>) -> Self {
        self.element_key = Some(key.into());
        self
    }
    fn attr(mut self, key: &str, value: AttrValue) -> Self {
        self.attributes.push((key.into(), value));
        self
    }
}

/// The `log stream` predicate: only candidate lines leave `logd`.
pub fn predicate(test_hook: bool) -> String {
    let mut clauses = vec![
        r#"(process == "ReportCrash" AND eventMessage BEGINSWITH "Formulating")"#.to_string(),
        r#"(process == "diskarbitrationd" AND (eventMessage BEGINSWITH "mounted disk" OR eventMessage BEGINSWITH "unmounted disk" OR eventMessage BEGINSWITH "ejected disk"))"#.into(),
        r#"(process == "powerd" AND (eventMessage BEGINSWITH "Battery percentage" OR eventMessage CONTAINS "Entering Sleep" OR eventMessage BEGINSWITH "Wake from" OR eventMessage CONTAINS "DarkWake from"))"#.into(),
        r#"(process == "kernel" AND eventMessage BEGINSWITH "Wake reason")"#.into(),
        r#"(process == "configd" AND (eventMessage BEGINSWITH "Process interface link status" OR eventMessage CONTAINS " link ACTIVE" OR eventMessage CONTAINS " link INACTIVE" OR eventMessage BEGINSWITH "network changed"))"#.into(),
        r#"(process == "airportd" AND eventMessage BEGINSWITH "[corewifi] AUTO-JOIN: Updated associated network")"#.into(),
        r#"(process == "powerexperienced" AND eventMessage BEGINSWITH "kThermalPressureContext")"#.into(),
    ];
    if test_hook {
        clauses.push(format!(r#"(process == "logger" AND eventMessage BEGINSWITH "{TEST_LINE_PREFIX}")"#));
    }
    clauses.join(" OR ")
}

/// Recognises the events of [`predicate`]; `None` for anything else.
pub fn classify(entry: &UnifiedLogEntry, test_hook: bool) -> Option<Classified> {
    let message = entry.event_message.as_str();
    match entry.process() {
        "logger" if test_hook && message.starts_with(TEST_LINE_PREFIX) => {
            Some(Classified::new("macos.test.line", Category::Test).element("host"))
        }
        "ReportCrash" => {
            // "Formulating fatal 309 report for corpse[62576] ihmusecrash"
            let rest = message.strip_prefix("Formulating ")?;
            let (kind, rest) = rest.split_once(' ')?;
            let target = rest.split_once("report for ")?.1;
            let open = target.find('[')?;
            let close = open + target[open..].find(']')?;
            let pid: i64 = target[open + 1..close].parse().ok()?;
            let name = target[close + 1..].trim();
            let name = if name.is_empty() { target[..open].trim() } else { name };
            if pid <= 0 || name.is_empty() {
                return None;
            }
            let fatal = kind == "fatal";
            Some(
                Classified::new(if fatal { "macos.process.crash" } else { "macos.process.report" }, Category::Crash)
                    .floor(if fatal { 17 } else { 13 })
                    .attr("macos.crash.kind", AttrValue::Str(kind.into()))
                    .with_process(pid, name),
            )
        }
        "diskarbitrationd" => {
            // "mounted disk, id = /dev/disk4s1, success."
            let (event, rest) = message.split_once(" disk, id = ")?;
            let event_name = match event {
                "mounted" => "macos.disk.mounted",
                "unmounted" => "macos.disk.unmounted",
                "ejected" => "macos.disk.ejected",
                _ => return None,
            };
            let (device, outcome) = rest.split_once(", ")?;
            let outcome = outcome.trim_end_matches('.').trim();
            if outcome == "ongoing" {
                return None;
            }
            Some(
                Classified::new(event_name, Category::Disk)
                    .floor(if outcome == "success" { 0 } else { 13 })
                    .element("group:storage")
                    .attr("macos.disk.device", AttrValue::Str(device.into()))
                    .attr("macos.disk.outcome", AttrValue::Str(outcome.into())),
            )
        }
        "powerd" => {
            if let Some(rest) = message.strip_prefix("Battery percentage last ") {
                let (last, now) = rest.split_once(" now ")?;
                let last: i64 = last.trim().parse().ok()?;
                let now: i64 = now.trim().parse().ok()?;
                return Some(
                    Classified::new("macos.battery.level", Category::Power)
                        .element("power:battery:internal")
                        .attr("macos.battery.previous_percent", AttrValue::Int(last))
                        .attr("macos.battery.percent", AttrValue::Int(now)),
                );
            }
            if message.contains("Entering Sleep") {
                return Some(Classified::new("macos.power.sleep", Category::Power).element("group:power"));
            }
            if message.starts_with("Wake from") || message.contains("DarkWake from") {
                return Some(
                    Classified::new("macos.power.wake", Category::Power)
                        .element("group:power")
                        .attr("macos.power.dark_wake", AttrValue::Bool(message.contains("DarkWake"))),
                );
            }
            None
        }
        "kernel" if message.starts_with("Wake reason") => {
            Some(Classified::new("macos.power.wake_reason", Category::Power).element("group:power"))
        }
        "configd" => {
            if let Some(rest) = message.strip_prefix("Process interface link status ") {
                let (state, interface) = rest.split_once(": ")?;
                return Some(link_event(state == "active", interface.trim()));
            }
            if let Some((interface, state)) = message.split_once(" link ") {
                let interface = interface.trim();
                if !interface.is_empty() && !interface.contains(' ') {
                    match state.trim() {
                        "ACTIVE" => return Some(link_event(true, interface)),
                        "INACTIVE" => return Some(link_event(false, interface)),
                        _ => {}
                    }
                }
            }
            message
                .starts_with("network changed")
                .then(|| Classified::new("macos.network.changed", Category::Network).element("group:network"))
        }
        "airportd" if message.starts_with("[corewifi] AUTO-JOIN: Updated associated network") => Some(
            Classified::new("macos.wifi.associated", Category::Network).element("group:network"),
        ),
        "powerexperienced" => {
            let level = message.strip_prefix("kThermalPressureContext is now")?;
            let level: i64 = level.trim_start_matches([' ', ':']).trim().parse().ok()?;
            Some(
                Classified::new("macos.thermal.pressure", Category::Thermal)
                    .floor(if level > 0 { 13 } else { 0 })
                    .element("group:thermal")
                    .attr("macos.thermal.pressure_level", AttrValue::Int(level)),
            )
        }
        _ => None,
    }
}

fn link_event(up: bool, interface: &str) -> Classified {
    Classified::new(if up { "macos.network.link_up" } else { "macos.network.link_down" }, Category::Network)
        .element(format!("network:{interface}"))
        .attr("network.interface.name", AttrValue::Str(interface.into()))
}

impl Classified {
    fn with_process(mut self, pid: i64, name: &str) -> Self {
        self.process = Some((pid, name.into()));
        self
    }
}

/// What redaction replaced in one text.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Redactions {
    pub user_paths: u32,
    pub emails: u32,
    pub addresses: u32,
}

impl Redactions {
    pub fn total(&self) -> u32 {
        self.user_paths + self.emails + self.addresses
    }
}

/// Masks user home paths (`/Users/<name>` and `$HOME`), e-mail addresses,
/// IPv4 addresses, IPv6 and MAC addresses in `text`.
pub fn redact(text: &str, home: Option<&str>) -> (String, Redactions) {
    let mut counts = Redactions::default();
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    // User paths first: `/Users/<name>` (any user) and the Muse's own home.
    while let Some(at) = rest.find("/Users/") {
        out.push_str(&rest[..at]);
        let after = &rest[at + "/Users/".len()..];
        let end = after.find(|c: char| c == '/' || c.is_whitespace() || c == '"' || c == '\'').unwrap_or(after.len());
        if end == 0 || &after[..end] == "Shared" || &after[..end] == "<user>" {
            out.push_str(&rest[at..at + "/Users/".len() + end]);
        } else {
            out.push_str("/Users/<user>");
            counts.user_paths += 1;
        }
        rest = &after[end..];
    }
    out.push_str(rest);
    if let Some(home) = home.filter(|home| home.len() > 1 && !home.starts_with("/Users/")) {
        let replaced = out.matches(home).count() as u32;
        if replaced > 0 {
            out = out.replace(home, "<home>");
            counts.user_paths += replaced;
        }
    }
    // Then token by token: e-mail and network addresses.
    let mut result = String::with_capacity(out.len());
    let mut token = String::new();
    let flush = |token: &mut String, result: &mut String, counts: &mut Redactions| {
        if token.is_empty() {
            return;
        }
        let core = token.trim_matches(|c: char| matches!(c, '(' | ')' | '[' | ']' | '<' | '>' | ',' | ';' | '.' | '\'' | '"' | '{' | '}'));
        let replacement = if is_email(core) {
            counts.emails += 1;
            Some("<email>")
        } else if is_ipv4(core.split('/').next().unwrap_or(core)) || is_ipv6_or_mac(core) {
            counts.addresses += 1;
            Some("<address>")
        } else {
            None
        };
        match replacement {
            Some(replacement) if !core.is_empty() => result.push_str(&token.replacen(core, replacement, 1)),
            _ => result.push_str(&mask_embedded_ipv4(token, counts)),
        }
        token.clear();
    };
    for c in out.chars() {
        if c.is_whitespace() || c == '=' {
            flush(&mut token, &mut result, &mut counts);
            result.push(c);
        } else {
            token.push(c);
        }
    }
    flush(&mut token, &mut result, &mut counts);
    (result, counts)
}

/// Masks IPv4 addresses inside a longer token (`v4(en0:192.168.1.2)`).
fn mask_embedded_ipv4(token: &str, counts: &mut Redactions) -> String {
    let mut out = String::with_capacity(token.len());
    let mut run = String::new();
    let push_run = |run: &mut String, out: &mut String, counts: &mut Redactions| {
        let trimmed = run.trim_end_matches('.');
        if is_ipv4(trimmed) {
            out.push_str("<address>");
            out.push_str(&run[trimmed.len()..]);
            counts.addresses += 1;
        } else {
            out.push_str(run);
        }
        run.clear();
    };
    for c in token.chars() {
        if c.is_ascii_digit() || c == '.' {
            run.push(c);
        } else {
            push_run(&mut run, &mut out, counts);
            out.push(c);
        }
    }
    push_run(&mut run, &mut out, counts);
    out
}

fn is_email(token: &str) -> bool {
    let Some((local, domain)) = token.split_once('@') else { return false };
    !local.is_empty()
        && local.chars().all(|c| c.is_ascii_alphanumeric() || "._%+-".contains(c))
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && domain.chars().all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
}

fn is_ipv4(token: &str) -> bool {
    let parts = token.split('.').collect::<Vec<_>>();
    parts.len() == 4 && parts.iter().all(|part| !part.is_empty() && part.len() <= 3 && part.parse::<u8>().is_ok())
}

/// IPv6 (`::` or 5 colons and more) and MAC addresses; clock times such as
/// `17:50:33` stay.
fn is_ipv6_or_mac(token: &str) -> bool {
    let token = token.split('%').next().unwrap_or(token);
    let colons = token.matches(':').count();
    (colons >= 5 || (token.contains("::") && token.len() > 2))
        && token.chars().all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.')
        && token.chars().any(|c| c.is_ascii_hexdigit())
}

/// Cuts `text` to at most `max` bytes on a character boundary.
pub fn truncate(text: &mut String, max: usize) -> bool {
    if text.len() <= max {
        return false;
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    true
}

/// A token bucket: `rate` tokens per second up to `burst`.
#[derive(Clone, Debug)]
pub struct TokenBucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(rate: f64, burst: f64, now: Instant) -> Self {
        let burst = burst.max(1.0);
        Self { rate: rate.max(0.0), burst, tokens: burst, last: now }
    }

    /// Takes one token if available.
    pub fn take(&mut self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.last = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// One record ready to encode.
#[derive(Clone, Debug, PartialEq)]
pub struct OutRecord {
    pub time_unix_nano: u64,
    pub observed_unix_nano: u64,
    pub severity_number: u8,
    pub severity_text: String,
    pub body: String,
    /// Sorted by key (Poet's record identity hashes them as sent).
    pub attributes: Vec<(String, AttrValue)>,
}

/// Counters of the collector, reported as the Muse's own metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LogStats {
    /// Lines read from `log stream` (after the predicate).
    pub lines_read: u64,
    /// Lines the classifier did not keep (filtered at the Muse).
    pub filtered: u64,
    /// Lines skipped as unparsable or oversized.
    pub unparsable: u64,
    /// Records accepted into the queue.
    pub collected: u64,
    pub dropped_rate_limited: u64,
    pub dropped_queue_full: u64,
    /// Records a Poet refused (4xx); not retried.
    pub dropped_rejected: u64,
    pub sent: u64,
    pub send_failures: u64,
    pub truncated: u64,
    pub redacted: u64,
    pub stream_restarts: u64,
    /// Records waiting for a Poet now.
    pub queued: u64,
}

/// The classify, redact, bound and queue stage, shared by the reader thread
/// and the sender.
#[derive(Debug)]
pub struct LogBuffer {
    settings: LogSettings,
    home: Option<String>,
    total: TokenBucket,
    categories: HashMap<Category, TokenBucket>,
    queue: VecDeque<OutRecord>,
    stats: LogStats,
}

impl LogBuffer {
    pub fn new(settings: LogSettings, home: Option<String>, now: Instant) -> Self {
        Self {
            total: TokenBucket::new(settings.rate_per_second, settings.burst, now),
            settings,
            home,
            categories: HashMap::new(),
            queue: VecDeque::new(),
            stats: LogStats::default(),
        }
    }

    pub fn stats(&self) -> LogStats {
        LogStats { queued: self.queue.len() as u64, ..self.stats }
    }

    pub fn note_restart(&mut self) {
        self.stats.stream_restarts += 1;
    }

    /// Handles one ndjson line; `true` when a record was queued.
    pub fn ingest_line(&mut self, line: &str, now: Instant, observed_unix_nano: u64) -> bool {
        if line.len() > MAX_LINE_BYTES {
            self.stats.unparsable += 1;
            return false;
        }
        let Some(entry) = parse_line(line) else {
            if line.trim_start().starts_with('{') {
                self.stats.unparsable += 1;
            }
            return false;
        };
        self.stats.lines_read += 1;
        let Some(classified) = classify(&entry, self.settings.test_hook) else {
            self.stats.filtered += 1;
            return false;
        };
        let bucket = self
            .categories
            .entry(classified.category)
            .or_insert_with(|| TokenBucket::new(CATEGORY_RATE_PER_SECOND, CATEGORY_BURST, now));
        // A category over its own rate does not spend the shared budget.
        if !bucket.take(now) || !self.total.take(now) {
            self.stats.dropped_rate_limited += 1;
            return false;
        }
        let record = self.record(&entry, classified, observed_unix_nano);
        self.queue.push_back(record);
        self.stats.collected += 1;
        while self.queue.len() > MAX_QUEUED_RECORDS {
            self.queue.pop_front();
            self.stats.dropped_queue_full += 1;
        }
        true
    }

    fn record(&mut self, entry: &UnifiedLogEntry, classified: Classified, observed_unix_nano: u64) -> OutRecord {
        let mut body = entry.event_message.clone();
        if self.settings.redact {
            let (redacted, counts) = redact(&body, self.home.as_deref());
            if counts.total() > 0 {
                self.stats.redacted += 1;
                body = redacted;
            }
        }
        let truncated = truncate(&mut body, MAX_BODY_BYTES);
        if truncated {
            self.stats.truncated += 1;
        }
        let mut attributes = classified.attributes;
        attributes.push(("event.name".into(), AttrValue::Str(classified.event_name.into())));
        attributes.push(("macos.log.process".into(), AttrValue::Str(entry.process().into())));
        attributes.push(("macos.log.process_pid".into(), AttrValue::Int(entry.process_id)));
        if !entry.subsystem.is_empty() {
            attributes.push(("macos.log.subsystem".into(), AttrValue::Str(entry.subsystem.clone())));
        }
        if !entry.category.is_empty() {
            attributes.push(("macos.log.category".into(), AttrValue::Str(entry.category.clone())));
        }
        if let Some(key) = classified.element_key {
            attributes.push(("macos.element.key".into(), AttrValue::Str(key)));
        }
        if let Some((pid, name)) = classified.process {
            attributes.push(("process.pid".into(), AttrValue::Int(pid)));
            attributes.push(("process.executable.name".into(), AttrValue::Str(name)));
        }
        if truncated {
            attributes.push(("macos.log.body_truncated".into(), AttrValue::Bool(true)));
        }
        attributes.sort_by(|left, right| left.0.cmp(&right.0));
        attributes.dedup_by(|later, earlier| later.0 == earlier.0);
        OutRecord {
            time_unix_nano: parse_timestamp(&entry.timestamp).unwrap_or(observed_unix_nano),
            observed_unix_nano,
            severity_number: severity_number(&entry.message_type).max(classified.severity_floor),
            severity_text: if entry.message_type.is_empty() { "Default".into() } else { entry.message_type.clone() },
            body,
            attributes,
        }
    }

    /// The oldest queued records for one request (bounded by count and
    /// bytes), without removing them.
    pub fn peek_batch(&self) -> Vec<OutRecord> {
        let mut bytes = 0;
        let mut batch = Vec::new();
        for record in &self.queue {
            let size = approximate_size(record);
            if !batch.is_empty() && (batch.len() >= MAX_BATCH_RECORDS || bytes + size > MAX_BATCH_BYTES) {
                break;
            }
            bytes += size;
            batch.push(record.clone());
        }
        batch
    }

    /// The first `count` records were acknowledged (`sent`) or refused.
    pub fn complete(&mut self, count: usize, sent: bool) {
        let count = count.min(self.queue.len());
        self.queue.drain(..count);
        if sent {
            self.stats.sent += count as u64;
        } else {
            self.stats.dropped_rejected += count as u64;
        }
    }

    pub fn note_send_failure(&mut self) {
        self.stats.send_failures += 1;
    }
}

fn approximate_size(record: &OutRecord) -> usize {
    64 + record.body.len()
        + record.severity_text.len()
        + record.attributes.iter().map(|(key, value)| key.len() + 16 + match value {
            AttrValue::Str(text) => text.len(),
            _ => 8,
        }).sum::<usize>()
}

// --- OTLP protobuf encoding (opentelemetry.proto.collector.logs.v1) ---
//
// Only the fields this Muse sends, hand-encoded to keep the Muse free of a
// protobuf dependency. Field numbers follow the OTLP .proto files.

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn put_tag(out: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(out, (u64::from(field) << 3) | u64::from(wire));
}

fn put_bytes(out: &mut Vec<u8>, field: u32, bytes: &[u8]) {
    put_tag(out, field, 2);
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn put_fixed64(out: &mut Vec<u8>, field: u32, value: u64) {
    put_tag(out, field, 1);
    out.extend_from_slice(&value.to_le_bytes());
}

fn any_value(value: &AttrValue) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        AttrValue::Str(text) => put_bytes(&mut out, 1, text.as_bytes()),
        AttrValue::Bool(flag) => {
            put_tag(&mut out, 2, 0);
            put_varint(&mut out, u64::from(*flag));
        }
        AttrValue::Int(number) => {
            put_tag(&mut out, 3, 0);
            put_varint(&mut out, *number as u64);
        }
    }
    out
}

fn key_value(key: &str, value: &AttrValue) -> Vec<u8> {
    let mut out = Vec::new();
    put_bytes(&mut out, 1, key.as_bytes());
    put_bytes(&mut out, 2, &any_value(value));
    out
}

fn log_record(record: &OutRecord) -> Vec<u8> {
    let mut out = Vec::new();
    put_fixed64(&mut out, 1, record.time_unix_nano);
    if record.severity_number > 0 {
        put_tag(&mut out, 2, 0);
        put_varint(&mut out, u64::from(record.severity_number));
    }
    put_bytes(&mut out, 3, record.severity_text.as_bytes());
    put_bytes(&mut out, 5, &any_value(&AttrValue::Str(record.body.clone())));
    for (key, value) in &record.attributes {
        put_bytes(&mut out, 6, &key_value(key, value));
    }
    put_fixed64(&mut out, 11, record.observed_unix_nano);
    out
}

/// The resource attributes of every record: the Mac host (`host.name` is
/// the Muse's host id, the key Poet's element mapping resolves).
pub fn resource_attributes(host_name: &str) -> Vec<(String, AttrValue)> {
    vec![
        ("host.name".into(), AttrValue::Str(host_name.into())),
        ("os.type".into(), AttrValue::Str("darwin".into())),
        ("service.name".into(), AttrValue::Str("ih-muse-macos".into())),
        ("telemetry.source".into(), AttrValue::Str("macos.unified_log".into())),
    ]
}

/// An `ExportLogsServiceRequest` holding `records` under one resource and
/// one instrumentation scope.
pub fn encode_request(resource: &[(String, AttrValue)], records: &[OutRecord]) -> Vec<u8> {
    let mut resource_message = Vec::new();
    for (key, value) in resource {
        put_bytes(&mut resource_message, 1, &key_value(key, value));
    }
    let mut scope = Vec::new();
    put_bytes(&mut scope, 1, b"ih-muse-macos.unified-log");
    put_bytes(&mut scope, 2, env!("CARGO_PKG_VERSION").as_bytes());
    let mut scope_logs = Vec::new();
    put_bytes(&mut scope_logs, 1, &scope);
    for record in records {
        put_bytes(&mut scope_logs, 2, &log_record(record));
    }
    let mut resource_logs = Vec::new();
    put_bytes(&mut resource_logs, 1, &resource_message);
    put_bytes(&mut resource_logs, 2, &scope_logs);
    let mut request = Vec::new();
    put_bytes(&mut request, 1, &resource_logs);
    request
}

// --- Collection and delivery ---

fn now_unix_nano() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_nanos() as u64)
}

/// Starts `log stream` under a small shell watchdog that stops it when this
/// Muse exits (however it exits), and feeds its lines to `buffer` on a
/// background thread. The stream is restarted after it ends.
pub fn spawn_collector(buffer: Arc<Mutex<LogBuffer>>, test_hook: bool) {
    let predicate = predicate(test_hook);
    std::thread::Builder::new()
        .name("unified-log".into())
        .spawn(move || loop {
            match start_stream(&predicate) {
                Ok(mut child) => {
                    if let Some(stdout) = child.stdout.take() {
                        let mut reader = BufReader::new(stdout);
                        let mut line = String::new();
                        loop {
                            line.clear();
                            match reader.read_line(&mut line) {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {
                                    let mut buffer = buffer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                    buffer.ingest_line(&line, Instant::now(), now_unix_nano());
                                }
                            }
                        }
                    }
                    let _ = child.kill();
                    let _ = child.wait();
                }
                Err(error) => eprintln!("unified log collection could not start: {error}"),
            }
            buffer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).note_restart();
            std::thread::sleep(Duration::from_secs(5));
        })
        .expect("spawn the unified log thread");
}

/// The watchdog script: `$1` is the predicate; `$PPID` is this Muse.
const WATCHDOG: &str = r#"/usr/bin/log stream --style ndjson --predicate "$1" &
L=$!
trap 'kill $L 2>/dev/null' EXIT INT TERM HUP
while kill -0 "$PPID" 2>/dev/null && kill -0 "$L" 2>/dev/null; do sleep 2; done"#;

fn start_stream(predicate: &str) -> std::io::Result<Child> {
    Command::new("/bin/sh")
        .args(["-c", WATCHDOG, "ih-muse-unified-log", predicate])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
}

/// Outcome of one delivery attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    Rejected,
    Unavailable,
}

/// Sends queued records to the Poets of the cluster: the same Poets, token
/// and failover order as the graph batches, starting at the Poet that last
/// accepted a graph batch (`preferred`).
pub struct LogSender {
    endpoints: Vec<String>,
    token: String,
    resource: Vec<(String, AttrValue)>,
    client: reqwest::Client,
}

impl LogSender {
    pub fn new(endpoints: &[String], token: &str, host_name: &str) -> reqwest::Result<Self> {
        Ok(Self {
            endpoints: endpoints
                .iter()
                .map(|endpoint| endpoint.trim().trim_end_matches('/').to_owned())
                .filter(|endpoint| !endpoint.is_empty())
                .collect(),
            token: token.into(),
            resource: resource_attributes(host_name),
            client: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
        })
    }

    /// Sends one batch from `buffer`, if any: 2xx removes it, 4xx drops it
    /// (another Poet would refuse it too), anything else keeps it queued.
    pub async fn send_once(&self, buffer: &Mutex<LogBuffer>, preferred: &str) -> Option<Delivery> {
        let batch = buffer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).peek_batch();
        if batch.is_empty() {
            return None;
        }
        let body = encode_request(&self.resource, &batch);
        let start = self.endpoints.iter().position(|endpoint| endpoint == preferred.trim_end_matches('/')).unwrap_or(0);
        let mut outcome = Delivery::Unavailable;
        for offset in 0..self.endpoints.len() {
            let endpoint = &self.endpoints[(start + offset) % self.endpoints.len()];
            match self
                .client
                .post(format!("{endpoint}/v1/logs"))
                .bearer_auth(&self.token)
                .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
                .body(body.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    outcome = Delivery::Sent;
                    break;
                }
                Ok(response) if response.status().is_client_error() => {
                    eprintln!("Poet refused {} log record(s): HTTP {}", batch.len(), response.status());
                    outcome = Delivery::Rejected;
                    break;
                }
                _ => {}
            }
        }
        let mut buffer = buffer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match outcome {
            Delivery::Sent => buffer.complete(batch.len(), true),
            Delivery::Rejected => buffer.complete(batch.len(), false),
            Delivery::Unavailable => buffer.note_send_failure(),
        }
        Some(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOGGER: &str = r#"{"timezoneName":"","messageType":"Default","eventType":"logEvent","source":null,"formatString":"%s","userID":501,"activityIdentifier":0,"subsystem":"","category":"","threadID":139361863,"senderImageUUID":"72E310F7-1A63-3C21-926C-BEF6061890C0","backtrace":{"frames":[{"imageOffset":1764,"imageUUID":"72E310F7-1A63-3C21-926C-BEF6061890C0"}]},"bootUUID":"","processImagePath":"\/usr\/bin\/logger","senderImagePath":"\/usr\/bin\/logger","timestamp":"2026-10-04 17:50:33.803491+0200","machTimestamp":23247436516259,"eventMessage":"ih-muse-test hello 1","processImageUUID":"72E310F7-1A63-3C21-926C-BEF6061890C0","traceID":13353070231556,"processID":21314,"senderProgramCounter":1764,"parentActivityIdentifier":0}"#;
    const CRASH: &str = r#"{"timezoneName":"","messageType":"Default","eventType":"logEvent","source":null,"formatString":"Formulating %{public}s 309 report for %s[%d] %{public}@","userID":501,"subsystem":"","category":"","processImagePath":"\/System\/Library\/CoreServices\/ReportCrash","senderImagePath":"\/System\/Library\/CoreServices\/ReportCrash","timestamp":"2026-10-04 17:59:28.555652+0200","eventMessage":"Formulating fatal 309 report for corpse[62576] ihmusecrash","traceID":1277701231083524,"processID":12264}"#;

    fn entry(process: &str, message: &str) -> UnifiedLogEntry {
        UnifiedLogEntry {
            message_type: "Default".into(),
            process_image_path: format!("/usr/libexec/{process}"),
            event_message: message.into(),
            timestamp: "2026-10-04 17:50:33.803491+0200".into(),
            process_id: 370,
            ..Default::default()
        }
    }

    fn buffer(test_hook: bool) -> LogBuffer {
        LogBuffer::new(LogSettings { test_hook, ..LogSettings::default() }, Some("/Users/alice".into()), Instant::now())
    }

    #[test]
    fn parses_real_ndjson_lines_and_skips_the_banner() {
        let parsed = parse_line(LOGGER).expect("a logger line");
        assert_eq!(parsed.process(), "logger");
        assert_eq!(parsed.event_message, "ih-muse-test hello 1");
        assert_eq!(parsed.process_id, 21314);
        assert!(parse_line(r#"Filtering the log data using "process == \"logger\"""#).is_none());
        assert!(parse_line("").is_none());
        assert!(parse_line("{not json").is_none());
        assert!(parse_line(r#"{"eventType":"activityCreateEvent","eventMessage":"x"}"#).is_none());
    }

    #[test]
    fn timestamps_honour_the_offset() {
        // 17:50:33.803491 at +02:00 is 15:50:33.803491 UTC.
        assert_eq!(parse_timestamp("2026-10-04 17:50:33.803491+0200"), Some(1_791_129_033_803_491_000));
        assert_eq!(parse_timestamp("2026-10-04 15:50:33.803491+0000"), Some(1_791_129_033_803_491_000));
        assert_eq!(parse_timestamp("2026-10-04 10:50:33.803491-0500"), Some(1_791_129_033_803_491_000));
        assert_eq!(parse_timestamp("garbage"), None);
    }

    #[test]
    fn severity_follows_the_unified_log_level_with_floors_for_crashes() {
        assert_eq!(severity_number("Debug"), 5);
        assert_eq!(severity_number("Info"), 9);
        assert_eq!(severity_number("Default"), 10);
        assert_eq!(severity_number("Error"), 17);
        assert_eq!(severity_number("Fault"), 18);
        let mut buffer = buffer(false);
        assert!(buffer.ingest_line(CRASH, Instant::now(), 1));
        let record = &buffer.peek_batch()[0];
        assert_eq!(record.severity_number, 17, "a fatal crash logged at Default is an error");
        assert_eq!(record.severity_text, "Default");
    }

    #[test]
    fn crashes_name_the_crashed_process_for_element_mapping() {
        let classified = classify(&parse_line(CRASH).unwrap(), false).unwrap();
        assert_eq!(classified.event_name, "macos.process.crash");
        assert_eq!(classified.process, Some((62576, "ihmusecrash".into())));
        let mut buffer = buffer(false);
        buffer.ingest_line(CRASH, Instant::now(), 1);
        let attributes = &buffer.peek_batch()[0].attributes;
        assert!(attributes.contains(&("process.pid".into(), AttrValue::Int(62576))));
        assert!(attributes.contains(&("process.executable.name".into(), AttrValue::Str("ihmusecrash".into()))));
        assert!(attributes.contains(&("macos.log.process_pid".into(), AttrValue::Int(12264))), "ReportCrash's own pid is not process.pid");
        assert!(attributes.windows(2).all(|pair| pair[0].0 < pair[1].0), "sorted, unique keys");
    }

    #[test]
    fn recognises_each_system_event() {
        let cases = [
            ("diskarbitrationd", "mounted disk, id = /dev/disk4s1, success.", Some("macos.disk.mounted")),
            ("diskarbitrationd", "unmounted disk, id = /dev/disk4s1, success.", Some("macos.disk.unmounted")),
            ("diskarbitrationd", "ejected disk, id = /dev/disk4, success.", Some("macos.disk.ejected")),
            ("diskarbitrationd", "mounted disk, id = /dev/disk4s1, ongoing.", None),
            ("diskarbitrationd", "created disk, id = /dev/disk4.", None),
            ("powerd", "Battery percentage last 80 now 79", Some("macos.battery.level")),
            ("powerd", "Entering Sleep state due to 'Clamshell Sleep'", Some("macos.power.sleep")),
            ("powerd", "Wake from Deep Idle [CDNVA] : due to NUB.SPMI0Sw3IRQ", Some("macos.power.wake")),
            ("powerd", "Sleep revert state: 1", None),
            ("kernel", "Wake reason: NUB.SPMI0Sw3IRQ", Some("macos.power.wake_reason")),
            ("configd", "Process interface link status inactive: en15", Some("macos.network.link_down")),
            ("configd", "Process interface link status active: en0", Some("macos.network.link_up")),
            ("configd", "en15 link ACTIVE", Some("macos.network.link_up")),
            ("configd", "network changed: v4(en0:192.168.100.26) DNS* Proxy SMB", Some("macos.network.changed")),
            ("configd", "LINKLOCAL en15: publish success { IPv4 }", None),
            ("airportd", "[corewifi] AUTO-JOIN: Updated associated network (<redacted> - ssid=<redacted>)", Some("macos.wifi.associated")),
            ("powerexperienced", "kThermalPressureContext is now : 2", Some("macos.thermal.pressure")),
            ("logger", "ih-muse-test token", None),
            ("Finder", "mounted disk, id = /dev/disk4s1, success.", None),
        ];
        for (process, message, expected) in cases {
            assert_eq!(classify(&entry(process, message), false).map(|c| c.event_name), expected, "{process}: {message}");
        }
        let link = classify(&entry("configd", "en15 link ACTIVE"), false).unwrap();
        assert_eq!(link.element_key.as_deref(), Some("network:en15"));
        let thermal = classify(&entry("powerexperienced", "kThermalPressureContext is now : 2"), false).unwrap();
        assert_eq!(thermal.severity_floor, 13, "thermal pressure above nominal is a warning");
        assert_eq!(classify(&entry("logger", "ih-muse-test token"), true).unwrap().event_name, "macos.test.line");
    }

    #[test]
    fn the_test_clause_is_only_in_the_predicate_when_asked() {
        assert!(!predicate(false).contains("logger"));
        assert!(predicate(true).contains(r#"process == "logger" AND eventMessage BEGINSWITH "ih-muse-test""#));
        assert!(predicate(false).contains("diskarbitrationd"));
    }

    #[test]
    fn redacts_user_paths_emails_and_addresses() {
        let (text, counts) = redact(
            "crash in /Users/bob/Library/x and /Users/Shared/y, mail bob.smith@example.com, v4(en0:192.168.100.26) mac 3c:22:fb:01:02:03 fe80::1%en0 at 17:50:33",
            Some("/Users/bob"),
        );
        assert_eq!(
            text,
            "crash in /Users/<user>/Library/x and /Users/Shared/y, mail <email>, v4(en0:<address>) mac <address> <address> at 17:50:33"
        );
        assert_eq!(counts, Redactions { user_paths: 1, emails: 1, addresses: 3 });
        let (plain, counts) = redact("mounted disk, id = /dev/disk4s1, success.", None);
        assert_eq!(plain, "mounted disk, id = /dev/disk4s1, success.");
        assert_eq!(counts.total(), 0);
        let (home, counts) = redact("file /private/var/root/x", Some("/private/var/root"));
        assert_eq!((home.as_str(), counts.user_paths), ("file <home>/x", 1));
    }

    #[test]
    fn redaction_is_on_by_default_and_can_be_turned_off() {
        let line = LOGGER.replace("ih-muse-test hello 1", "ih-muse-test /Users/alice/secret me@example.org");
        let mut redacting = buffer(true);
        redacting.ingest_line(&line, Instant::now(), 1);
        assert_eq!(redacting.peek_batch()[0].body, "ih-muse-test /Users/<user>/secret <email>");
        assert_eq!(redacting.stats().redacted, 1);
        let mut plain = LogBuffer::new(LogSettings { redact: false, test_hook: true, ..LogSettings::default() }, None, Instant::now());
        plain.ingest_line(&line, Instant::now(), 1);
        assert_eq!(plain.peek_batch()[0].body, "ih-muse-test /Users/alice/secret me@example.org");
    }

    #[test]
    fn bounds_rate_size_and_queue_and_counts_every_drop() {
        let start = Instant::now();
        let mut buffer = LogBuffer::new(LogSettings { test_hook: true, rate_per_second: 1.0, burst: 10.0, ..LogSettings::default() }, None, start);
        for _ in 0..30 {
            buffer.ingest_line(LOGGER, start, 1);
        }
        let stats = buffer.stats();
        assert_eq!((stats.collected, stats.dropped_rate_limited, stats.queued), (10, 20, 10), "burst of 10, then limited");
        // A noisy category cannot spend more than its own burst.
        let mut fair = LogBuffer::new(LogSettings { test_hook: true, ..LogSettings::default() }, None, start);
        for _ in 0..100 {
            fair.ingest_line(LOGGER, start, 1);
        }
        assert!(fair.ingest_line(CRASH, start, 1), "another category still gets through");
        assert_eq!(fair.stats().collected, CATEGORY_BURST as u64 + 1);

        // Time refills the bucket.
        assert!(buffer.ingest_line(LOGGER, start + Duration::from_secs(2), 1));

        // Body size limit.
        let long = LOGGER.replace("ih-muse-test hello 1", &format!("ih-muse-test {}", "é".repeat(3000)));
        let mut sized = LogBuffer::new(LogSettings { test_hook: true, ..LogSettings::default() }, None, start);
        sized.ingest_line(&long, start, 1);
        let record = &sized.peek_batch()[0];
        assert!(record.body.len() <= MAX_BODY_BYTES && record.body.is_char_boundary(record.body.len()));
        assert!(record.attributes.contains(&("macos.log.body_truncated".into(), AttrValue::Bool(true))));
        assert_eq!(sized.stats().truncated, 1);
        // Oversized and unparsable lines.
        sized.ingest_line(&"x".repeat(MAX_LINE_BYTES + 1), start, 1);
        sized.ingest_line("{broken", start, 1);
        assert_eq!(sized.stats().unparsable, 2);
        // Lines that pass the predicate but no rule are filtered at the Muse.
        sized.ingest_line(&LOGGER.replace("ih-muse-test hello 1", "unrelated"), start, 1);
        assert_eq!(sized.stats().filtered, 1);

        // Queue bound: the oldest go first.
        let mut queue = LogBuffer::new(LogSettings { test_hook: true, rate_per_second: 1e9, burst: 1e9, ..LogSettings::default() }, None, start);
        let mut at = start;
        for _ in 0..(MAX_QUEUED_RECORDS + 25) {
            at += Duration::from_secs(1);
            queue.ingest_line(LOGGER, at, 1);
        }
        assert_eq!(queue.stats().queued, MAX_QUEUED_RECORDS as u64);
        assert_eq!(queue.stats().dropped_queue_full, 25);
        // Batches are bounded and stay queued until acknowledged.
        let batch = queue.peek_batch();
        assert_eq!(batch.len(), MAX_BATCH_RECORDS);
        queue.complete(batch.len(), true);
        queue.complete(10, false);
        let stats = queue.stats();
        assert_eq!((stats.sent, stats.dropped_rejected, stats.queued), (500, 10, (MAX_QUEUED_RECORDS - 510) as u64));
    }

    /// A minimal protobuf reader for the encoding test: (field, wire, bytes or value).
    fn fields(mut bytes: &[u8]) -> Vec<(u32, u8, Vec<u8>, u64)> {
        fn varint(bytes: &mut &[u8]) -> u64 {
            let mut value = 0;
            let mut shift = 0;
            loop {
                let byte = bytes[0];
                *bytes = &bytes[1..];
                value |= u64::from(byte & 0x7f) << shift;
                if byte < 0x80 {
                    return value;
                }
                shift += 7;
            }
        }
        let mut out = Vec::new();
        while !bytes.is_empty() {
            let tag = varint(&mut bytes);
            let (field, wire) = ((tag >> 3) as u32, (tag & 7) as u8);
            match wire {
                0 => out.push((field, wire, Vec::new(), varint(&mut bytes))),
                1 => {
                    out.push((field, wire, Vec::new(), u64::from_le_bytes(bytes[..8].try_into().unwrap())));
                    bytes = &bytes[8..];
                }
                2 => {
                    let len = varint(&mut bytes) as usize;
                    out.push((field, wire, bytes[..len].to_vec(), 0));
                    bytes = &bytes[len..];
                }
                _ => panic!("unexpected wire type {wire}"),
            }
        }
        out
    }

    #[test]
    fn encodes_an_otlp_logs_request_poet_can_map_to_the_host() {
        let mut buffer = buffer(true);
        buffer.ingest_line(LOGGER, Instant::now(), 7);
        buffer.ingest_line(CRASH, Instant::now(), 8);
        let records = buffer.peek_batch();
        let bytes = encode_request(&resource_attributes("macbook"), &records);
        let request = fields(&bytes);
        assert_eq!(request.len(), 1);
        let resource_logs = fields(&request[0].2);
        let resource = fields(&resource_logs[0].2);
        let first_attribute = fields(&resource[0].2);
        assert_eq!(first_attribute[0].2, b"host.name");
        assert_eq!(fields(&first_attribute[1].2)[0].2, b"macbook");
        let scope_logs = fields(&resource_logs[1].2);
        assert_eq!(fields(&scope_logs[0].2)[0].2, b"ih-muse-macos.unified-log");
        let logs = scope_logs.iter().filter(|field| field.0 == 2).collect::<Vec<_>>();
        assert_eq!(logs.len(), 2);
        let first = fields(&logs[0].2);
        assert_eq!((first[0].0, first[0].3), (1, 1_791_129_033_803_491_000), "time_unix_nano");
        assert_eq!((first[1].0, first[1].3), (2, 10), "severity number");
        assert_eq!((first[2].0, first[2].2.as_slice()), (3, b"Default".as_slice()));
        assert_eq!(fields(&first[3].2)[0].2, b"ih-muse-test hello 1", "body");
        assert_eq!(first.last().map(|field| (field.0, field.3)), Some((11, 7)), "observed time");
        let crash = fields(&logs[1].2);
        let pid = crash
            .iter()
            .filter(|field| field.0 == 6)
            .map(|field| fields(&field.2))
            .find(|kv| kv[0].2 == b"process.pid")
            .expect("process.pid attribute");
        assert_eq!(fields(&pid[1].2)[0].3, 62576);
    }
}
