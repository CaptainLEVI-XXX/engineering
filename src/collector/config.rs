//! Operator configuration file (YAML), mapped onto the components' own
//! `Config` structs. Precedence: built-in defaults < file < CLI overrides.
//!
//! The file schema is owned here, not by the components: units are explicit
//! (`*_ms`), unknown keys are rejected, and values the collector must derive
//! for consistency (sessions, selection, snapshot cadence, decoder message
//! bytes) are recorded with the effective config but cannot be set.
use crate::{orderbook, recording, websocket};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Challenge contract: every initialized live book is snapshotted each minute.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);
/// SBE snapshot groups carry a u16 level count; a larger side cannot be recorded.
const MAX_LEVELS_PER_SIDE: usize = u16::MAX as usize;

fn ms(value: Duration) -> u64 {
    u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorConfig {
    pub run: RunConfig,
    pub transport: TransportConfig,
    pub books: BookConfig,
    pub recording: RecordingConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunConfig {
    pub gamma_url: String,
    /// Bounded ingress between socket workers and the book owner.
    pub queue_items: usize,
    pub queue_bytes: usize,
}
impl Default for RunConfig {
    fn default() -> Self {
        Self {
            gamma_url: crate::discovery::polymarket::GAMMA_MARKETS.to_owned(),
            // Live full-universe bursts exceeded 23k envelopes and drained
            // promptly. 64 MiB is burst headroom, not a speed setting.
            queue_items: 65_536,
            queue_bytes: 64 << 20,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportConfig {
    pub endpoint: String,
    pub assets_per_socket: usize,
    pub max_sockets: usize,
    pub replicas_per_market: u16,
    pub lifecycle_hints: bool,
    pub subscription_batch: usize,
    pub max_message_bytes: usize,
    pub io_timeout_ms: u64,
    pub heartbeat_ms: u64,
    pub pong_timeout_ms: u64,
    pub reconnect_min_ms: u64,
    pub reconnect_max_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub connection_concurrency: usize,
    pub connection_interval_ms: u64,
    pub initialization_timeout_ms: u64,
}
impl Default for TransportConfig {
    fn default() -> Self {
        let c = websocket::Config::default();
        Self {
            endpoint: c.endpoint,
            assets_per_socket: c.assets_per_socket,
            max_sockets: c.max_sockets,
            replicas_per_market: c.replicas_per_market,
            lifecycle_hints: c.lifecycle_hints,
            subscription_batch: c.subscription_batch,
            max_message_bytes: c.max_message_bytes,
            io_timeout_ms: ms(c.io_timeout),
            heartbeat_ms: ms(c.heartbeat),
            pong_timeout_ms: ms(c.pong_timeout),
            reconnect_min_ms: ms(c.reconnect_min),
            reconnect_max_ms: ms(c.reconnect_max),
            shutdown_timeout_ms: ms(c.shutdown_timeout),
            connection_concurrency: c.connection_concurrency,
            connection_interval_ms: ms(c.connection_interval),
            initialization_timeout_ms: ms(c.initialization_timeout),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BookConfig {
    pub monitoring: bool,
    pub renewal_interval_ms: u64,
    pub instruments: usize,
    pub replicas: usize,
    /// Per-book side limit; a deeper exchange book is quarantined, not truncated.
    pub levels_per_side: usize,
    /// Memory bound on price levels across every replica, including the
    /// transient old+new peak while a snapshot replaces a book.
    pub total_levels: usize,
    pub initialization_timeout_ms: u64,
    pub recovery_timeout_ms: u64,
    pub recovery_attempts: u32,
    pub wire_events: usize,
    pub wire_entries: usize,
    pub identity_bytes: usize,
}
impl Default for BookConfig {
    fn default() -> Self {
        let c = orderbook::Config::default();
        Self {
            monitoring: c.monitoring,
            renewal_interval_ms: ms(c.renewal_interval),
            instruments: c.instruments,
            replicas: c.replicas,
            levels_per_side: c.levels_per_side,
            total_levels: c.total_levels,
            initialization_timeout_ms: ms(c.initialization_timeout),
            recovery_timeout_ms: ms(c.recovery_timeout),
            recovery_attempts: c.recovery_attempts,
            wire_events: c.wire.events,
            wire_entries: c.wire.entries,
            identity_bytes: c.wire.identity_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecordingConfig {
    pub batches: usize,
    pub batch_bytes: usize,
    pub entries: usize,
    pub publish_bytes: usize,
    pub publish_age_ms: u64,
    pub buffer_bytes: usize,
    pub flush_age_ms: u64,
    pub chunk_age_ms: u64,
    /// Opt-in stage latency rows in the integrity sidecar (never stdout).
    pub latency_histograms: bool,
}
impl Default for RecordingConfig {
    fn default() -> Self {
        let c = recording::Config::default();
        Self {
            batches: c.batches,
            batch_bytes: c.batch_bytes,
            entries: c.entries,
            publish_bytes: c.publish_bytes,
            publish_age_ms: ms(c.publish_age),
            buffer_bytes: c.buffer_bytes,
            flush_age_ms: ms(c.flush_age),
            chunk_age_ms: ms(c.chunk_age),
            latency_histograms: c.latency_histograms,
        }
    }
}

impl CollectorConfig {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("read config {path}"))?;
        Self::parse(&text).with_context(|| format!("invalid config {path}"))
    }

    pub fn parse(text: &str) -> Result<Self> {
        Ok(serde_yaml_ng::from_str(text)?)
    }

    pub fn transport_config(&self) -> websocket::Config {
        let t = &self.transport;
        let d = Duration::from_millis;
        websocket::Config {
            endpoint: t.endpoint.clone(),
            assets_per_socket: t.assets_per_socket,
            max_sockets: t.max_sockets,
            replicas_per_market: t.replicas_per_market,
            lifecycle_hints: t.lifecycle_hints,
            subscription_batch: t.subscription_batch,
            max_message_bytes: t.max_message_bytes,
            io_timeout: d(t.io_timeout_ms),
            heartbeat: d(t.heartbeat_ms),
            pong_timeout: d(t.pong_timeout_ms),
            reconnect_min: d(t.reconnect_min_ms),
            reconnect_max: d(t.reconnect_max_ms),
            shutdown_timeout: d(t.shutdown_timeout_ms),
            connection_concurrency: t.connection_concurrency,
            connection_interval: d(t.connection_interval_ms),
            initialization_timeout: d(t.initialization_timeout_ms),
        }
    }

    /// Book-owner config. Fields the collector derives are set here only, so
    /// they cannot drift from the transport and recording settings.
    pub fn book_config(&self, transport_only: bool) -> orderbook::Config {
        let (b, t) = (&self.books, &self.transport);
        let d = Duration::from_millis;
        orderbook::Config {
            wire: crate::market_data::Limits {
                // The decoder may not accept more than the transport admits.
                message_bytes: t.max_message_bytes,
                events: b.wire_events,
                entries: b.wire_entries,
                identity_bytes: b.identity_bytes,
            },
            selection: if t.replicas_per_market == 2 || b.monitoring {
                orderbook::SelectionPolicy::Automatic
            } else {
                orderbook::SelectionPolicy::Manual
            },
            monitoring: b.monitoring,
            renewal_interval: d(b.renewal_interval_ms),
            instruments: b.instruments,
            replicas: b.replicas,
            sessions: t.max_sockets,
            levels_per_side: b.levels_per_side,
            total_levels: b.total_levels,
            initialization_timeout: d(b.initialization_timeout_ms),
            recovery_timeout: d(b.recovery_timeout_ms),
            recovery_attempts: b.recovery_attempts,
            snapshot_interval: (!transport_only).then_some(SNAPSHOT_INTERVAL),
        }
    }

    pub fn recording_config(&self) -> recording::Config {
        let r = &self.recording;
        let d = Duration::from_millis;
        recording::Config {
            batches: r.batches,
            batch_bytes: r.batch_bytes,
            entries: r.entries,
            publish_bytes: r.publish_bytes,
            publish_age: d(r.publish_age_ms),
            buffer_bytes: r.buffer_bytes,
            flush_age: d(r.flush_age_ms),
            chunk_age: d(r.chunk_age_ms),
            latency_histograms: r.latency_histograms,
        }
    }

    /// Checks owned by this file layer. Component constructors still validate
    /// their own invariants (socket budgets, recorder batch fit, ...).
    pub fn validate(&self) -> Result<()> {
        self.transport_config().validate()?;
        ensure!(
            (1..=MAX_LEVELS_PER_SIDE).contains(&self.books.levels_per_side),
            "books.levels_per_side must be 1..={MAX_LEVELS_PER_SIDE} (SBE u16 group count)"
        );
        ensure!(
            self.books.total_levels > 0,
            "books.total_levels must be positive"
        );
        Ok(())
    }

    /// Everything that shaped this run, including derived values, for the
    /// integrity sidecar. A capture is reproducible from this row alone.
    pub fn effective(&self, transport_only: bool) -> serde_json::Value {
        let books = self.book_config(transport_only);
        serde_json::json!({
            "kind": "config",
            "config": self,
            "derived": {
                "sessions": books.sessions,
                "selection": format!("{:?}", books.selection),
                "snapshot_interval_ms": books.snapshot_interval.map(ms),
                "wire_message_bytes": books.wire.message_bytes,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_map_exactly_onto_component_defaults() {
        let c = CollectorConfig::default();
        // Component Configs have no PartialEq; Debug covers every field.
        assert_eq!(
            format!("{:?}", c.transport_config()),
            format!("{:?}", websocket::Config::default())
        );
        assert_eq!(
            format!("{:?}", c.recording_config()),
            format!("{:?}", recording::Config::default())
        );
        let books = orderbook::Config {
            snapshot_interval: Some(SNAPSHOT_INTERVAL),
            sessions: websocket::Config::default().max_sockets,
            wire: crate::market_data::Limits {
                message_bytes: websocket::Config::default().max_message_bytes,
                ..Default::default()
            },
            ..orderbook::Config::default()
        };
        assert_eq!(format!("{:?}", c.book_config(false)), format!("{books:?}"));
        assert_eq!(c.book_config(true).snapshot_interval, None);
        c.validate().unwrap();
    }

    #[test]
    fn partial_file_keeps_defaults_and_derives_consistent_values() {
        let c = CollectorConfig::parse(
            "books:\n  total_levels: 120000000\n  monitoring: true\ntransport:\n  max_sockets: 900\n  max_message_bytes: 4194304\n",
        )
        .unwrap();
        assert_eq!(c.books.total_levels, 120_000_000);
        assert_eq!(
            c.books.levels_per_side,
            BookConfig::default().levels_per_side
        );
        let books = c.book_config(false);
        assert_eq!(books.sessions, 900);
        assert_eq!(books.selection, orderbook::SelectionPolicy::Automatic);
        assert_eq!(books.wire.message_bytes, 4 << 20);
        let row = c.effective(false);
        assert_eq!(row["config"]["books"]["total_levels"], 120_000_000);
        assert_eq!(row["derived"]["sessions"], 900);
        assert_eq!(row["derived"]["snapshot_interval_ms"], 60_000);
    }

    #[test]
    fn rejects_unknown_keys_wrong_types_and_unrecordable_limits() {
        for (text, expected) in [
            ("books:\n  total_level: 5\n", "unknown field `total_level`"),
            ("bogus: 1\n", "unknown field `bogus`"),
            ("books:\n  total_levels: lots\n", "invalid type"),
            (
                "books:\n  total_levels: -1\n",
                "books.total_levels: invalid type",
            ),
        ] {
            let error = CollectorConfig::parse(text).unwrap_err().to_string();
            assert!(error.contains(expected), "{text:?}: {error}");
        }
        for (text, expected) in [
            ("books:\n  levels_per_side: 65536\n", "SBE u16"),
            ("books:\n  levels_per_side: 0\n", "SBE u16"),
            ("books:\n  total_levels: 0\n", "must be positive"),
            (
                "transport:\n  replicas_per_market: 3\n",
                "replicas_per_market",
            ),
        ] {
            let error = CollectorConfig::parse(text)
                .unwrap()
                .validate()
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{text:?}: {error}");
        }
    }

    #[test]
    fn shipped_example_is_valid_and_states_the_defaults() {
        let text = include_str!("../../collector.example.yaml");
        let example = CollectorConfig::parse(text).unwrap();
        assert_eq!(
            example,
            CollectorConfig::default(),
            "example drifted from defaults"
        );
        // Every field is spelled out, so the example documents the schema.
        assert_eq!(
            serde_yaml_ng::to_string(&example).unwrap().lines().count(),
            text.lines()
                .filter(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'))
                .count()
        );
    }
}
