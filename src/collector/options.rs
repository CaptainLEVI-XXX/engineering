//! Command-line options: parsed and validated before any task starts.
use crate::{discovery::polymarket::GAMMA_MARKETS, websocket::Config};
use anyhow::{Context, Result, ensure};

// Live full-universe bursts exceeded 23k envelopes and drained promptly.
// Keep independent byte accounting at 64 MiB; this is burst headroom, not speed.
pub const DEFAULT_QUEUE_ITEMS: usize = 65_536;
const DEFAULT_QUEUE_BYTES: usize = 64 << 20;

const USAGE: &str = "options require a value: --seconds N --gamma-url URL --ws-url URL --assets-per-socket N --max-sockets N --queue-items N --queue-bytes N";

#[derive(Debug)]
pub struct Options {
    pub transport: Config,
    pub gamma: String,
    /// Bounded run length; `None` runs until SIGINT.
    pub seconds: Option<u64>,
    pub queue_items: usize,
    pub queue_bytes: usize,
    /// Count and discard raw messages; nothing is written to stdout.
    pub transport_only: bool,
    pub monitoring: bool,
    /// Create-new integrity sidecar; required unless `transport_only`.
    pub integrity_path: Option<String>,
}

impl Options {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut options = Self {
            transport: Config::default(),
            gamma: GAMMA_MARKETS.to_owned(),
            seconds: None,
            queue_items: DEFAULT_QUEUE_ITEMS,
            queue_bytes: DEFAULT_QUEUE_BYTES,
            transport_only: false,
            monitoring: false,
            integrity_path: None,
        };
        let config = &mut options.transport;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            let value = args.next().context(USAGE)?;
            match arg.as_str() {
                "--seconds" => options.seconds = Some(value.parse::<u64>()?),
                "--gamma-url" => options.gamma = value,
                "--ws-url" => config.endpoint = value,
                "--assets-per-socket" => config.assets_per_socket = value.parse()?,
                "--max-sockets" => config.max_sockets = value.parse()?,
                "--replicas-per-market" => config.replicas_per_market = value.parse()?,
                "--lifecycle-hints" => config.lifecycle_hints = value.parse()?,
                "--monitoring" => options.monitoring = value.parse()?,
                "--queue-items" => options.queue_items = value.parse()?,
                "--queue-bytes" => options.queue_bytes = value.parse()?,
                "--transport-only" => options.transport_only = value.parse()?,
                "--integrity-path" => options.integrity_path = Some(value),
                _ => anyhow::bail!("unknown option {arg}"),
            }
        }
        config.validate()?;
        ensure!(options.seconds != Some(0), "duration must be positive");
        ensure!(
            options.transport_only || options.integrity_path.is_some(),
            "recording requires --integrity-path PATH (new file)"
        );
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Options> {
        Options::parse(args.iter().map(|a| (*a).to_owned()))
    }

    #[test]
    fn defaults_and_overrides() {
        let o = parse(&["--integrity-path", "x.jsonl"]).unwrap();
        assert_eq!(o.queue_items, DEFAULT_QUEUE_ITEMS);
        assert_eq!(o.queue_bytes, 64 << 20);
        assert_eq!(o.gamma, GAMMA_MARKETS);
        assert_eq!(o.seconds, None);
        assert!(!o.transport_only && !o.monitoring);
        let o = parse(&[
            "--transport-only",
            "true",
            "--seconds",
            "5",
            "--max-sockets",
            "3",
            "--replicas-per-market",
            "2",
            "--ws-url",
            "ws://127.0.0.1:1",
        ])
        .unwrap();
        assert!(o.transport_only && o.integrity_path.is_none());
        assert_eq!(o.seconds, Some(5));
        assert_eq!(o.transport.max_sockets, 3);
        assert_eq!(o.transport.replicas_per_market, 2);
        assert_eq!(o.transport.endpoint, "ws://127.0.0.1:1");
    }

    #[test]
    fn rejects_invalid_invocations_before_starting_work() {
        for (args, expected) in [
            (&["--seconds"][..], "options require a value"),
            (&["--bogus", "1"], "unknown option --bogus"),
            (
                &["--transport-only", "true", "--seconds", "0"],
                "duration must be positive",
            ),
            (&[], "recording requires --integrity-path"),
            (
                &["--transport-only", "true", "--replicas-per-market", "3"],
                "replicas_per_market",
            ),
            (&["--transport-only", "yes"], "provided string was not"),
        ] {
            let error = parse(args).unwrap_err().to_string();
            assert!(error.contains(expected), "{args:?}: {error}");
        }
    }
}
