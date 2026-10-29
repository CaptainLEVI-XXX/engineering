//! Command-line options: an optional config file plus per-run overrides,
//! parsed and validated before any task starts.
use super::config::CollectorConfig;
use anyhow::{Context, Result, ensure};

const USAGE: &str = "options require a value: --config FILE --seconds N --integrity-path FILE --transport-only BOOL; overrides: --gamma-url URL --ws-url URL --assets-per-socket N --max-sockets N --replicas-per-market N --lifecycle-hints BOOL --monitoring BOOL --queue-items N --queue-bytes N";

#[derive(Debug)]
pub struct Options {
    /// Defaults, then `--config FILE`, then the override flags below.
    pub config: CollectorConfig,
    /// Bounded run length; `None` runs until SIGINT.
    pub seconds: Option<u64>,
    /// Count and discard raw messages; nothing is written to stdout.
    pub transport_only: bool,
    /// Create-new integrity sidecar; required unless `transport_only`.
    pub integrity_path: Option<String>,
}

impl Options {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut pairs = vec![];
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            pairs.push((arg, args.next().context(USAGE)?));
        }
        // The file is the base layer wherever --config appears; flags override it.
        let mut config = match pairs.iter().filter(|(a, _)| a == "--config").count() {
            0 => CollectorConfig::default(),
            1 => CollectorConfig::load(&pairs.iter().find(|(a, _)| a == "--config").unwrap().1)?,
            _ => anyhow::bail!("--config may be given once"),
        };
        let mut options = Self {
            config: CollectorConfig::default(),
            seconds: None,
            transport_only: false,
            integrity_path: None,
        };
        let (run, transport, books) = (&mut config.run, &mut config.transport, &mut config.books);
        for (arg, value) in pairs {
            match arg.as_str() {
                "--config" => {}
                "--seconds" => options.seconds = Some(value.parse::<u64>()?),
                "--transport-only" => options.transport_only = value.parse()?,
                "--integrity-path" => options.integrity_path = Some(value),
                "--gamma-url" => run.gamma_url = value,
                "--queue-items" => run.queue_items = value.parse()?,
                "--queue-bytes" => run.queue_bytes = value.parse()?,
                "--ws-url" => transport.endpoint = value,
                "--assets-per-socket" => transport.assets_per_socket = value.parse()?,
                "--max-sockets" => transport.max_sockets = value.parse()?,
                "--replicas-per-market" => transport.replicas_per_market = value.parse()?,
                "--lifecycle-hints" => transport.lifecycle_hints = value.parse()?,
                "--monitoring" => books.monitoring = value.parse()?,
                _ => anyhow::bail!("unknown option {arg}"),
            }
        }
        config.validate()?;
        options.config = config;
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
        assert_eq!(o.config, CollectorConfig::default());
        assert_eq!(o.seconds, None);
        assert!(!o.transport_only);
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
            "--monitoring",
            "true",
            "--queue-items",
            "7",
        ])
        .unwrap();
        assert!(o.transport_only && o.integrity_path.is_none());
        assert_eq!(o.seconds, Some(5));
        let t = &o.config.transport;
        assert_eq!((t.max_sockets, t.replicas_per_market), (3, 2));
        assert_eq!(t.endpoint, "ws://127.0.0.1:1");
        assert!(o.config.books.monitoring);
        assert_eq!(o.config.run.queue_items, 7);
    }

    #[test]
    fn file_is_the_base_layer_and_flags_override_it_in_any_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.yaml");
        std::fs::write(
            &path,
            "transport:\n  max_sockets: 900\n  assets_per_socket: 500\nbooks:\n  total_levels: 120000000\n",
        )
        .unwrap();
        let path = path.to_str().unwrap();
        // The override precedes --config on the command line and still wins.
        let o = parse(&[
            "--max-sockets",
            "950",
            "--config",
            path,
            "--transport-only",
            "true",
        ])
        .unwrap();
        assert_eq!(o.config.transport.max_sockets, 950);
        assert_eq!(o.config.transport.assets_per_socket, 500);
        assert_eq!(o.config.books.total_levels, 120_000_000);
    }

    #[test]
    fn rejects_invalid_invocations_before_starting_work() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("bad.yaml");
        std::fs::write(&bad, "books:\n  levels_per_side: 70000\n").unwrap();
        let typo = dir.path().join("typo.yaml");
        std::fs::write(&typo, "book:\n  total_levels: 1\n").unwrap();
        let (bad, typo) = (bad.to_str().unwrap(), typo.to_str().unwrap());
        let missing = dir.path().join("missing.yaml");
        let missing = missing.to_str().unwrap();
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
            (&["--transport-only", "true", "--config", bad], "SBE u16"),
            (
                &["--transport-only", "true", "--config", typo],
                "unknown field `book`",
            ),
            (
                &["--transport-only", "true", "--config", missing],
                "read config",
            ),
            (
                &["--config", bad, "--config", bad],
                "--config may be given once",
            ),
        ] {
            let error = format!("{:#}", parse(args).unwrap_err());
            assert!(error.contains(expected), "{args:?}: {error}");
        }
    }
}
