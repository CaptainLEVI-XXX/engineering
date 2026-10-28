//! Opt-in, bounded research tool. Never invoked by the collector.
use anyhow::{Context, Result, bail, ensure};
use engineering_challenge::{
    discovery::AdapterId,
    market_data::{Limits, envelope, parse_scaled},
    websocket::protocol::{ControlFrame, SubscriptionOp, WireProtocol, protocol},
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{BufWriter, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpSocket, lookup_host},
    sync::{mpsc, oneshot},
    time::{MissedTickBehavior, interval, sleep, timeout},
};
use tokio_tungstenite::{
    client_async_tls_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

const ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const HOST: &str = "ws-subscriptions-clob.polymarket.com";
const MESSAGE_BYTES: usize = 2 << 20;
const REPORT_BYTES: usize = 2 << 20;
const SAMPLE_CAP: usize = 200_000;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Serialize)]
struct Config {
    assets: Vec<String>,
    adapter: AdapterId,
    destination: PathBuf,
    cohort: String,
    trial: u32,
    region: String,
    revision: String,
    toolchain: String,
    seconds: u64,
    warmup: u64,
    local_port: Option<u16>,
    raw_bytes: u64,
    raw_observations: u64,
    matcher_entries: usize,
    matcher_bytes: usize,
    window_ms: u64,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
        let mut values = HashMap::new();
        let mut approved = false;
        let mut args = args.into_iter();
        while let Some(key) = args.next() {
            if key == "--help" {
                eprintln!(
                    "latency_probe --approve-live --assets ID,ID --adapter v1|v2 \
                    --destination NEW_DIR --cohort baseline|renewal|source-port --trial N \
                    --region LABEL --revision COMMIT --toolchain LABEL \
                    [--seconds 600] [--warmup 60] [--local-port N] \
                    [--raw-bytes 268435456] [--raw-observations 1000000] \
                    [--matcher-entries 100000] [--matcher-bytes 67108864] [--window-ms 5000]\n\
                    NO DEFAULT LIVE RUN. See docs/latency-research.md for approval and limits."
                );
                return Ok(None);
            }
            if key == "--approve-live" {
                ensure!(!approved, "duplicate approval flag");
                approved = true;
                continue;
            }
            ensure!(
                [
                    "--assets",
                    "--adapter",
                    "--destination",
                    "--cohort",
                    "--trial",
                    "--region",
                    "--revision",
                    "--toolchain",
                    "--seconds",
                    "--warmup",
                    "--local-port",
                    "--raw-bytes",
                    "--raw-observations",
                    "--matcher-entries",
                    "--matcher-bytes",
                    "--window-ms"
                ]
                .contains(&key.as_str()),
                "unknown option {key}"
            );
            let value = args.next().context("option requires a value")?;
            ensure!(value.len() <= 32_768, "oversized CLI value");
            ensure!(values.insert(key, value).is_none(), "duplicate option");
        }
        ensure!(
            approved,
            "live experiment requires explicit approval; use --help"
        );
        let required = |key: &str| -> Result<String> {
            values
                .get(key)
                .cloned()
                .with_context(|| format!("required {key}"))
        };
        let number = |key: &str, default: u64| -> Result<u64> {
            values.get(key).map_or(Ok(default), |v| Ok(v.parse()?))
        };
        let config = Self {
            assets: required("--assets")?
                .split(',')
                .map(str::to_owned)
                .collect(),
            adapter: match required("--adapter")?.as_str() {
                "v1" => AdapterId::PolymarketV1,
                "v2" => AdapterId::PolymarketV2,
                _ => bail!("adapter must be v1 or v2"),
            },
            destination: required("--destination")?.into(),
            cohort: required("--cohort")?,
            trial: required("--trial")?.parse()?,
            region: required("--region")?,
            revision: required("--revision")?,
            toolchain: required("--toolchain")?,
            seconds: number("--seconds", 600)?,
            warmup: number("--warmup", 60)?,
            local_port: values.get("--local-port").map(|s| s.parse()).transpose()?,
            raw_bytes: number("--raw-bytes", 256 << 20)?,
            raw_observations: number("--raw-observations", 1_000_000)?,
            matcher_entries: usize::try_from(number("--matcher-entries", 100_000)?)?,
            matcher_bytes: usize::try_from(number("--matcher-bytes", 64 << 20)?)?,
            window_ms: number("--window-ms", 5_000)?,
        };
        ensure!(
            (1..=100).contains(&config.assets.len()),
            "explicit sample must have 1..100 assets; no universe discovery"
        );
        let mut unique = HashSet::new();
        for id in &config.assets {
            ensure!(
                !id.is_empty() && id.len() <= 256 && unique.insert(id),
                "empty, duplicate or oversized asset"
            );
        }
        ensure!(
            ["baseline", "renewal", "source-port"].contains(&config.cohort.as_str()),
            "unknown cohort"
        );
        ensure!(config.trial > 0, "trial numbering starts at 1");
        ensure!(
            (60..=3600).contains(&config.seconds),
            "measured duration must be 60..3600 seconds"
        );
        ensure!(
            config.cohort != "renewal" || config.seconds >= 120,
            "renewal requires at least 120 measured seconds"
        );
        ensure!(
            (30..=300).contains(&config.warmup),
            "warmup must be 30..300 seconds"
        );
        ensure!(
            config.local_port.is_some() == (config.cohort == "source-port"),
            "only source-port cohort requires --local-port"
        );
        ensure!(
            config.local_port.is_none_or(|p| p >= 1024),
            "source port must be nonprivileged and nonzero"
        );
        ensure!(
            (1 << 20..=2_u64 << 30).contains(&config.raw_bytes),
            "raw-byte cap outside 1MiB..2GiB"
        );
        ensure!(
            (1..=5_000_000).contains(&config.raw_observations),
            "invalid observation cap"
        );
        ensure!(
            (1..=100_000).contains(&config.matcher_entries),
            "invalid matcher entry cap"
        );
        ensure!(
            (1024..=128 << 20).contains(&config.matcher_bytes),
            "invalid matcher byte cap"
        );
        ensure!(
            (100..=10_000).contains(&config.window_ms),
            "window must be 100..10000ms"
        );
        for label in [&config.region, &config.revision, &config.toolchain] {
            ensure!(
                !label.is_empty() && label.len() <= 256 && !label.contains(['\n', '\r']),
                "invalid metadata label"
            );
        }
        Ok(Some(config))
    }

    fn treatment(&self) -> usize {
        // Predeclared alternating assignment; no post-result choice of winner.
        ((self.trial - 1) % 2) as usize
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Stamp {
    mono_ns: u64,
    wall_ns: u64,
}
impl Stamp {
    fn now(origin: Instant) -> Result<Self> {
        let mono = Instant::now();
        let wall = SystemTime::now();
        Ok(Self {
            mono_ns: u64::try_from(mono.duration_since(origin).as_nanos())?,
            wall_ns: u64::try_from(wall.duration_since(UNIX_EPOCH)?.as_nanos())?,
        })
    }
}

/// All writes share a hard serialized-byte cap, with space reserved for termination.
/// Raw JSON strings round-trip the complete UTF-8 message, including whitespace.
trait EvidenceSink: Write {
    fn sync_all(&self) -> std::io::Result<()>;
}
impl EvidenceSink for File {
    fn sync_all(&self) -> std::io::Result<()> {
        File::sync_all(self)
    }
}
struct Evidence<W: EvidenceSink = File> {
    writer: Option<BufWriter<W>>,
    bytes: u64,
    limit: u64,
    rows: u64,
    failed_write: bool,
}
impl Evidence<File> {
    fn create(path: &std::path::Path, limit: u64) -> Result<Self> {
        Ok(Self::new(
            OpenOptions::new().write(true).create_new(true).open(path)?,
            limit,
        ))
    }
}
impl<W: EvidenceSink> Evidence<W> {
    fn new(writer: W, limit: u64) -> Self {
        Self {
            writer: Some(BufWriter::with_capacity(64 << 10, writer)),
            bytes: 0,
            limit,
            rows: 0,
            failed_write: false,
        }
    }
    fn poison(&mut self) {
        self.failed_write = true;
        // BufWriter's destructor otherwise retries its remaining buffer after
        // a failed/partial flush. The prefix is uncertain: never retry it.
        if let Some(writer) = self.writer.take() {
            let _ = writer.into_parts();
        }
    }
    fn row(&mut self, row: &Value) -> Result<()> {
        self.write(row, 16 << 10)
    }
    fn write(&mut self, row: &Value, reserve: u64) -> Result<()> {
        ensure!(
            !self.failed_write,
            "evidence has an uncertain partial write; cannot append completion"
        );
        let mut bytes = serde_json::to_vec(row)?;
        bytes.push(b'\n');
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .context("evidence counter overflow")?;
        ensure!(
            next <= self.limit.saturating_sub(reserve),
            "evidence byte cap reached; trial incomplete"
        );
        if let Err(error) = self
            .writer
            .as_mut()
            .context("evidence closed")?
            .write_all(&bytes)
        {
            self.poison();
            return Err(error.into());
        }
        self.bytes = next;
        self.rows += 1;
        Ok(())
    }
    fn finish(&mut self, status: &Value) -> Result<()> {
        let result = (|| {
            // Detect failed prefix writes before appending a success marker.
            self.writer.as_mut().context("evidence closed")?.flush()?;
            self.write(status, 0)?;
            let writer = self.writer.as_mut().context("evidence closed")?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            self.poison();
        }
        result
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize)]
enum Kind {
    InitialSnapshot,
    ReplacementSnapshot,
    Change,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize)]
struct Entry {
    side: u8,
    price: i64,
    quantity: i64,
    hash: Option<String>,
}
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize)]
struct Canonical {
    asset: String,
    kind: Kind,
    exchange_us: u64,
    hash: Option<String>,
    entries: Vec<Entry>,
}
impl Canonical {
    fn retained_bytes(&self) -> usize {
        // Charged capacities plus a conservative allowance for collection nodes.
        256 + self.asset.capacity()
            + self.hash.as_ref().map_or(0, String::capacity)
            + self.entries.capacity() * std::mem::size_of::<Entry>()
            + self
                .entries
                .iter()
                .map(|e| e.hash.as_ref().map_or(0, String::capacity))
                .sum::<usize>()
    }
}

#[derive(Deserialize)]
struct WireLevel {
    price: String,
    size: String,
}
#[derive(Deserialize)]
struct WireChange {
    asset_id: String,
    side: String,
    price: String,
    size: String,
    hash: Option<String>,
}
#[derive(Deserialize)]
struct WireEvent {
    event_type: String,
    asset_id: Option<String>,
    timestamp: Option<String>,
    hash: Option<String>,
    bids: Option<Vec<WireLevel>>,
    asks: Option<Vec<WireLevel>>,
    price_changes: Option<Vec<WireChange>>,
    new_tick_size: Option<String>,
}

fn checked_hash(hash: Option<String>) -> Result<Option<String>> {
    ensure!(
        hash.as_ref().is_none_or(|s| s.len() <= 256),
        "oversized hash"
    );
    Ok(hash)
}
fn price(s: &str) -> Result<i64> {
    let p = parse_scaled(s)?;
    ensure!(p <= 1_000_000, "Polymarket price outside probability range");
    Ok(p)
}
fn exchange_time(s: Option<String>) -> Result<u64> {
    let s = s.context("missing exchange timestamp")?;
    ensure!(
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
        "invalid exchange timestamp"
    );
    let us = s
        .parse::<u64>()?
        .checked_mul(1000)
        .context("exchange timestamp overflow")?;
    ensure!(
        chrono::DateTime::from_timestamp_micros(i64::try_from(us)?).is_some(),
        "exchange timestamp outside reader range"
    );
    Ok(us)
}

/// Probe-specific parsing deliberately does not depend on normalized Change.
/// Validation is message-atomic; callers update initialization only after success.
fn canonicalize(
    text: &str,
    assets: &HashSet<String>,
    initialized: &HashSet<String>,
) -> Result<Vec<Canonical>> {
    let limits = Limits {
        message_bytes: MESSAGE_BYTES,
        events: 4096,
        entries: 65_535,
        identity_bytes: 256,
    };
    let mut events = Vec::new();
    let mut seen = initialized.clone();
    let mut total_entries = 0usize;
    for raw in envelope(text, &limits)? {
        let wire: WireEvent = serde_json::from_str(raw.get())?;
        let hash = checked_hash(wire.hash)?;
        match wire.event_type.as_str() {
            "book" => {
                let asset = wire.asset_id.context("book missing asset")?;
                ensure!(assets.contains(&asset), "unexpected snapshot asset {asset}");
                let mut entries = Vec::new();
                for (side, levels) in [
                    (0, wire.bids.context("missing bids")?),
                    (1, wire.asks.context("missing asks")?),
                ] {
                    let mut prices = HashSet::new();
                    for level in levels {
                        let p = price(&level.price)?;
                        ensure!(prices.insert(p), "duplicate snapshot price");
                        entries.push(Entry {
                            side,
                            price: p,
                            quantity: parse_scaled(&level.size)?,
                            hash: None,
                        });
                    }
                }
                total_entries += entries.len();
                let kind = if seen.insert(asset.clone()) {
                    Kind::InitialSnapshot
                } else {
                    Kind::ReplacementSnapshot
                };
                events.push(Canonical {
                    asset,
                    kind,
                    exchange_us: exchange_time(wire.timestamp)?,
                    hash,
                    entries,
                });
            }
            "price_change" => {
                let exchange_us = exchange_time(wire.timestamp)?;
                let mut indices = HashMap::new();
                let changes = wire.price_changes.context("missing price_changes")?;
                ensure!(!changes.is_empty(), "empty price_changes");
                total_entries += changes.len();
                for change in changes {
                    ensure!(assets.contains(&change.asset_id), "unexpected change asset");
                    ensure!(
                        seen.contains(&change.asset_id),
                        "change before snapshot: coverage degraded"
                    );
                    let side = match change.side.as_str() {
                        "BUY" => 0,
                        "SELL" => 1,
                        _ => bail!("invalid side"),
                    };
                    let entry = Entry {
                        side,
                        price: price(&change.price)?,
                        quantity: parse_scaled(&change.size)?,
                        hash: checked_hash(change.hash)?,
                    };
                    let index = *indices.entry(change.asset_id.clone()).or_insert_with(|| {
                        events.push(Canonical {
                            asset: change.asset_id,
                            kind: Kind::Change,
                            exchange_us,
                            hash: hash.clone(),
                            entries: Vec::new(),
                        });
                        events.len() - 1
                    });
                    events[index].entries.push(entry);
                }
            }
            "tick_size_change" => {
                ensure!(
                    assets.contains(&wire.asset_id.context("tick missing asset")?),
                    "unexpected tick asset"
                );
                exchange_time(wire.timestamp)?;
                ensure!(
                    price(&wire.new_tick_size.context("missing tick size")?)? > 0,
                    "zero tick size"
                );
            }
            "last_trade_price" | "best_bid_ask" | "new_market" | "market_resolved" => {}
            _ => bail!("unknown event type / server rejection"),
        }
        ensure!(
            total_entries <= limits.entries && events.len() <= limits.events,
            "canonical message limit"
        );
    }
    Ok(events)
}

#[derive(Clone, Debug)]
struct Observation {
    event: Canonical,
    source: usize,
    generation: u32,
    ordinal: u64,
    event_index: u32,
    stamp: Stamp,
    decided: bool,
}
#[derive(Clone, Debug, Serialize)]
struct Pair {
    asset: String,
    kind: Kind,
    a_ordinal: u64,
    b_ordinal: u64,
    a_event_index: u32,
    b_event_index: u32,
    a_generation: u32,
    b_generation: u32,
    a_ns: u64,
    b_ns: u64,
    delta_ns: i64,
}
#[derive(Clone, Copy, Debug)]
enum Outcome {
    Matched,
    Ambiguous,
    Unmatched,
    Censored,
}
#[derive(Default, Debug, Serialize)]
struct Counts {
    seen: [u64; 2],
    matched: [u64; 2],
    ambiguous: [u64; 2],
    unmatched: [u64; 2],
    censored: [u64; 2],
}
impl Counts {
    fn outcome(&mut self, source: usize, outcome: Outcome) {
        let counts = match outcome {
            Outcome::Matched => &mut self.matched,
            Outcome::Ambiguous => &mut self.ambiguous,
            Outcome::Unmatched => &mut self.unmatched,
            Outcome::Censored => &mut self.censored,
        };
        counts[source] += 1;
    }
}

/// Conservative three-block matcher: the middle block is decided only with a
/// complete leading and trailing guard block. No pairing across block edges.
/// Full canonical equality, not hash equality, drives the index.
struct Matcher {
    window_ns: u64,
    start_ns: u64,
    blocks: VecDeque<Vec<Observation>>,
    entries: usize,
    bytes: usize,
    entry_cap: usize,
    byte_cap: usize,
    counts: BTreeMap<(String, Kind), Counts>,
    resource_resets: u64,
    expired_blocks: u64,
}
impl Matcher {
    fn new(start_ns: u64, window_ns: u64, entry_cap: usize, byte_cap: usize) -> Self {
        Self {
            window_ns,
            start_ns,
            blocks: VecDeque::from([Vec::new()]),
            entries: 0,
            bytes: 0,
            entry_cap,
            byte_cap,
            counts: BTreeMap::new(),
            resource_resets: 0,
            expired_blocks: 0,
        }
    }
    fn count(&mut self, o: &Observation, outcome: Outcome) {
        self.counts
            .entry((o.event.asset.clone(), o.event.kind))
            .or_default()
            .outcome(o.source, outcome);
    }
    fn push(&mut self, o: Observation) -> Result<Vec<Pair>> {
        ensure!(
            o.stamp.mono_ns >= self.start_ns + self.window_ns * (self.blocks.len() - 1) as u64,
            "arrival behind matcher watermark; cannot relabel old input"
        );
        let pairs = self.advance(o.stamp.mono_ns)?;
        self.counts
            .entry((o.event.asset.clone(), o.event.kind))
            .or_default()
            .seen[o.source] += 1;
        let bytes = o.event.retained_bytes();
        if self.entries >= self.entry_cap || bytes > self.byte_cap.saturating_sub(self.bytes) {
            // No tombstone can be forgotten into apparent uniqueness: censor all
            // retained undecided observations and require a new full guard block.
            self.censor_all();
            self.resource_resets += 1;
            self.start_ns = o.stamp.mono_ns;
            self.count(&o, Outcome::Censored);
            return Ok(pairs);
        }
        self.entries += 1;
        self.bytes += bytes;
        self.blocks
            .back_mut()
            .context("missing matcher block")?
            .push(o);
        Ok(pairs)
    }
    fn advance(&mut self, now: u64) -> Result<Vec<Pair>> {
        let mut pairs = Vec::new();
        while now >= self.start_ns + self.window_ns * self.blocks.len() as u64 {
            if self.blocks.len() == 3 {
                pairs.extend(self.decide_middle()?);
                let old = self.blocks.pop_front().context("missing guard")?;
                for o in old {
                    if !o.decided {
                        self.count(&o, Outcome::Censored);
                    }
                    self.entries -= 1;
                    self.bytes -= o.event.retained_bytes();
                }
                self.start_ns += self.window_ns;
                self.expired_blocks += 1;
            }
            self.blocks.push_back(Vec::new());
        }
        Ok(pairs)
    }
    fn decide_middle(&mut self) -> Result<Vec<Pair>> {
        let mut index: HashMap<&Canonical, [Vec<(usize, usize)>; 2]> = HashMap::new();
        for (block, observations) in self.blocks.iter().enumerate() {
            for (i, o) in observations.iter().enumerate() {
                index.entry(&o.event).or_default()[o.source].push((block, i));
            }
        }
        let middle = &self.blocks[1];
        let mut outcomes = vec![Outcome::Unmatched; middle.len()];
        let mut candidates = Vec::new();
        for (i, o) in middle.iter().enumerate() {
            let slots = &index[&o.event];
            if slots[0].len() > 1 || slots[1].len() > 1 {
                outcomes[i] = Outcome::Ambiguous;
            } else if slots[1 - o.source].is_empty() {
                outcomes[i] = Outcome::Unmatched;
            } else {
                let (other_block, other_i) = slots[1 - o.source][0];
                if other_block != 1 {
                    outcomes[i] = Outcome::Censored;
                } else if o.source == 0 {
                    candidates.push((i, other_i));
                }
            }
        }
        // Reject the entire asset's candidate set on any within-source order
        // inversion, across event kinds too. Never sort by exchange timestamp.
        let mut last_b = HashMap::new();
        let mut inverted = HashSet::new();
        for &(a, b) in &candidates {
            let asset = middle[a].event.asset.as_str();
            if last_b
                .insert(asset, b)
                .is_some_and(|previous| b <= previous)
            {
                inverted.insert(asset);
            }
        }
        let mut pairs = Vec::new();
        for (a, b) in candidates {
            let left = &middle[a];
            let right = &middle[b];
            if inverted.contains(left.event.asset.as_str()) {
                outcomes[a] = Outcome::Ambiguous;
                outcomes[b] = Outcome::Ambiguous;
                continue;
            }
            outcomes[a] = Outcome::Matched;
            outcomes[b] = Outcome::Matched;
            pairs.push(Pair {
                asset: left.event.asset.clone(),
                kind: left.event.kind,
                a_ordinal: left.ordinal,
                b_ordinal: right.ordinal,
                a_event_index: left.event_index,
                b_event_index: right.event_index,
                a_generation: left.generation,
                b_generation: right.generation,
                a_ns: left.stamp.mono_ns,
                b_ns: right.stamp.mono_ns,
                delta_ns: signed_delta(left.stamp.mono_ns, right.stamp.mono_ns)?,
            });
        }
        drop(index);
        for (i, outcome) in outcomes.into_iter().enumerate() {
            let o = &mut self.blocks[1][i];
            ensure!(!o.decided, "matcher decided a window twice");
            o.decided = true;
            self.counts
                .entry((o.event.asset.clone(), o.event.kind))
                .or_default()
                .outcome(o.source, outcome);
        }
        Ok(pairs)
    }
    fn censor_all(&mut self) {
        let blocks = std::mem::replace(&mut self.blocks, VecDeque::from([Vec::new()]));
        for block in blocks {
            for o in block {
                if !o.decided {
                    self.count(&o, Outcome::Censored);
                }
            }
        }
        self.entries = 0;
        self.bytes = 0;
    }
}

fn signed_delta(a: u64, b: u64) -> Result<i64> {
    Ok(i64::try_from(i128::from(b) - i128::from(a))?)
}

enum Input {
    Connected {
        source: usize,
        generation: u32,
        stamp: Stamp,
        details: Value,
    },
    Sent {
        source: usize,
        generation: u32,
        stamp: Stamp,
        payload: String,
    },
    Frame {
        source: usize,
        generation: u32,
        ordinal: u64,
        stamp: Stamp,
        frame: Message,
    },
    Renewal {
        source: usize,
        stamp: Stamp,
        grant: oneshot::Sender<()>,
    },
}
fn deliver(tx: &mpsc::Sender<Input>, input: Input) -> Result<()> {
    tx.try_send(input)
        .map_err(|e| anyhow::anyhow!("probe input full/closed: {e}"))
}

// Private injection seam: the CLI always constructs production(), never accepts
// an endpoint or timing override. Local tests use plain ws and accelerated clocks
// only to exercise control flow, never as connection-latency evidence.
struct Network {
    endpoint: String,
    host: String,
    port: u16,
    start_gap: Duration,
    connect_timeout: Duration,
    io_timeout: Duration,
    heartbeat: Duration,
    renewal_at: Duration,
    init_timeout: Duration,
}
impl Network {
    fn production(config: &Config) -> Self {
        Self {
            endpoint: ENDPOINT.into(),
            host: HOST.into(),
            port: 443,
            start_gap: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(15),
            io_timeout: IO_TIMEOUT,
            heartbeat: Duration::from_secs(10),
            renewal_at: Duration::from_secs(60 + config.warmup + config.seconds / 2),
            init_timeout: Duration::from_secs(30),
        }
    }
}

struct ConnectionBudget {
    next: Cell<Instant>,
    gap: Duration,
}
impl ConnectionBudget {
    async fn acquire(&self) {
        loop {
            let next = self.next.get();
            let now = Instant::now();
            if now >= next {
                // Both feed futures run on this one runtime thread; no await
                // between checking and reserving, and no lock held over I/O.
                self.next.set(now + self.gap);
                return;
            }
            tokio::time::sleep_until(next.into()).await;
        }
    }
}

async fn feed(
    config: &Config,
    source: usize,
    origin: Instant,
    tx: mpsc::Sender<Input>,
    budget: &ConnectionBudget,
    network: &Network,
) -> Result<()> {
    // A starts at 0, B at >=30s. Concurrent measurement, not simultaneous handshakes.
    sleep(network.start_gap * source as u32).await;
    let wire = protocol(config.adapter);
    let mut generation = 0u32;
    let mut renew = config.cohort == "renewal" && source == config.treatment();
    loop {
        budget.acquire().await;
        let connect_start = Stamp::now(origin)?;
        deliver(
            &tx,
            Input::Connected {
                source,
                generation,
                stamp: connect_start,
                details: json!({"phase":"attempt", "endpoint":network.endpoint}),
            },
        )?;
        let (mut socket, details) = timeout(network.connect_timeout, async {
            let dns: Vec<SocketAddr> = lookup_host((network.host.as_str(), network.port))
                .await?
                .take(65)
                .collect();
            ensure!(dns.len() <= 64, "DNS answer budget exceeded");
            let remote = *dns.first().context("empty DNS answer")?;
            let tcp = if remote.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            let requested_port = if source == config.treatment() {
                config.local_port.unwrap_or(0)
            } else {
                0
            };
            let ip = if remote.is_ipv4() {
                IpAddr::V4(Ipv4Addr::UNSPECIFIED)
            } else {
                IpAddr::V6(Ipv6Addr::UNSPECIFIED)
            };
            tcp.bind(SocketAddr::new(ip, requested_port))?;
            let stream = tcp.connect(remote).await?;
            stream.set_nodelay(true)?;
            let local = stream.local_addr()?;
            let peer = stream.peer_addr()?;
            let ws = WebSocketConfig::default()
                .max_message_size(Some(MESSAGE_BYTES))
                .max_frame_size(Some(MESSAGE_BYTES))
                .max_write_buffer_size(256 << 10);
            // Default rustls connector preserves WebPKI roots, SNI and verification.
            let (socket, response) = client_async_tls_with_config(
                wire.handshake_request(&network.endpoint)?,
                stream,
                Some(ws),
                None,
            )
            .await?;
            Ok::<_, anyhow::Error>((
                socket,
                json!({"phase":"connected", "dns":dns,
                "local":local, "remote":peer, "http_status":response.status().as_u16(),
                "requested_local_port":requested_port, "tcp_nodelay":true,
                "tls":if network.endpoint.starts_with("wss:") {
                    "rustls default WebPKI verification + SNI; negotiated cipher not captured"
                } else { "plain ws: local test only, not latency evidence" }}),
            ))
        })
        .await
        .context("connect deadline")??;
        deliver(
            &tx,
            Input::Connected {
                source,
                generation,
                stamp: Stamp::now(origin)?,
                details,
            },
        )?;
        let subscription = wire.subscription(SubscriptionOp::Initial, &config.assets)?;
        timeout(
            network.io_timeout,
            socket.send(Message::Text(subscription.clone().into())),
        )
        .await
        .context("subscription write deadline")??;
        deliver(
            &tx,
            Input::Sent {
                source,
                generation,
                stamp: Stamp::now(origin)?,
                payload: subscription,
            },
        )?;
        let mut heartbeat = interval(network.heartbeat);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        heartbeat.tick().await;
        // Fixed origin schedule is recorded, independent of observed fast/slow source.
        let renewal_at = origin + network.renewal_at;
        let renewal_timer = tokio::time::sleep_until(renewal_at.into());
        tokio::pin!(renewal_timer);
        let mut ordinal = 0u64;
        loop {
            tokio::select! {
                frame = socket.next() => {
                    // Capture immediately after complete-message delivery, before parsing/queueing.
                    let stamp = Stamp::now(origin)?;
                    let frame = frame.context("unexpected socket EOF")??;
                    ordinal = ordinal.checked_add(1).context("ordinal overflow")?;
                    let closed = matches!(&frame, Message::Close(_));
                    let ping = matches!(&frame, Message::Ping(_));
                    deliver(&tx, Input::Frame { source, generation, ordinal, stamp, frame })?;
                    ensure!(!closed, "peer closed: coverage degraded; no retry");
                    if ping {
                        timeout(network.io_timeout, socket.flush()).await.context("control pong deadline")??;
                    }
                    // Buffered socket messages must not fill the queue in a
                    // single uninterrupted poll before the consumer can run.
                    tokio::task::yield_now().await;
                }
                _ = heartbeat.tick() => {
                    if let Some(text) = wire.heartbeat() {
                        timeout(network.io_timeout, socket.send(Message::Text(text.into()))).await.context("heartbeat deadline")??;
                        deliver(&tx, Input::Sent { source, generation, stamp: Stamp::now(origin)?, payload: text.into() })?;
                    }
                }
                _ = &mut renewal_timer, if renew => {
                    let (grant, permitted) = oneshot::channel();
                    deliver(&tx, Input::Renewal { source, stamp: Stamp::now(origin)?, grant })?;
                    timeout(network.io_timeout, permitted).await.context("renewal grant deadline")?.context("renewal grant refused")?;
                    timeout(network.io_timeout, socket.close(None)).await.context("renewal close deadline")??;
                    drop(socket); // At most two physical sockets, never old+new+peer.
                    generation += 1;
                    renew = false;
                    break;
                }
            }
        }
    }
}

struct Probe<W: EvidenceSink = File> {
    config: Config,
    origin: Instant,
    evidence: Evidence<W>,
    report: File,
    assets: HashSet<String>,
    initialized: [HashSet<String>; 2],
    requested_ns: [Option<u64>; 2],
    ready_ns: [Option<u64>; 2],
    measured_start: Option<u64>,
    matcher: Matcher,
    samples: Vec<Pair>,
    observations: u64,
    captured_observations: u64,
    max_processing_ns: u64,
    max_queue_age_ns: u64,
    max_tick_lateness_ns: u64,
    warmup_events: u64,
    renewal_start: Option<u64>,
    renewal_ready_ns: Option<u64>,
}
impl<W: EvidenceSink> Probe<W> {
    fn pairs(&mut self, pairs: Vec<Pair>) -> Result<()> {
        for pair in pairs {
            ensure!(
                self.samples.len() < SAMPLE_CAP,
                "sample retention cap reached"
            );
            self.evidence.row(&json!({"type":"match", "pair":pair}))?;
            self.samples.push(pair);
        }
        Ok(())
    }
    fn input(&mut self, input: Input) -> Result<()> {
        let start = Instant::now();
        match input {
            Input::Connected {
                source,
                generation,
                stamp,
                details,
            } => {
                self.evidence.row(&json!({"type":"connection", "source":source, "generation":generation, "stamp":stamp, "details":details}))?;
                if details["phase"] == "attempt" {
                    self.initialized[source].clear();
                    self.ready_ns[source] = None;
                    self.requested_ns[source] = Some(stamp.mono_ns);
                }
            }
            Input::Sent {
                source,
                generation,
                stamp,
                payload,
            } => {
                self.evidence.row(&json!({"type":"sent", "source":source, "generation":generation, "stamp":stamp, "payload":payload}))?;
            }
            Input::Renewal {
                source,
                stamp,
                grant,
            } => {
                ensure!(
                    self.ready_ns.iter().all(Option::is_some),
                    "renewal refused: peer coverage incomplete"
                );
                self.evidence
                    .row(&json!({"type":"renewal_cutoff", "source":source, "stamp":stamp}))?;
                self.renewal_start = Some(stamp.mono_ns);
                self.initialized[source].clear();
                self.ready_ns[source] = None;
                // Bound the close-to-new-attempt interval too; do not reuse the
                // initial generation's already-expired initialization deadline.
                self.requested_ns[source] = Some(stamp.mono_ns);
                self.matcher.censor_all();
                self.matcher.start_ns = stamp.mono_ns;
                grant
                    .send(())
                    .map_err(|()| anyhow::anyhow!("renewal grant receiver closed"))?;
            }
            Input::Frame {
                source,
                generation,
                ordinal,
                stamp,
                frame,
            } => {
                ensure!(
                    self.observations < self.config.raw_observations,
                    "raw observation cap reached"
                );
                self.observations += 1;
                self.max_queue_age_ns = self.max_queue_age_ns.max(
                    Stamp::now(self.origin)?
                        .mono_ns
                        .saturating_sub(stamp.mono_ns),
                );
                let (kind, payload) = match &frame {
                    Message::Text(text) => ("text", json!(text.as_str())),
                    Message::Ping(bytes) => ("ping", json!(bytes.as_ref())),
                    Message::Pong(bytes) => ("pong", json!(bytes.as_ref())),
                    Message::Binary(bytes) => ("binary", json!(bytes.as_ref())),
                    Message::Close(close) => ("close", json!(format!("{close:?}"))),
                    Message::Frame(_) => bail!("unexpected raw frame"),
                };
                self.evidence.row(&json!({"type":"received", "source":source, "generation":generation, "ordinal":ordinal, "stamp":stamp, "frame":kind, "payload":payload}))?;
                self.captured_observations += 1;
                match frame {
                    Message::Text(text) => {
                        if protocol(self.config.adapter).classify(text.as_str())
                            == ControlFrame::Data
                        {
                            let events = canonicalize(
                                text.as_str(),
                                &self.assets,
                                &self.initialized[source],
                            )?;
                            for (event_index, event) in events.into_iter().enumerate() {
                                if matches!(
                                    event.kind,
                                    Kind::InitialSnapshot | Kind::ReplacementSnapshot
                                ) {
                                    self.initialized[source].insert(event.asset.clone());
                                }
                                if self.measured_start.is_some_and(|s| {
                                    stamp.mono_ns >= s
                                        && stamp.mono_ns < s + self.config.seconds * 1_000_000_000
                                }) {
                                    let resets = self.matcher.resource_resets;
                                    let pairs = self.matcher.push(Observation {
                                        event,
                                        source,
                                        generation,
                                        ordinal,
                                        event_index: u32::try_from(event_index)?,
                                        stamp,
                                        decided: false,
                                    })?;
                                    self.pairs(pairs)?;
                                    if self.matcher.resource_resets != resets {
                                        self.evidence.row(&json!({"type":"matcher_resource_reset", "stamp":stamp,
                                            "resets":self.matcher.resource_resets, "policy":"whole-window censor; new leading guard"}))?;
                                    }
                                } else {
                                    self.warmup_events += 1;
                                }
                            }
                            if self.initialized[source].len() == self.assets.len()
                                && self.ready_ns[source].is_none()
                            {
                                self.ready_ns[source] = Some(stamp.mono_ns);
                                self.evidence.row(&json!({"type":"all_initial_snapshots_observed", "source":source, "generation":generation, "stamp":stamp,
                                    "elapsed_from_attempt_ns":stamp.mono_ns.saturating_sub(self.requested_ns[source].unwrap_or(stamp.mono_ns))}))?;
                                if generation > 0 {
                                    self.renewal_ready_ns = Some(stamp.mono_ns);
                                }
                            }
                            if self.measured_start.is_none()
                                && self.ready_ns.iter().all(Option::is_some)
                            {
                                let measured = stamp.mono_ns + self.config.warmup * 1_000_000_000;
                                self.measured_start = Some(measured);
                                self.matcher.start_ns = measured;
                                self.evidence.row(&json!({"type":"measurement_schedule", "start_mono_ns":measured, "duration_seconds":self.config.seconds}))?;
                            }
                        }
                    }
                    Message::Binary(_) => bail!("unexpected binary data; evidence retained"),
                    Message::Close(_) => bail!("peer close; coverage degraded"),
                    _ => {}
                }
            }
        }
        self.max_processing_ns = self
            .max_processing_ns
            .max(u64::try_from(start.elapsed().as_nanos())?);
        ensure!(
            self.max_processing_ns <= 1_000_000_000 && self.max_queue_age_ns <= 1_000_000_000,
            "local processing/queue stall exceeds 1s; stop biased trial"
        );
        Ok(())
    }
    fn tick(&mut self, inputs_empty: bool, init_timeout: Duration) -> Result<bool> {
        let now = Stamp::now(self.origin)?.mono_ns;
        for source in 0..2 {
            if self.ready_ns[source].is_none()
                && self.requested_ns[source]
                    .is_some_and(|s| u128::from(now.saturating_sub(s)) > init_timeout.as_nanos())
            {
                bail!("source {source} failed complete snapshot coverage within {init_timeout:?}");
            }
        }
        if let Some(start) = self.measured_start {
            if now >= start && inputs_empty {
                let watermark = now.min(start + self.config.seconds * 1_000_000_000);
                if watermark
                    >= self.matcher.start_ns
                        + self.matcher.window_ns * self.matcher.blocks.len() as u64
                {
                    // Record only effective timer advances, before their match
                    // rows. Push advances remain inferable from received events.
                    self.evidence
                        .row(&json!({"type":"matcher_advance", "mono_ns":watermark}))?;
                }
                let pairs = self.matcher.advance(watermark)?;
                self.pairs(pairs)?;
            }
            if now >= start + self.config.seconds * 1_000_000_000 {
                ensure!(
                    self.config.cohort != "renewal" || self.renewal_ready_ns.is_some(),
                    "renewal cohort did not complete reinitialization"
                );
                return Ok(true);
            }
        }
        ensure!(
            now < (90 + self.config.warmup + self.config.seconds) * 1_000_000_000,
            "overall deadline; no paired readiness"
        );
        Ok(false)
    }
}

fn quantile(sorted: &[i64], numerator: usize, denominator: usize) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (sorted.len() * numerator).div_ceil(denominator).max(1);
    sorted.get(rank - 1).copied()
}
fn distribution(values: &mut [i64]) -> Value {
    values.sort_unstable();
    json!({"n":values.len(), "unit":"ns", "p50":quantile(values,50,100),
        "p95":quantile(values,95,100), "p99":quantile(values,99,100),
        "p999":quantile(values,999,1000), "p999_exploratory":values.len() < 100_000,
        "min":values.first(), "max":values.last()})
}

/// Fixed-seed block bootstrap of the median of completed 30s block medians.
/// This is NOT a confidence interval for pooled p99.9 or independent events.
fn block_interval(medians: &[i64]) -> Option<(i64, i64)> {
    if medians.len() < 3 {
        return None;
    }
    let mut seed = 0x7265736561726368_u64;
    let mut estimates = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let mut resample = Vec::with_capacity(medians.len());
        for _ in medians {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            resample.push(medians[(seed % medians.len() as u64) as usize]);
        }
        resample.sort_unstable();
        estimates.push(quantile(&resample, 50, 100)?);
    }
    estimates.sort_unstable();
    Some((
        quantile(&estimates, 25, 1000)?,
        quantile(&estimates, 975, 1000)?,
    ))
}

fn report<W: EvidenceSink>(probe: &mut Probe<W>, status: &str, cutoff: Stamp) -> Result<()> {
    let start = probe.measured_start.unwrap_or(cutoff.mono_ns);
    let mut strata: BTreeMap<(String, Kind), Vec<i64>> = BTreeMap::new();
    let mut blocks: BTreeMap<u64, Vec<i64>> = BTreeMap::new();
    for pair in &probe.samples {
        strata
            .entry((pair.asset.clone(), pair.kind))
            .or_default()
            .push(pair.delta_ns);
        if pair.kind == Kind::Change {
            blocks
                .entry(pair.a_ns.saturating_sub(start) / 30_000_000_000)
                .or_default()
                .push(pair.delta_ns);
        }
    }
    let mut body = format!(
        "# Latency research trial {} — {}\n\nStatus: **{}**.\n\n\
        Application-receive difference: signed B − A on one monotonic host clock; positive means A arrived first. \
        Not one-way network latency, exchange-wide losslessness, or a production optimization.\n\n\
        Baseline connections begin 30s apart; measurement uses concurrent feeds after complete initial snapshots and warmup. \
        Treatment alternates A/B by trial parity. Local ports can be rewritten by NAT.\n\n\
        Received observations handled: {}; retained match pairs: {}; canonical events outside measurement (startup/warmup/tail): {}.\n\
        Matcher resource resets: {}; expired guard blocks: {}.\n\
        Maximum receive-to-consumer age: {}ns; input processing including buffered evidence: {}ns; timer lateness: {}ns.\n\
        Renewal cutoff/ready monotonic ns: {:?}/{:?}.\n\n\
        ## Configuration\n\n```json\n{}\n```\n\n\
        ## Strata (snapshot kinds never pooled with changes)\n\n\
        Matched fraction is matched observations / all measured canonical observations, including censored. \
        Counts below are [A,B]; each pair contributes one observation to each side.\n\n",
        probe.config.trial,
        probe.config.cohort,
        status,
        probe.observations,
        probe.samples.len(),
        probe.warmup_events,
        probe.matcher.resource_resets,
        probe.matcher.expired_blocks,
        probe.max_queue_age_ns,
        probe.max_processing_ns,
        probe.max_tick_lateness_ns,
        probe.renewal_start,
        probe.renewal_ready_ns,
        serde_json::to_string_pretty(&probe.config)?
    );
    for ((asset, kind), counts) in &probe.matcher.counts {
        let mut values = strata.remove(&(asset.clone(), *kind)).unwrap_or_default();
        let fraction: Vec<Option<f64>> = (0..2)
            .map(|s| (counts.seen[s] > 0).then(|| counts.matched[s] as f64 / counts.seen[s] as f64))
            .collect();
        body.push_str(&format!(
            "- Asset `{asset}`, {kind:?}: counts `{}`, matched fractions `{}`; distribution `{}`\n",
            serde_json::to_string(counts)?,
            json!(fraction),
            distribution(&mut values)
        ));
    }
    body.push_str("\n## Time-block uncertainty (changes only)\n\n");
    let mut medians = Vec::new();
    for (block, mut values) in blocks {
        let stats = distribution(&mut values);
        let complete = start + (block + 1) * 30_000_000_000 <= cutoff.mono_ns;
        if complete {
            medians.push(quantile(&values, 50, 100).context("empty block")?);
        }
        body.push_str(&format!(
            "- 30s block {block}, complete={complete}: `{stats}`\n"
        ));
    }
    body.push_str(&format!("\nFixed-seed 1,000-resample 95% block-bootstrap interval for median of completed, nonempty block medians (ns): `{:?}`; {} blocks. \
        Assumes approximately independent stationary 30s blocks; renewal and market regimes can violate this. No pooled tail-confidence claim.\n\n\
        ## Conclusion and remaining evidence\n\n\
        This single trial cannot establish a reliable improvement. A p99.9 with fewer than 100,000 unambiguous matches per stratum is exploratory. \
        Larger counts alone do not establish confidence. Compare at least three predeclared 10-minute trials per treatment, their block estimates, \
        alternating treatment and an independent time/cohort before any production recommendation. \
        Renewals require pre/post and stable-control analysis of raw evidence, not only pooled results.\n\n\
        Matching is heuristic full-content equality with conservative guard windows. Repetition, order inversions, edge arrivals, \
        startup/shutdown, renewal and resource resets cause ambiguity/censoring. No occurrence-index or hash-only pairing. \
        Socket close/error, malformed input, missing initialization or output failure invalidate the trial. \
        Quiet initialized assets are unknown freshness, not proof of health.\n\n\
        SBE visibility, end-to-end stage attribution, source-selection hysteresis and switching benefit are not measured here. \
        TCP_NODELAY is already enabled in the collector and this probe. \
        This report's status is provisional until evidence finalization and a zero process exit; consult the terminal raw.jsonl record. \
        Raw output is buffered (64KiB), finalized by flush and sync_all; this is not an archive durability guarantee. \
        The final intake cutoff drops sockets, not a verified peer close handshake.\n",
        block_interval(&medians), medians.len()));
    ensure!(body.len() <= REPORT_BYTES, "report byte cap reached");
    probe.report.write_all(body.as_bytes())?;
    probe.report.flush()?;
    probe.report.sync_all()?;
    Ok(())
}

fn host_file(path: &str) -> String {
    // No environment or credentials. These Linux host fields are public metadata.
    let result = (|| -> std::io::Result<String> {
        let mut text = String::new();
        File::open(path)?.take(4096).read_to_string(&mut text)?;
        Ok(text)
    })();
    result.unwrap_or_else(|e| format!("unavailable: {e}"))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let Some(config) = Config::parse(std::env::args().skip(1))? else {
        return Ok(());
    };
    eprintln!(
        "APPROVED SAMPLE ONLY: {} assets, {:?}, cohort {}, trial {}, warmup {}s + measurement {}s; destination {}. Two sockets; starts >=30s apart; no retries.",
        config.assets.len(),
        config.adapter,
        config.cohort,
        config.trial,
        config.warmup,
        config.seconds,
        config.destination.display()
    );
    std::fs::create_dir(&config.destination)
        .context("destination must be a new directory with an existing parent")?;
    let mut evidence = Evidence::create(&config.destination.join("raw.jsonl"), config.raw_bytes)?;
    let report_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(config.destination.join("report.md"))?;
    let origin = Instant::now();
    evidence.row(&json!({"type":"metadata", "format_version":1, "config":config,
        "endpoint":ENDPOINT, "start":Stamp::now(origin)?, "hostname":host_file("/etc/hostname"),
        "cpu":host_file("/proc/cpuinfo"), "load":host_file("/proc/loadavg"),
        "os":std::env::consts::OS, "arch":std::env::consts::ARCH,
        "crate_version":env!("CARGO_PKG_VERSION"), "sample_cap":SAMPLE_CAP,
        "input_queue":8, "max_message_bytes":MESSAGE_BYTES, "treatment_source":config.treatment(),
        "policy":{"matcher":"three-block-full-content-v1", "matcher_advance_logged":true, "financial_scale":1_000_000,
            "guard_blocks":3, "init_deadline_seconds":30, "connect_deadline_seconds":15,
            "send_deadline_seconds":5, "local_stall_limit_ns":1_000_000_000,
            "report_byte_cap":REPORT_BYTES, "dns_answer_cap":64, "tail_minimum_matches":100_000,
            "bootstrap_seed":"7265736561726368", "bootstrap_resamples":1000, "statistics_block_seconds":30},
        "source_labels":["A","B"], "connection_start_offsets_seconds":[0,30],
        "renewal_origin_seconds":60+config.warmup+config.seconds/2,
        "clock":"std::time::Instant relative to run origin; wall SystemTime at delivery",
        "approval":"operator supplied --approve-live; no authorization inferred from implementation"}))?;
    let mut probe = Probe {
        assets: config.assets.iter().cloned().collect(),
        initialized: [HashSet::new(), HashSet::new()],
        requested_ns: [None; 2],
        ready_ns: [None; 2],
        measured_start: None,
        matcher: Matcher::new(
            0,
            config.window_ms * 1_000_000,
            config.matcher_entries,
            config.matcher_bytes,
        ),
        config: config.clone(),
        origin,
        evidence,
        report: report_file,
        samples: Vec::new(),
        observations: 0,
        captured_observations: 0,
        max_processing_ns: 0,
        max_queue_age_ns: 0,
        max_tick_lateness_ns: 0,
        warmup_events: 0,
        renewal_start: None,
        renewal_ready_ns: None,
    };
    drive(
        &mut probe,
        &Network::production(&config),
        tokio::signal::ctrl_c(),
    )
    .await?;
    eprintln!(
        "Bounded trial complete; inspect raw.jsonl and report.md. No production recommendation."
    );
    Ok(())
}

async fn drive<W: EvidenceSink>(
    probe: &mut Probe<W>,
    network: &Network,
    signal: impl std::future::Future<Output = std::io::Result<()>>,
) -> Result<()> {
    let config = probe.config.clone();
    let origin = probe.origin;
    let (tx, mut rx) = mpsc::channel(8);
    let budget = ConnectionBudget {
        next: Cell::new(origin),
        gap: network.start_gap,
    };
    let outcome: Result<()> = {
        let a = feed(&config, 0, origin, tx.clone(), &budget, network);
        let b = feed(&config, 1, origin, tx, &budget, network);
        tokio::pin!(a, b);
        tokio::pin!(signal);
        let mut timer = interval(Duration::from_millis(100));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            let step: Result<bool> = tokio::select! {
                result = &mut a => Err(result.err().unwrap_or_else(|| anyhow::anyhow!("source A stopped"))),
                result = &mut b => Err(result.err().unwrap_or_else(|| anyhow::anyhow!("source B stopped"))),
                result = &mut signal => Err(result.err().map(anyhow::Error::from).unwrap_or_else(|| anyhow::anyhow!("SIGINT: incomplete trial"))),
                input = rx.recv() => match input {
                    Some(input) => probe.input(input).map(|()| false),
                    None => Err(anyhow::anyhow!("input channel closed")),
                },
                scheduled = timer.tick() => {
                    let lateness = u64::try_from(scheduled.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    probe.max_tick_lateness_ns = probe.max_tick_lateness_ns.max(lateness);
                    if lateness > 1_000_000_000 { Err(anyhow::anyhow!("local timer stalled >1s")) } else { probe.tick(rx.is_empty(), network.init_timeout) }
                },
            };
            match step {
                Ok(false) => {}
                Ok(true) => break Ok(()),
                Err(e) => break Err(e),
            }
        }
        // Dropping both futures establishes intake cutoff and closes their TCP
        // streams. No new scheduling or background worker survives this scope.
    };
    let cutoff = Stamp::now(origin)?;
    let mut errors = Vec::new();
    if let Err(error) = outcome {
        errors.push(format!("{error:#}"));
    }
    // Preserve the already accepted bounded prefix even on a worker error.
    while let Ok(input) = rx.try_recv() {
        if let Err(error) = probe.input(input) {
            errors.push(format!("drain: {error:#}"));
            break;
        }
    }
    probe.matcher.censor_all();
    let status = if errors.is_empty() {
        "completed bounded trial; not research acceptance".to_owned()
    } else {
        format!("INCOMPLETE: {}", errors.join("; "))
    };
    if let Err(error) = report(probe, &status, cutoff) {
        errors.push(format!("report: {error:#}"));
    }
    let summary = json!({"type":"complete", "successful":errors.is_empty(), "errors":errors,
        "cutoff":cutoff, "raw_observations":probe.observations, "matched_pairs":probe.samples.len(),
        "captured_observations":probe.captured_observations,
        "resource_resets":probe.matcher.resource_resets, "evidence_rows_before_complete":probe.evidence.rows,
        "undrained_inputs":rx.len(), "load_end":host_file("/proc/loadavg"),
        "close_policy":"intake cutoff then TCP drop; no verified peer close handshake"});
    if let Err(error) = probe.evidence.finish(&summary) {
        errors.push(format!("evidence finalization: {error:#}"));
    }
    ensure!(errors.is_empty(), "{}", errors.join("; "));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(asset: &str, quantity: i64) -> Canonical {
        Canonical {
            asset: asset.into(),
            kind: Kind::Change,
            exchange_us: 123_000,
            hash: Some("repeated-hash".into()),
            entries: vec![Entry {
                side: 0,
                price: 500_000,
                quantity,
                hash: Some("same".into()),
            }],
        }
    }
    fn observation(source: usize, time: u64, asset: &str, quantity: i64) -> Observation {
        Observation {
            event: event(asset, quantity),
            source,
            stamp: Stamp {
                mono_ns: time,
                wall_ns: 999 - time,
            },
            ordinal: time,
            event_index: 0,
            generation: 0,
            decided: false,
        }
    }
    fn matcher() -> Matcher {
        Matcher::new(0, 100, 100, 1 << 20)
    }
    fn counts<'a>(m: &'a Matcher, asset: &str) -> &'a Counts {
        &m.counts[&(asset.into(), Kind::Change)]
    }

    #[test]
    fn delayed_matches_signed_arrival_not_wall_or_exchange_time() {
        let mut m = matcher();
        m.push(observation(1, 110, "A", 7)).unwrap();
        m.push(observation(0, 130, "A", 7)).unwrap();
        let pairs = m.advance(300).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].delta_ns, -20);
        assert_eq!(signed_delta(5, 9).unwrap(), 4);
        assert!(signed_delta(0, u64::MAX).is_err());
    }

    #[test]
    fn repeats_are_ambiguous_not_occurrence_pairs() {
        let mut m = matcher();
        for (source, time) in [(0, 110), (1, 120), (0, 130), (1, 140)] {
            m.push(observation(source, time, "A", 7)).unwrap();
        }
        assert!(m.advance(300).unwrap().is_empty());
        assert_eq!(counts(&m, "A").ambiguous, [2, 2]);
    }

    #[test]
    fn repeated_hash_and_same_time_do_not_merge_distinct_content() {
        let mut m = matcher();
        for (source, time, q) in [(0, 110, 7), (0, 120, 8), (1, 130, 7), (1, 140, 8)] {
            m.push(observation(source, time, "A", q)).unwrap();
        }
        assert_eq!(m.advance(300).unwrap().len(), 2);
    }

    #[test]
    fn independent_assets_can_reorder_but_same_asset_cannot() {
        let mut m = matcher();
        for (s, t, a, q) in [
            (0, 110, "A", 1),
            (0, 120, "B", 2),
            (1, 130, "B", 2),
            (1, 140, "A", 1),
        ] {
            m.push(observation(s, t, a, q)).unwrap();
        }
        assert_eq!(m.advance(300).unwrap().len(), 2);
        let mut m = matcher();
        for (s, t, q) in [(0, 110, 1), (0, 120, 2), (1, 130, 2), (1, 140, 1)] {
            m.push(observation(s, t, "A", q)).unwrap();
        }
        assert!(m.advance(300).unwrap().is_empty());
        assert_eq!(counts(&m, "A").ambiguous, [2, 2]);
    }

    #[test]
    fn missing_and_boundary_matches_are_not_fabricated() {
        let mut m = matcher();
        m.push(observation(0, 110, "missing", 7)).unwrap();
        m.push(observation(0, 190, "edge", 7)).unwrap();
        m.push(observation(1, 210, "edge", 7)).unwrap();
        assert!(m.advance(400).unwrap().is_empty());
        assert_eq!(counts(&m, "missing").unmatched, [1, 0]);
        assert_eq!(counts(&m, "edge").censored, [1, 1]);
    }

    #[test]
    fn ambiguity_survives_expiry_and_capacity_reset() {
        let mut m = matcher();
        for (s, t) in [(0, 190), (0, 210), (1, 220)] {
            m.push(observation(s, t, "A", 7)).unwrap();
        }
        assert!(m.advance(400).unwrap().is_empty());
        assert_eq!(counts(&m, "A").ambiguous, [2, 1]);
        let mut m = Matcher::new(0, 100, 2, 1 << 20);
        for (s, t) in [(0, 110), (0, 120), (1, 130)] {
            m.push(observation(s, t, "A", 7)).unwrap();
        }
        m.push(observation(0, 140, "A", 7)).unwrap();
        m.push(observation(1, 150, "A", 7)).unwrap();
        assert!(m.advance(500).unwrap().is_empty());
        m.censor_all();
        assert_eq!(m.resource_resets, 1);
        assert_eq!(counts(&m, "A").censored, [3, 2]);
        assert_eq!(m.entries, 0);
        assert_eq!(m.bytes, 0);
    }

    #[test]
    fn byte_cap_and_end_censoring_account_for_every_observation() {
        let mut m = Matcher::new(0, 100, 100, 1);
        m.push(observation(0, 110, "A", 7)).unwrap();
        assert_eq!(m.resource_resets, 1);
        assert_eq!(counts(&m, "A").censored, [1, 0]);
        let mut m = matcher();
        m.push(observation(0, 110, "A", 7)).unwrap();
        m.push(observation(1, 120, "A", 7)).unwrap();
        m.censor_all();
        assert_eq!(counts(&m, "A").censored, [1, 1]);
        assert_eq!(counts(&m, "A").seen, [1, 1]);
    }

    #[test]
    fn exact_canonicalization_ignores_whitespace_and_envelope_batching() {
        let assets = HashSet::from(["opaque-A".into()]);
        let single = r#"{"event_type":"price_change","timestamp":"123","price_changes":[{"asset_id":"opaque-A","side":"BUY","price":"0.5","size":"7.0000000","hash":"h"}]}"#;
        let spaced = format!("[ \n {} \n ]", single.replace("0.5", "0.500000"));
        assert_eq!(
            canonicalize(single, &assets, &assets).unwrap(),
            canonicalize(&spaced, &assets, &assets).unwrap()
        );
        for (old, new) in [
            ("7.0000000", "7.0000001"),
            ("0.5", "1.1"),
            ("7.0000000", "-7"),
            ("7.0000000", "1e2"),
        ] {
            assert!(canonicalize(&single.replace(old, new), &assets, &assets).is_err());
        }
        assert!(canonicalize(single, &assets, &HashSet::new()).is_err());
        assert_eq!(parse_scaled("9223372036854.775807").unwrap(), i64::MAX);
        assert!(parse_scaled("9223372036854.775808").is_err());
    }

    #[test]
    fn snapshots_are_separate_and_ordered_entries_are_significant() {
        let assets = HashSet::from(["A".into()]);
        let book = r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[{"price":"0.5","size":"0"}],"asks":[],"hash":"x"}"#;
        let first = canonicalize(book, &assets, &HashSet::new()).unwrap();
        let replacement = canonicalize(book, &assets, &assets).unwrap();
        assert_eq!(first[0].kind, Kind::InitialSnapshot);
        assert_eq!(replacement[0].kind, Kind::ReplacementSnapshot);
        assert_ne!(first, replacement);
        let duplicate = book.replace(
            "\"size\":\"0\"}",
            "\"size\":\"0\"},{\"price\":\"0.50\",\"size\":\"1\"}",
        );
        assert!(canonicalize(&duplicate, &assets, &assets).is_err());
        let mut left = event("A", 7);
        left.entries.push(Entry {
            side: 1,
            price: 600_000,
            quantity: 9,
            hash: None,
        });
        let mut right = left.clone();
        right.entries.reverse();
        assert_ne!(left, right);
    }

    #[test]
    fn whole_message_validation_rejects_late_bad_financial_fields() {
        let assets = HashSet::from(["A".into()]);
        let text = r#"[{"event_type":"book","asset_id":"A","timestamp":"1","bids":[],"asks":[]},{"event_type":"price_change","timestamp":"2","price_changes":[{"asset_id":"A","side":"BUY","price":"0.5","size":"1"},{"asset_id":"A","side":"BUY","price":"0.5","size":"bad"}]}]"#;
        let initialized = HashSet::new();
        assert!(canonicalize(text, &assets, &initialized).is_err());
        assert!(initialized.is_empty());
    }

    #[test]
    fn evidence_is_create_new_bounded_and_has_reserved_termination() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw.jsonl");
        let mut evidence = Evidence::create(&path, 20_000).unwrap();
        assert!(Evidence::create(&path, 20_000).is_err());
        evidence.row(&json!({"payload":"exact\n text"})).unwrap();
        assert!(evidence.row(&json!({"payload":"x".repeat(5000)})).is_err());
        evidence
            .finish(&json!({"type":"complete","successful":false}))
            .unwrap();
        let lines = std::fs::read_to_string(path).unwrap();
        assert_eq!(lines.lines().count(), 2);
        assert_eq!(
            serde_json::from_str::<Value>(lines.lines().next().unwrap()).unwrap()["payload"],
            "exact\n text"
        );
    }

    #[test]
    fn approval_and_tail_uncertainty_are_not_implicit() {
        assert!(Config::parse(Vec::<String>::new()).is_err());
        assert!(Config::parse(["--approve-live".into(), "--unknown".into(), "x".into()]).is_err());
        assert_eq!(quantile(&[-9, 0, 10], 50, 100), Some(0));
        assert!(block_interval(&[1, 2]).is_none());
        assert_eq!(block_interval(&[7, 7, 7]), Some((7, 7)));
        assert_eq!(distribution(&mut [1, 2, 3])["p999_exploratory"], true);
    }

    #[test]
    fn cli_cohorts_are_explicit_bounded_and_alternating() {
        let base: Vec<String> = [
            "--approve-live",
            "--assets",
            "opaque-A,opaque-B",
            "--adapter",
            "v2",
            "--destination",
            "new-trial",
            "--cohort",
            "baseline",
            "--trial",
            "1",
            "--region",
            "test",
            "--revision",
            "test-tree",
            "--toolchain",
            "test-toolchain",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        let parsed = Config::parse(base.clone()).unwrap().unwrap();
        assert_eq!(parsed.treatment(), 0);
        assert_eq!(parsed.seconds, 600);
        let mut port = base.clone();
        *port.iter_mut().find(|v| v.as_str() == "baseline").unwrap() = "source-port".into();
        *port.iter_mut().find(|v| v.as_str() == "1").unwrap() = "2".into();
        assert!(Config::parse(port.clone()).is_err());
        port.extend(["--local-port".into(), "20000".into()]);
        assert_eq!(Config::parse(port).unwrap().unwrap().treatment(), 1);
        let mut invalid = base;
        invalid.extend(["--matcher-entries".into(), "100001".into()]);
        assert!(Config::parse(invalid).is_err());
    }

    #[test]
    fn bounded_control_handoff_fails_explicitly_on_full_and_closed() {
        let (tx, rx) = mpsc::channel(1);
        let input = || Input::Sent {
            source: 0,
            generation: 0,
            stamp: Stamp {
                mono_ns: 0,
                wall_ns: 0,
            },
            payload: "PING".into(),
        };
        deliver(&tx, input()).unwrap();
        assert!(deliver(&tx, input()).is_err());
        drop(rx);
        assert!(deliver(&tx, input()).is_err());
    }

    #[test]
    fn out_of_order_delivery_cannot_enter_an_expired_window() {
        let mut m = matcher();
        m.advance(300).unwrap();
        assert!(m.push(observation(0, 110, "A", 7)).is_err());
        assert_eq!(m.entries, 0);
    }

    // These fixtures deliberately bypass CLI duration minimums. They exercise
    // real loopback TCP/WebSocket + feed + Probe + finalization, not latency.
    fn local_config(destination: PathBuf) -> Config {
        Config {
            assets: vec!["A".into()],
            adapter: AdapterId::PolymarketV2,
            destination,
            cohort: "baseline".into(),
            trial: 1,
            region: "LOCAL TEST ONLY".into(),
            revision: "synthetic".into(),
            toolchain: "test".into(),
            seconds: 1,
            warmup: 0,
            local_port: None,
            raw_bytes: 1 << 20,
            raw_observations: 1000,
            matcher_entries: 1000,
            matcher_bytes: 1 << 20,
            window_ms: 200,
        }
    }
    async fn local_network(config: &Config) -> (tokio::net::TcpListener, Network) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut network = Network::production(config);
        network.endpoint = format!("ws://{address}/ws/market");
        network.host = "127.0.0.1".into();
        network.port = address.port();
        network.start_gap = Duration::from_millis(20);
        network.connect_timeout = Duration::from_millis(500);
        network.io_timeout = Duration::from_millis(500);
        network.heartbeat = Duration::from_millis(50);
        network.renewal_at = Duration::from_millis(400);
        network.init_timeout = Duration::from_millis(500);
        (listener, network)
    }
    fn local_probe<W: EvidenceSink>(config: Config, evidence: Evidence<W>) -> Probe<W> {
        let report = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(config.destination.join("report.md"))
            .unwrap();
        Probe {
            assets: config.assets.iter().cloned().collect(),
            initialized: [HashSet::new(), HashSet::new()],
            requested_ns: [None; 2],
            ready_ns: [None; 2],
            measured_start: None,
            matcher: Matcher::new(
                0,
                config.window_ms * 1_000_000,
                config.matcher_entries,
                config.matcher_bytes,
            ),
            config,
            origin: Instant::now(),
            evidence,
            report,
            samples: Vec::new(),
            observations: 0,
            captured_observations: 0,
            max_processing_ns: 0,
            max_queue_age_ns: 0,
            max_tick_lateness_ns: 0,
            warmup_events: 0,
            renewal_start: None,
            renewal_ready_ns: None,
        }
    }
    fn file_probe(config: Config) -> Probe {
        let evidence =
            Evidence::create(&config.destination.join("raw.jsonl"), config.raw_bytes).unwrap();
        local_probe(config, evidence)
    }
    fn raw(probe: &Probe) -> Vec<Value> {
        std::fs::read_to_string(probe.config.destination.join("raw.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
    fn complete(rows: &[Value], success: bool) {
        let last = rows.last().unwrap();
        assert_eq!(last["type"], "complete");
        assert_eq!(last["successful"], success);
        assert_eq!(last["undrained_inputs"], 0);
        assert_eq!(last["raw_observations"], last["captured_observations"]);
    }
    type ServerSocket = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;
    async fn accept_subscription(listener: &tokio::net::TcpListener) -> ServerSocket {
        let (stream, _) = listener.accept().await.unwrap();
        stream.set_nodelay(true).unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let text = socket.next().await.unwrap().unwrap().into_text().unwrap();
        // Independent assertion of the actual bytes decoded by the fake venue.
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            json!({
                "type":"market", "assets_ids":["A"], "level":2, "initial_dump":true
            })
        );
        socket
    }
    fn book() -> Message {
        Message::Text(
            r#"{"event_type":"book","asset_id":"A","timestamp":"1","bids":[],"asks":[]}"#.into(),
        )
    }
    fn change(quantity: &str) -> Value {
        json!({"event_type":"price_change", "timestamp":"2", "price_changes":[{
            "asset_id":"A", "side":"BUY", "price":"0.50", "size":quantity, "hash":"same-hash"
        }]})
    }
    async fn until_drop(socket: &mut ServerSocket) -> usize {
        let mut heartbeats = 0;
        while let Some(Ok(frame)) = socket.next().await {
            match frame {
                Message::Text(text) => {
                    assert_eq!(text.as_str(), "PING");
                    heartbeats += 1;
                    // The client can drop between receive and this send at cutoff.
                    if socket.send(Message::Text("PONG".into())).await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                Message::Pong(_) => {}
                other => panic!("unexpected client frame: {other:?}"),
            }
        }
        heartbeats
    }
    async fn bounded<F: std::future::Future>(future: F) -> F::Output {
        timeout(Duration::from_secs(5), future)
            .await
            .expect("local fixture deadline")
    }
    fn never_cancel() -> impl std::future::Future<Output = std::io::Result<()>> {
        std::future::pending()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_orchestration_matches_controls_and_preserves_auditable_evidence() {
        let temp = tempfile::tempdir().unwrap();
        // Only this test reads the retention variable. Never overwrites a run.
        let destination = std::env::var_os("PROBE_TEST_EVIDENCE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| temp.path().join("trial"));
        std::fs::create_dir(&destination).unwrap();
        let config = local_config(destination);
        let (listener, network) = local_network(&config).await;
        let mut probe = file_probe(config);
        probe.report.write_all(b"> LOCAL SYNTHETIC TEST ONLY: accelerated 20ms starts, 50ms heartbeats, zero warmup, 1s measurement, 200ms guards. Not latency evidence. Production timing descriptions below do not apply to this fixture.\n\n").unwrap();
        probe.evidence.row(&json!({"type":"metadata", "format_version":1,
            "config":probe.config, "endpoint":network.endpoint,
            "test_only":true, "latency_evidence":false, "timing":"accelerated real monotonic clock, not paused",
            "start_gap_ns":network.start_gap.as_nanos(), "heartbeat_ns":network.heartbeat.as_nanos(),
            "window_ns":probe.matcher.window_ns, "matcher":"three-block-full-content-v1",
            "policy":{"matcher":"three-block-full-content-v1", "matcher_advance_logged":true,"guard_blocks":3,"financial_scale":1_000_000},
            "fixture":"unique 7; repeated 8 twice A/once B; unmatched 9 on A; identical hash and exchange timestamp"})).unwrap();
        let server = async {
            let mut a = accept_subscription(&listener).await;
            let mut b = accept_subscription(&listener).await;
            a.send(book()).await.unwrap();
            b.send(book()).await.unwrap();
            a.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
            // Verify the protocol-level pong, not just the evidence of a ping.
            loop {
                match a.next().await.unwrap().unwrap() {
                    Message::Pong(payload) => {
                        assert_eq!(payload.as_ref(), &[1, 2, 3]);
                        break;
                    }
                    Message::Text(text) => assert_eq!(text.as_str(), "PING"),
                    other => panic!("expected pong: {other:?}"),
                }
            }
            a.send(Message::Pong(vec![4, 5].into())).await.unwrap();
            b.send(Message::Text("PONG".into())).await.unwrap();
            // Middle of the [200,400)ms block; a full guard on either side.
            sleep(Duration::from_millis(300)).await;
            a.send(Message::Text(
                json!([change("7.0000000"), change("8"), change("8"), change("9")])
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            b.send(Message::Text(
                json!([change("7"), change("8.0")]).to_string().into(),
            ))
            .await
            .unwrap();
            let (ha, hb) = tokio::join!(until_drop(&mut a), until_drop(&mut b));
            assert!(
                ha > 0 && hb > 0,
                "both feeds must send application heartbeats"
            );
        };
        let (result, ()) =
            bounded(async { tokio::join!(drive(&mut probe, &network, never_cancel()), server) })
                .await;
        result.unwrap();
        assert_eq!(probe.samples.len(), 1);
        assert_eq!(counts(&probe.matcher, "A").matched, [1, 1]);
        assert_eq!(counts(&probe.matcher, "A").ambiguous, [2, 1]);
        assert_eq!(counts(&probe.matcher, "A").unmatched, [1, 0]);
        let rows = raw(&probe);
        complete(&rows, true);
        assert_eq!(rows[0]["policy"]["matcher_advance_logged"], true);
        let advances: Vec<_> = rows
            .iter()
            .filter(|r| r["type"] == "matcher_advance")
            .collect();
        assert!(!advances.is_empty() && advances.len() <= 5);
        assert!(advances.iter().all(
            |r| r["mono_ns"].as_u64().unwrap() <= probe.measured_start.unwrap() + 1_000_000_000
        ));
        let matched_at = rows.iter().position(|r| r["type"] == "match").unwrap();
        assert_eq!(rows[matched_at - 1]["type"], "matcher_advance");
        assert!(rows.iter().any(|r| r["frame"] == "ping"));
        assert!(rows.iter().any(|r| r["frame"] == "pong"));
        for source in 0..2 {
            let received: Vec<_> = rows
                .iter()
                .filter(|r| r["type"] == "received" && r["source"] == source)
                .collect();
            for (index, row) in received.iter().enumerate() {
                assert_eq!(row["ordinal"], index + 1);
                assert_eq!(row["generation"], 0);
            }
        }
        let attempts: Vec<_> = rows
            .iter()
            .filter(|r| r["details"]["phase"] == "attempt")
            .collect();
        assert_eq!(attempts.len(), 2);
        assert!(
            attempts[1]["stamp"]["mono_ns"].as_u64().unwrap()
                - attempts[0]["stamp"]["mono_ns"].as_u64().unwrap()
                >= network.start_gap.as_nanos() as u64
        );
        assert!(
            std::fs::read_to_string(probe.config.destination.join("report.md"))
                .unwrap()
                .contains("completed bounded trial")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "bounded local load/RSS qualification; run alone, never latency evidence"]
    async fn local_bounded_load_retains_every_offered_event() {
        use std::io::BufRead;
        const BATCHES: usize = 1000;
        const EVENTS_PER_BATCH: usize = 20;
        const PERIOD: Duration = Duration::from_millis(5);
        let temp = tempfile::tempdir().unwrap();
        let destination = std::env::var_os("PROBE_TEST_LOAD_EVIDENCE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| temp.path().join("load"));
        std::fs::create_dir(&destination).unwrap();
        let mut config = local_config(destination);
        config.seconds = 7;
        config.raw_bytes = 64 << 20;
        config.raw_observations = 10_000;
        config.matcher_entries = 100_000;
        config.matcher_bytes = 64 << 20;
        let (listener, mut network) = local_network(&config).await;
        // Heartbeat/control behavior is covered by the short fixture. Retain
        // production heartbeat here so the fake can concentrate on offered load.
        network.heartbeat = Duration::from_secs(10);
        let mut probe = file_probe(config);
        probe.report.write_all(b"> LOCAL SYNTHETIC LOAD TEST ONLY: 20ms starts, zero warmup, 7s measurement, 200ms guards. Not latency evidence. Production timing descriptions below do not apply.\n\n").unwrap();
        probe.evidence.row(&json!({"type":"metadata", "format_version":1,
            "config":probe.config, "endpoint":network.endpoint, "test_only":true, "latency_evidence":false,
            "timing":"real monotonic clock; accelerated starts and measurement; absolute offered schedule",
            "policy":{"matcher":"three-block-full-content-v1", "matcher_advance_logged":true,
                "guard_blocks":3, "financial_scale":1_000_000},
            "load":{"sources":2,"batches_per_source":BATCHES,"events_per_batch":EVENTS_PER_BATCH,
                "period_ns":PERIOD.as_nanos(),"offered_events_per_second_per_source":4000,
                "total_canonical_changes":2*BATCHES*EVENTS_PER_BATCH,"max_generator_lateness_ns":250_000_000},
            "rss_scope":"whole test process including local fake, probe, report and streaming evidence verification"})).unwrap();
        let server = async {
            let mut a = accept_subscription(&listener).await;
            let mut b = accept_subscription(&listener).await;
            a.send(book()).await.unwrap();
            b.send(book()).await.unwrap();
            let first = tokio::time::Instant::now() + Duration::from_millis(500);
            let mut max_lateness = Duration::ZERO;
            for batch in 0..BATCHES {
                let scheduled = first + PERIOD * batch as u32;
                tokio::time::sleep_until(scheduled).await;
                // Unique quantities retain canonical equality across sources,
                // without relying on hashes or timestamps for uniqueness.
                let events: Vec<_> = (0..EVENTS_PER_BATCH)
                    .map(|i| change(&(10 + batch * EVENTS_PER_BATCH + i).to_string()))
                    .collect();
                let frame = Message::Text(serde_json::to_string(&events).unwrap().into());
                a.send(frame.clone()).await.unwrap();
                b.send(frame).await.unwrap();
                max_lateness = max_lateness.max(scheduled.elapsed());
                assert!(
                    max_lateness <= Duration::from_millis(250),
                    "offered-load schedule missed: {max_lateness:?}; cannot claim requested rate"
                );
            }
            tokio::join!(until_drop(&mut a), until_drop(&mut b));
            max_lateness
        };
        let (result, max_lateness) = timeout(Duration::from_secs(15), async {
            tokio::join!(drive(&mut probe, &network, never_cancel()), server)
        })
        .await
        .expect("local load deadline");
        result.unwrap();
        assert_eq!(probe.matcher.resource_resets, 0);
        assert_eq!(probe.observations, (2 * BATCHES + 2) as u64);
        assert_eq!(probe.captured_observations, probe.observations);
        let counts = counts(&probe.matcher, "A");
        for source in 0..2 {
            assert_eq!(counts.seen[source], (BATCHES * EVENTS_PER_BATCH) as u64);
            assert_eq!(
                counts.matched[source]
                    + counts.ambiguous[source]
                    + counts.unmatched[source]
                    + counts.censored[source],
                counts.seen[source]
            );
        }
        assert!(!probe.samples.is_empty());
        // Stream verification: do not inflate the RSS measurement by retaining
        // a second complete parsed copy of the evidence file.
        let path = probe.config.destination.join("raw.jsonl");
        let mut retained_rows = 0;
        let mut received_rows = 0;
        let mut matched_rows = 0;
        let mut last = Value::Null;
        for line in std::io::BufReader::new(File::open(&path).unwrap()).lines() {
            let row: Value = serde_json::from_str(&line.unwrap()).unwrap();
            retained_rows += 1;
            received_rows += u64::from(row["type"] == "received");
            matched_rows += u64::from(row["type"] == "match");
            last = row;
        }
        complete(std::slice::from_ref(&last), true);
        assert_eq!(last["errors"], json!([]));
        assert_eq!(retained_rows, probe.evidence.rows);
        assert_eq!(received_rows, probe.observations);
        assert_eq!(matched_rows, probe.samples.len() as u64);
        let bytes = std::fs::metadata(&path).unwrap().len();
        assert!(bytes <= probe.config.raw_bytes);
        eprintln!(
            "LOCAL LOAD ONLY: offered=4000 changes/s/source, sources=2, total_changes=40000, batches/source=1000, period=5ms; generator_max_lateness_ns={}; retained_rows={retained_rows}; received_rows={received_rows}; matched_pairs={matched_rows}; raw_bytes={bytes}; counts={}; max_queue_age_ns={}; max_processing_ns={}; max_tick_lateness_ns={}",
            max_lateness.as_nanos(),
            serde_json::to_string(counts).unwrap(),
            probe.max_queue_age_ns,
            probe.max_processing_ns,
            probe.max_tick_lateness_ns
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_bad_frames_close_and_eof_are_terminal_without_retries() {
        let cases = [
            (Some(Message::Close(None)), "close"),
            (Some(Message::Binary(vec![1, 2].into())), "binary"),
            (Some(Message::Text("not-json".into())), "JSON"),
            (
                Some(Message::Text(r#"{"event_type":"rejected"}"#.into())),
                "unknown event",
            ),
            (
                Some(Message::Text("x".repeat(MESSAGE_BYTES + 1).into())),
                "Space limit",
            ),
            (None, "reset"),
        ];
        for (frame, expected) in cases {
            let temp = tempfile::tempdir().unwrap();
            let config = local_config(temp.path().into());
            let (listener, mut network) = local_network(&config).await;
            network.start_gap = Duration::from_secs(2);
            let mut probe = file_probe(config);
            let server = async {
                let mut socket = accept_subscription(&listener).await;
                if let Some(frame) = frame {
                    // An oversized message may be rejected while the server is writing.
                    let _ = socket.send(frame).await;
                    until_drop(&mut socket).await;
                }
            };
            let (result, ()) = bounded(async {
                tokio::join!(drive(&mut probe, &network, never_cancel()), server)
            })
            .await;
            let error = format!("{:#}", result.unwrap_err());
            if expected == "JSON" {
                assert!(
                    error.contains("expected") || error.contains("JSON"),
                    "{error}"
                );
            } else {
                assert!(
                    error.to_lowercase().contains(&expected.to_lowercase()),
                    "{error}"
                );
            }
            let rows = raw(&probe);
            complete(&rows, false);
            assert_eq!(
                rows.iter()
                    .filter(|r| r["details"]["phase"] == "attempt")
                    .count(),
                1
            );
            assert!(probe.measured_start.is_none());
            assert!(
                timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_handshake_and_initialization_deadlines_are_terminal() {
        use tokio::io::AsyncReadExt;
        for handshake in [true, false] {
            let temp = tempfile::tempdir().unwrap();
            let config = local_config(temp.path().into());
            let (listener, mut network) = local_network(&config).await;
            network.start_gap = Duration::from_secs(2);
            network.connect_timeout = Duration::from_millis(100);
            network.init_timeout = Duration::from_millis(200);
            let mut probe = file_probe(config);
            let server = async {
                if handshake {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut bytes = Vec::new();
                    socket.read_to_end(&mut bytes).await.unwrap();
                    assert!(bytes.starts_with(b"GET /ws/market HTTP/1.1"));
                } else {
                    let mut socket = accept_subscription(&listener).await;
                    until_drop(&mut socket).await;
                }
            };
            let (result, ()) = bounded(async {
                tokio::join!(drive(&mut probe, &network, never_cancel()), server)
            })
            .await;
            let error = format!("{:#}", result.unwrap_err());
            assert!(
                error.contains(if handshake {
                    "connect deadline"
                } else {
                    "failed complete snapshot coverage"
                }),
                "{error}"
            );
            complete(&raw(&probe), false);
            assert!(probe.ready_ns.iter().all(Option::is_none));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_cancellation_during_handshake_drops_socket_and_pending_start() {
        use tokio::io::AsyncReadExt;
        let temp = tempfile::tempdir().unwrap();
        let config = local_config(temp.path().into());
        let (listener, mut network) = local_network(&config).await;
        network.start_gap = Duration::from_secs(2);
        let mut probe = file_probe(config);
        let (cancel, cancelled) = oneshot::channel();
        let server = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 1024];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
            cancel.send(()).unwrap();
            let mut tail = Vec::new();
            socket.read_to_end(&mut tail).await.unwrap();
        };
        let signal = async {
            cancelled.await.unwrap();
            Ok(())
        };
        let (result, ()) =
            bounded(async { tokio::join!(drive(&mut probe, &network, signal), server) }).await;
        assert!(format!("{:#}", result.unwrap_err()).contains("SIGINT: incomplete trial"));
        let rows = raw(&probe);
        complete(&rows, false);
        assert_eq!(
            rows.iter()
                .filter(|r| r["details"]["phase"] == "attempt")
                .count(),
            1
        );
        assert!(
            timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_renewal_closes_before_replacement_and_resets_generation() {
        for trial in [1, 2] {
            let temp = tempfile::tempdir().unwrap();
            let mut config = local_config(temp.path().into());
            config.cohort = "renewal".into();
            config.trial = trial;
            let treatment = config.treatment();
            let (listener, network) = local_network(&config).await;
            let mut probe = file_probe(config);
            let server = async {
                let mut sockets = [
                    accept_subscription(&listener).await,
                    accept_subscription(&listener).await,
                ];
                for socket in &mut sockets {
                    socket.send(book()).await.unwrap();
                }
                // Treatment must close before its sole replacement is accepted.
                loop {
                    match sockets[treatment].next().await.unwrap().unwrap() {
                        Message::Close(_) => break,
                        Message::Text(text) => assert_eq!(text.as_str(), "PING"),
                        other => panic!("unexpected pre-renewal frame {other:?}"),
                    }
                }
                let mut replacement = accept_subscription(&listener).await;
                replacement.send(book()).await.unwrap();
                let (_, _) = tokio::join!(
                    until_drop(&mut replacement),
                    until_drop(&mut sockets[1 - treatment])
                );
            };
            let (result, ()) = bounded(async {
                tokio::join!(drive(&mut probe, &network, never_cancel()), server)
            })
            .await;
            result.unwrap();
            assert!(probe.renewal_ready_ns.unwrap() >= probe.renewal_start.unwrap());
            let rows = raw(&probe);
            complete(&rows, true);
            assert_eq!(
                rows.iter()
                    .filter(|r| r["type"] == "renewal_cutoff")
                    .count(),
                1
            );
            let attempts: Vec<_> = rows
                .iter()
                .filter(|r| r["details"]["phase"] == "attempt")
                .collect();
            assert_eq!(attempts.len(), 3);
            for pair in attempts.windows(2) {
                assert!(
                    pair[1]["stamp"]["mono_ns"].as_u64().unwrap()
                        - pair[0]["stamp"]["mono_ns"].as_u64().unwrap()
                        >= network.start_gap.as_nanos() as u64
                );
            }
            assert_eq!(attempts[2]["source"], treatment);
            assert_eq!(attempts[2]["generation"], 1);
            let renewed: Vec<_> = rows
                .iter()
                .filter(|r| r["type"] == "received" && r["generation"] == 1)
                .collect();
            assert!(!renewed.is_empty());
            assert_eq!(renewed[0]["ordinal"], 1);
            assert!(
                rows.iter()
                    .any(|r| r["type"] == "all_initial_snapshots_observed" && r["generation"] == 1)
            );
            assert!(
                timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_renewal_refuses_incomplete_peer_coverage() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = local_config(temp.path().into());
        config.cohort = "renewal".into();
        let (listener, network) = local_network(&config).await;
        let mut probe = file_probe(config);
        let server = async {
            let mut a = accept_subscription(&listener).await;
            let mut b = accept_subscription(&listener).await;
            a.send(book()).await.unwrap();
            tokio::join!(until_drop(&mut a), until_drop(&mut b));
        };
        let (result, ()) =
            bounded(async { tokio::join!(drive(&mut probe, &network, never_cancel()), server) })
                .await;
        assert!(
            format!("{:#}", result.unwrap_err())
                .contains("renewal refused: peer coverage incomplete")
        );
        complete(&raw(&probe), false);
        assert!(probe.renewal_start.is_none());
        assert!(
            timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_port_binding_is_actual_and_conflict_is_terminal() {
        for occupied in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = reservation.local_addr().unwrap().port();
            let mut config = local_config(temp.path().into());
            config.cohort = "source-port".into();
            config.local_port = Some(port);
            let (listener, mut network) = local_network(&config).await;
            network.start_gap = Duration::from_secs(2);
            let mut probe = file_probe(config);
            let held = occupied.then_some(reservation);
            let (cancel, cancelled) = oneshot::channel();
            let server = async {
                let mut socket = accept_subscription(&listener).await;
                assert_eq!(socket.get_ref().peer_addr().unwrap().port(), port);
                socket.send(book()).await.unwrap();
                // Receipt of ping/pong establishes that the feed is polling.
                socket.send(Message::Ping(vec![9].into())).await.unwrap();
                loop {
                    if matches!(socket.next().await.unwrap().unwrap(), Message::Pong(_)) {
                        break;
                    }
                }
                cancel.send(()).unwrap();
                until_drop(&mut socket).await;
            };
            let result = if occupied {
                bounded(drive(&mut probe, &network, never_cancel())).await
            } else {
                let signal = async {
                    cancelled.await.unwrap();
                    Ok(())
                };
                let (result, ()) =
                    bounded(async { tokio::join!(drive(&mut probe, &network, signal), server) })
                        .await;
                result
            };
            let error = format!("{:#}", result.unwrap_err());
            assert!(
                error.contains(if occupied {
                    "Address already in use"
                } else {
                    "SIGINT"
                }),
                "{error}"
            );
            complete(&raw(&probe), false);
            if !occupied {
                let rows = raw(&probe);
                let connected = rows
                    .iter()
                    .find(|r| r["details"]["phase"] == "connected")
                    .unwrap();
                assert_eq!(connected["details"]["requested_local_port"], port);
                assert_eq!(
                    connected["details"]["local"]
                        .as_str()
                        .unwrap()
                        .parse::<SocketAddr>()
                        .unwrap()
                        .port(),
                    port
                );
            }
            assert!(
                timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
            drop(held);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_full_ingress_stops_real_feed_without_retry() {
        let temp = tempfile::tempdir().unwrap();
        let config = local_config(temp.path().into());
        let (listener, network) = local_network(&config).await;
        let origin = Instant::now();
        let budget = ConnectionBudget {
            next: Cell::new(origin),
            gap: network.start_gap,
        };
        // Attempt, connected, subscription occupy all three slots. The received
        // book must fail the real feed handoff, never silently disappear.
        let (tx, mut rx) = mpsc::channel(3);
        let server = async {
            let mut socket = accept_subscription(&listener).await;
            socket.send(book()).await.unwrap();
            until_drop(&mut socket).await;
        };
        let (result, ()) = bounded(async {
            tokio::join!(feed(&config, 0, origin, tx, &budget, &network), server)
        })
        .await;
        assert!(format!("{:#}", result.unwrap_err()).contains("probe input full/closed"));
        assert!(matches!(rx.try_recv().unwrap(), Input::Connected { .. }));
        assert!(matches!(rx.try_recv().unwrap(), Input::Connected { .. }));
        assert!(matches!(rx.try_recv().unwrap(), Input::Sent { .. }));
        assert!(rx.try_recv().is_err());
        assert!(
            timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    // A recoverable-on-next-call sink detects unsafe implicit retries, including
    // BufWriter::drop. It injects at the actual buffered writer boundary.
    #[derive(Default)]
    struct OutputState {
        bytes: Vec<u8>,
        fail_after: Option<usize>,
        fail_flush: bool,
        fail_sync: bool,
        failed: bool,
        writes_after_failure: usize,
        delay_once: Option<Duration>,
    }
    #[derive(Clone, Default)]
    struct TestOutput(std::rc::Rc<std::cell::RefCell<OutputState>>);
    impl Write for TestOutput {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let mut state = self.0.borrow_mut();
            if let Some(delay) = state.delay_once.take() {
                std::thread::sleep(delay);
            }
            if state.failed {
                state.writes_after_failure += 1;
            } else if let Some(limit) = state.fail_after {
                if state.bytes.len() >= limit {
                    state.failed = true;
                    return Err(std::io::Error::other("injected write failure"));
                }
                let count = bytes.len().min(limit - state.bytes.len());
                state.bytes.extend_from_slice(&bytes[..count]);
                return Ok(count);
            }
            state.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            let mut state = self.0.borrow_mut();
            if state.fail_flush && !state.failed {
                state.failed = true;
                return Err(std::io::Error::other("injected flush failure"));
            }
            Ok(())
        }
    }
    impl EvidenceSink for TestOutput {
        fn sync_all(&self) -> std::io::Result<()> {
            if self.0.borrow().fail_sync {
                Err(std::io::Error::other("injected sync failure"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn legacy_bufwriter_drop_retries_a_failed_partial_flush() {
        // Regression reproducer for the old writer ownership: failed_write
        // alone cannot prevent the BufWriter destructor from writing again.
        let sink = TestOutput::default();
        sink.0.borrow_mut().fail_after = Some(3);
        let mut writer = BufWriter::with_capacity(32, sink.clone());
        writer.write_all(b"abcdef").unwrap();
        assert!(writer.flush().is_err());
        assert_eq!(sink.0.borrow().bytes, b"abc");
        drop(writer);
        assert_eq!(sink.0.borrow().writes_after_failure, 1);
        assert_eq!(sink.0.borrow().bytes, b"abcdef");
    }

    #[test]
    fn failed_partial_evidence_never_retries_or_appends_success() {
        for direct in [false, true] {
            for prefix in [0, 3] {
                let sink = TestOutput::default();
                sink.0.borrow_mut().fail_after = Some(prefix);
                let mut evidence = Evidence::new(sink.clone(), 1 << 20);
                let row = json!({"payload":"x".repeat(if direct { 100_000 } else { 100 })});
                let result = evidence.row(&row);
                assert_eq!(result.is_err(), direct);
                assert!(
                    evidence
                        .finish(&json!({"type":"complete", "successful":true}))
                        .is_err()
                );
                assert!(evidence.failed_write);
                assert!(evidence.row(&json!({"payload":"must not retry"})).is_err());
                assert!(
                    evidence
                        .finish(&json!({"type":"complete", "successful":false}))
                        .is_err()
                );
                drop(evidence);
                let state = sink.0.borrow();
                assert_eq!(state.bytes.len(), prefix);
                assert_eq!(state.writes_after_failure, 0);
                assert!(!String::from_utf8_lossy(&state.bytes).contains("complete"));
            }
        }
    }

    #[test]
    fn partial_terminal_marker_is_not_retried_into_success() {
        let sink = TestOutput::default();
        let mut evidence = Evidence::new(sink.clone(), 1 << 20);
        evidence.row(&json!({"type":"prefix"})).unwrap();
        let prefix_len = evidence.bytes as usize;
        sink.0.borrow_mut().fail_after = Some(prefix_len + 3);
        assert!(
            evidence
                .finish(&json!({"type":"complete", "successful":true}))
                .is_err()
        );
        drop(evidence);
        let state = sink.0.borrow();
        assert_eq!(state.bytes.len(), prefix_len + 3);
        assert_eq!(state.writes_after_failure, 0);
        let mut lines = state.bytes.split(|b| *b == b'\n');
        assert_eq!(
            serde_json::from_slice::<Value>(lines.next().unwrap()).unwrap()["type"],
            "prefix"
        );
        assert!(serde_json::from_slice::<Value>(lines.next().unwrap()).is_err());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "current_thread")]
    async fn local_report_write_failure_marks_raw_completion_unsuccessful() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = local_config(temp.path().into());
        config.seconds = 0;
        let (listener, network) = local_network(&config).await;
        let mut probe = file_probe(config);
        probe.report = OpenOptions::new().write(true).open("/dev/full").unwrap();
        let server = async {
            let mut a = accept_subscription(&listener).await;
            let mut b = accept_subscription(&listener).await;
            a.send(book()).await.unwrap();
            b.send(book()).await.unwrap();
            tokio::join!(until_drop(&mut a), until_drop(&mut b));
        };
        let (result, ()) =
            bounded(async { tokio::join!(drive(&mut probe, &network, never_cancel()), server) })
                .await;
        assert!(format!("{:#}", result.unwrap_err()).contains("report:"));
        assert!(probe.ready_ns.iter().all(Option::is_some));
        let rows = raw(&probe);
        complete(&rows, false);
        assert!(
            rows.last().unwrap()["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v.as_str().unwrap().starts_with("report:"))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_final_output_failures_cannot_return_success() {
        for failure in ["write", "partial", "flush", "sync"] {
            let temp = tempfile::tempdir().unwrap();
            let mut config = local_config(temp.path().into());
            config.seconds = 0; // Finalization test only, not a measurement.
            let (listener, network) = local_network(&config).await;
            let sink = TestOutput::default();
            {
                let mut state = sink.0.borrow_mut();
                state.fail_after = match failure {
                    "write" => Some(0),
                    "partial" => Some(3),
                    _ => None,
                };
                state.fail_flush = failure == "flush";
                state.fail_sync = failure == "sync";
            }
            let mut probe = local_probe(config, Evidence::new(sink.clone(), 1 << 20));
            let server = async {
                let mut a = accept_subscription(&listener).await;
                let mut b = accept_subscription(&listener).await;
                a.send(book()).await.unwrap();
                b.send(book()).await.unwrap();
                tokio::join!(until_drop(&mut a), until_drop(&mut b));
            };
            let (result, ()) = bounded(async {
                tokio::join!(drive(&mut probe, &network, never_cancel()), server)
            })
            .await;
            let error = format!("{:#}", result.unwrap_err());
            assert!(
                error.contains("evidence finalization") && error.contains("injected"),
                "{error}"
            );
            assert!(probe.ready_ns.iter().all(Option::is_some));
            drop(probe);
            let state = sink.0.borrow();
            assert_eq!(state.writes_after_failure, 0);
            let raw = String::from_utf8_lossy(&state.bytes);
            if failure != "sync" {
                assert!(!raw.contains("\"type\":\"complete\""), "{raw}");
            } else {
                // sync_all may fail AFTER a fully written marker. The marker
                // remains provisional: drive/CLI must fail, not claim durability.
                assert!(raw.contains("\"type\":\"complete\""));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_slow_evidence_write_invalidates_active_trial() {
        let temp = tempfile::tempdir().unwrap();
        let config = local_config(temp.path().into());
        let (_listener, network) = local_network(&config).await;
        let sink = TestOutput::default();
        sink.0.borrow_mut().delay_once = Some(Duration::from_millis(1100));
        let mut evidence = Evidence::new(sink.clone(), 1 << 20);
        // Fill almost all of the real 64KiB buffer so the first connection row
        // forces synchronous physical I/O while feeds are still active.
        evidence
            .row(&json!({"padding":"x".repeat(65_500)}))
            .unwrap();
        let mut probe = local_probe(config, evidence);
        let error = format!(
            "{:#}",
            bounded(drive(&mut probe, &network, never_cancel()))
                .await
                .unwrap_err()
        );
        assert!(
            error.contains("local processing/queue stall exceeds 1s"),
            "{error}"
        );
        assert!(probe.max_processing_ns > 1_000_000_000);
        let bytes = sink.0.borrow().bytes.clone();
        let rows: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        complete(&rows, false);
        assert!(probe.measured_start.is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "current_thread")]
    async fn local_real_sigint_cancels_both_sockets_in_isolated_process() {
        const CHILD: &str = "PROBE_SIGINT_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            // Never send a process-wide signal into the parallel test runner.
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::local_real_sigint_cancels_both_sockets_in_isolated_process",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let start = Instant::now();
            while child.try_wait().unwrap().is_none() {
                if start.elapsed() > Duration::from_secs(10) {
                    child.kill().unwrap();
                    let output = child.wait_with_output().unwrap();
                    panic!(
                        "SIGINT child deadline: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let config = local_config(temp.path().into());
        let (listener, network) = local_network(&config).await;
        let mut probe = file_probe(config);
        let server = async {
            let mut a = accept_subscription(&listener).await;
            let mut b = accept_subscription(&listener).await;
            for socket in [&mut a, &mut b] {
                socket.send(book()).await.unwrap();
                socket.send(Message::Ping(vec![42].into())).await.unwrap();
                loop {
                    if matches!(socket.next().await.unwrap().unwrap(), Message::Pong(_)) {
                        break;
                    }
                }
            }
            assert!(
                std::process::Command::new("kill")
                    .args(["-INT", &std::process::id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
            tokio::join!(until_drop(&mut a), until_drop(&mut b));
        };
        let (result, ()) = bounded(async {
            tokio::join!(drive(&mut probe, &network, tokio::signal::ctrl_c()), server)
        })
        .await;
        assert!(format!("{:#}", result.unwrap_err()).contains("SIGINT: incomplete trial"));
        let rows = raw(&probe);
        complete(&rows, false);
        for source in 0..2 {
            assert!(
                rows.iter()
                    .any(|r| r["type"] == "received" && r["source"] == source)
            );
        }
        assert!(
            timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
}
