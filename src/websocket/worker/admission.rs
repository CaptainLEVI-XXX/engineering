//! Connection admission: a shared start budget and cold-recovery cooldown.
use super::*;

/// One shared administrative limiter per manager. No market-data path takes
/// this mutex. Planned work acquires the smaller pool first, leaving at least
/// one global lease available to mandatory connection attempts.
pub(in crate::websocket) struct ConnectionBudget {
    pub(super) all: Arc<Semaphore>,
    pub(super) planned: Arc<Semaphore>,
    pub(super) next_start: Mutex<Instant>,
    interval: Duration,
    initialization_timeout: Duration,
}

impl ConnectionBudget {
    pub(in crate::websocket) fn new(config: &Config) -> Result<Arc<Self>> {
        config.validate()?;
        Ok(Arc::new(Self {
            all: Arc::new(Semaphore::new(config.connection_concurrency)),
            planned: Arc::new(Semaphore::new(config.connection_concurrency - 1)),
            next_start: Mutex::new(Instant::now()),
            interval: config.connection_interval,
            initialization_timeout: config.initialization_timeout,
        }))
    }

    pub(super) async fn acquire(&self, planned: bool) -> Result<ConnectionLease> {
        let planned_permit = if planned {
            Some(
                self.planned
                    .clone()
                    .acquire_owned()
                    .await
                    .context("planned connection limiter closed")?,
            )
        } else {
            None
        };
        let permit = self
            .all
            .clone()
            .acquire_owned()
            .await
            .context("connection limiter closed")?;
        let mut next = self.next_start.lock().await;
        sleep_until(*next).await;
        let started = Instant::now();
        *next = started
            .checked_add(self.interval)
            .context("connection pacing deadline overflow")?;
        let deadline = started
            .checked_add(self.initialization_timeout)
            .context("initialization deadline overflow")?;
        Ok(ConnectionLease {
            _permit: permit,
            _planned_permit: planned_permit,
            started,
            deadline,
        })
    }
}

pub(super) struct ConnectionLease {
    _permit: OwnedSemaphorePermit,
    _planned_permit: Option<OwnedSemaphorePermit>,
    pub(super) started: Instant,
    pub(super) deadline: Instant,
}

/// Transport recovery also covers attempts that never publish Started, which
/// the processor cannot cool down. Handshakes do not establish full readiness.
#[derive(Default)]
pub(super) struct ColdRecovery {
    pub(super) since: Option<Instant>,
    pub(super) cooldown: Duration,
}

impl ColdRecovery {
    pub(super) fn begin(&mut self, now: Instant) {
        self.since.get_or_insert(now);
    }

    pub(super) fn ready(&mut self) {
        *self = Self::default();
    }

    pub(super) fn retry_delay(&mut self, now: Instant, normal: Duration) -> Duration {
        if self
            .since
            .is_some_and(|since| now.saturating_duration_since(since) >= Duration::from_secs(120))
        {
            self.cooldown = if self.cooldown.is_zero() {
                Duration::from_secs(60)
            } else {
                self.cooldown
                    .saturating_mul(2)
                    .min(Duration::from_secs(900))
            };
            self.cooldown
        } else {
            normal
        }
    }
}
