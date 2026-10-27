//! Subscription ownership and raw WebSocket transport (Component 2).
use anyhow::{Result, ensure};
use std::time::Duration;
use tokio::sync::watch;

pub mod ingress;
mod manager;
pub mod protocol;
mod worker;
pub use manager::{Assignment, AssignmentChange, Assignments, run_manager};
#[cfg(test)]
pub(crate) use worker::run_worker_with_protocol;
pub use worker::{WorkerCommand, WorkerReport, run_worker, run_worker_session};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryRequest {
    Restart {
        session: ingress::Session,
        planned: bool,
    },
    Initialized(ingress::Session),
}

#[derive(Clone, Debug)]
pub struct Config {
    pub endpoint: String,
    pub assets_per_socket: usize,
    pub max_sockets: usize,
    pub replicas_per_market: u16,
    pub lifecycle_hints: bool,
    pub subscription_batch: usize,
    pub max_message_bytes: usize,
    pub io_timeout: Duration,
    pub heartbeat: Duration,
    pub pong_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    pub shutdown_timeout: Duration,
    pub connection_concurrency: usize,
    pub connection_interval: Duration,
    pub initialization_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            assets_per_socket: 1000,
            max_sockets: 1024,
            replicas_per_market: 1,
            lifecycle_hints: false,
            subscription_batch: 100,
            max_message_bytes: 2 << 20,
            io_timeout: Duration::from_secs(10),
            heartbeat: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(30),
            reconnect_min: Duration::from_millis(250),
            reconnect_max: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(15),
            connection_concurrency: 2,
            connection_interval: Duration::from_secs(1),
            initialization_timeout: Duration::from_secs(30),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            matches!(self.replicas_per_market, 1 | 2),
            "replicas_per_market must be 1 or 2"
        );
        ensure!(
            (2..=tokio::sync::Semaphore::MAX_PERMITS).contains(&self.connection_concurrency),
            "connection_concurrency must allow at least one mandatory and one voluntary lease"
        );
        ensure!(
            self.assets_per_socket
                .checked_mul(self.max_sockets)
                .is_some(),
            "socket capacity arithmetic overflow"
        );
        ensure!(
            self.assets_per_socket > 0
                && self.max_sockets > 0
                && self.subscription_batch > 0
                && self.max_message_bytes > 0,
            "budgets must be positive"
        );
        ensure!(
            [
                self.io_timeout,
                self.heartbeat,
                self.pong_timeout,
                self.reconnect_min,
                self.shutdown_timeout,
                self.connection_interval,
                self.initialization_timeout,
            ]
            .iter()
            .all(|d| !d.is_zero()),
            "timeouts must be positive"
        );
        ensure!(
            self.reconnect_max >= self.reconnect_min && self.pong_timeout > self.heartbeat,
            "invalid timeout ordering"
        );
        let now = std::time::Instant::now();
        ensure!(
            [self.connection_interval, self.initialization_timeout]
                .iter()
                .all(|duration| now.checked_add(*duration).is_some()),
            "connection deadline overflow"
        );
        Ok(())
    }
}

pub(crate) async fn stopping(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
