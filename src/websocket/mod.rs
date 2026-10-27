//! Subscription ownership and raw WebSocket transport (Component 2).
use anyhow::{Result, ensure};
use std::time::Duration;
use tokio::sync::watch;

pub mod ingress;
mod manager;
mod worker;
pub use manager::{AssignmentChange, Assignments, run_manager};
pub use worker::{WorkerCommand, WorkerReport, run_worker};

#[derive(Clone, Debug)]
pub struct Config {
    pub endpoint: String,
    pub assets_per_socket: usize,
    pub max_sockets: usize,
    pub subscription_batch: usize,
    pub max_message_bytes: usize,
    pub io_timeout: Duration,
    pub heartbeat: Duration,
    pub pong_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            assets_per_socket: 1000,
            max_sockets: 1024,
            subscription_batch: 100,
            max_message_bytes: 2 << 20,
            io_timeout: Duration::from_secs(10),
            heartbeat: Duration::from_secs(10),
            pong_timeout: Duration::from_secs(30),
            reconnect_min: Duration::from_millis(250),
            reconnect_max: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(15),
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
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
                self.shutdown_timeout
            ]
            .iter()
            .all(|d| !d.is_zero()),
            "timeouts must be positive"
        );
        ensure!(
            self.reconnect_max >= self.reconnect_min && self.pong_timeout > self.heartbeat,
            "invalid timeout ordering"
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
