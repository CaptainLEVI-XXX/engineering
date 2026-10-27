//! Venue-owned text framing. Identity namespaces remain distinct from wire formats.
use crate::discovery::AdapterId;
use anyhow::Result;
use serde_json::json;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::Request};

#[derive(Clone, Copy)]
pub enum SubscriptionOp {
    Initial,
    Add,
    Remove,
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ControlFrame {
    ApplicationPong,
    Data,
}

pub trait WireProtocol {
    fn handshake_request(&self, endpoint: &str) -> Result<Request<()>>;
    fn subscription(&self, operation: SubscriptionOp, assets: &[String]) -> Result<String>;
    fn heartbeat(&self) -> Option<&'static str>;
    fn classify(&self, text: &str) -> ControlFrame;
}

#[derive(Clone, Copy)]
pub enum Protocol {
    Polymarket,
    PolymarketLifecycle,
}
pub fn protocol(adapter: AdapterId) -> Protocol {
    protocol_with_lifecycle(adapter, false)
}
pub fn protocol_with_lifecycle(adapter: AdapterId, lifecycle_hints: bool) -> Protocol {
    match adapter {
        AdapterId::PolymarketV1 | AdapterId::PolymarketV2 => {
            if lifecycle_hints {
                Protocol::PolymarketLifecycle
            } else {
                Protocol::Polymarket
            }
        }
    }
}
impl WireProtocol for Protocol {
    fn handshake_request(&self, endpoint: &str) -> Result<Request<()>> {
        Ok(endpoint.into_client_request()?)
    }
    fn subscription(&self, operation: SubscriptionOp, assets: &[String]) -> Result<String> {
        let mut payload = match operation {
            SubscriptionOp::Initial => {
                json!({"type":"market","assets_ids":assets,"level":2,"initial_dump":true})
            }
            SubscriptionOp::Add => json!({"operation":"subscribe","assets_ids":assets,"level":2}),
            SubscriptionOp::Remove => json!({"operation":"unsubscribe","assets_ids":assets}),
        };
        if matches!(self, Self::PolymarketLifecycle) && !matches!(operation, SubscriptionOp::Remove)
        {
            payload["custom_feature_enabled"] = true.into();
        }
        Ok(payload.to_string())
    }
    fn heartbeat(&self) -> Option<&'static str> {
        Some("PING")
    }
    fn classify(&self, text: &str) -> ControlFrame {
        if text == "PONG" {
            ControlFrame::ApplicationPong
        } else {
            ControlFrame::Data
        }
    }
}
