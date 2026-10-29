//! One connected session: the receive/command/heartbeat select loop. Receive
//! timestamps are taken here, on complete-message delivery, before any parse.
use super::*;

/// Why an intact session ended. Only an owner-requested restart is followed by
/// a best-effort unsubscribe; every other end closes and reconnects directly.
#[derive(Debug)]
pub(super) enum SessionEnd {
    OwnerRestart(&'static str),
    Ended(String),
}
impl SessionEnd {
    pub(super) fn reason(&self) -> &str {
        match self {
            Self::OwnerRestart(reason) => reason,
            Self::Ended(reason) => reason,
        }
    }
}

impl<P: WireProtocol + Clone> Worker<P> {
    pub(super) async fn stream(&mut self, socket: &mut Socket) -> Result<SessionEnd> {
        let config = self.config.clone();
        let protocol = self.protocol.clone();
        let mut requested = HashSet::new();
        let mut removed = HashSet::new();
        let mut pending_subscriptions: Vec<_> = self.desired.keys().cloned().collect();
        let mut pending_unsubscriptions = Vec::new();
        pending_subscriptions.sort();
        let mut initial = true;
        let mut requests_complete = false;
        let mut readiness_ambiguous = false;
        let mut heartbeat = interval_at(Instant::now() + config.heartbeat, config.heartbeat);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_pong = Instant::now();
        self.report.requested_at_cutoff = 0;
        let result = 'session: loop {
            if *self.stop.borrow() {
                break Ok(SessionEnd::Ended("shutdown".into()));
            }
            if self.desired.is_empty() && pending_unsubscriptions.is_empty() {
                break Ok(SessionEnd::Ended("no assignments".into()));
            }
            if self
                .lease
                .as_ref()
                .is_some_and(|lease| Instant::now() >= lease.deadline)
            {
                self.expire_lease()?;
            }
            if !requests_complete
                && pending_subscriptions.is_empty()
                && pending_unsubscriptions.is_empty()
                && self.commands.is_empty()
            {
                ensure!(
                    requested.len() == self.desired.len()
                        && self.desired.keys().all(|id| requested.contains(id)),
                    "request-complete marker without complete desired request set"
                );
                self.output.send(ingress::Message::RequestsComplete {
                    session: self.session,
                })?;
                requests_complete = true;
            }
            let lease_deadline = self
                .lease
                .as_ref()
                .map_or(last_pong, |lease| lease.deadline);
            tokio::select! {
                _ = stopping(&mut self.stop) => continue,
                _ = async { sleep_until(lease_deadline).await }, if self.lease.is_some() => self.expire_lease()?,
                incoming = socket.next() => {
                    let received_at = Utc::now(); // Complete-message delivery, before parse.
                    let received_mono = std::time::Instant::now();
                    let message = match incoming {
                        Some(Ok(message)) => message,
                        // The oversized message was never accepted. Ending this
                        // session publishes a boundary for its books and keeps
                        // every other socket collecting; reconnect is backed off.
                        Some(Err(tungstenite::Error::Capacity(error))) => break 'session Ok(SessionEnd::Ended(format!("message exceeded transport limit: {error}"))),
                        other => break 'session Ok(SessionEnd::Ended(format!("connection interrupted: {other:?}"))),
                    };
                    match message {
                        Message::Text(text) if protocol.classify(&text) == ControlFrame::ApplicationPong => last_pong = Instant::now(),
                        Message::Text(text) => {
                            self.report.messages = self.report.messages.checked_add(1)
                                .context("received message counter exhausted")?;
                            self.emit(text, received_at, received_mono)?;
                        }
                        Message::Ping(_) => {
                            if let Err(error) = io(socket.flush(), self.write_timeout(), &mut self.stop).await {
                                break 'session Ok(SessionEnd::Ended(format!("control pong failed: {error}")));
                            }
                        }
                        Message::Pong(_) => {},
                        Message::Close(_) => {
                            break 'session Ok(SessionEnd::Ended("remote close".into()));
                        }
                        _ => anyhow::bail!("unexpected data-bearing WebSocket message"),
                    }
                }
                command = self.commands.recv() => {
                    let command = command.context("assignment owner closed")?;
                    if !command.add.is_empty() || !command.remove.is_empty() {
                        // Initialized carries only a generation, not a marker
                        // revision. After a published marker is invalidated, an
                        // old in-flight acknowledgement is indistinguishable
                        // from a fresh one: retain the lease until timeout/end.
                        if requests_complete && self.lease.is_some() && !readiness_ambiguous {
                            readiness_ambiguous = true;
                            eprintln!("connection_readiness_ambiguous session={:?} reason=assignment_changed_after_marker lease_release=timeout_or_end", self.session);
                        }
                        requests_complete = false;
                    }
                    // Persist intent BEFORE any fallible send.
                    pending_subscriptions.extend(command.add.iter().filter(|id| !self.desired.contains_key(*id)).cloned());
                    self.update(&command)?;
                    pending_subscriptions.retain(|id| self.desired.contains_key(id));
                    for id in &command.remove {
                        if removed.insert(id.clone()) && requested.contains(id) {
                            pending_unsubscriptions.push((id.clone(), command.revision));
                        }
                    }
                    if command.initialized == Some(self.session) && command.restart != Some(self.session)
                        && requests_complete && !readiness_ambiguous {
                        if self.lease.as_ref().is_some_and(|lease| Instant::now() >= lease.deadline) {
                            self.expire_lease()?;
                        }
                        self.initialized()?;
                    }
                    if command.restart == Some(self.session) {
                        self.next_planned = command.planned_restart;
                        break 'session Ok(SessionEnd::OwnerRestart(if command.planned_restart {
                            "processor granted planned renewal"
                        } else { "processor requested fresh baseline" }));
                    }
                    if command.add.iter().any(|id| removed.contains(id)) || removed.len() >= config.assets_per_socket {
                        break 'session Ok(SessionEnd::Ended("assignment reactivation requires fresh connection".into()));
                    }
                }
                // One batch per iteration gives reception, heartbeat and shutdown
                // another polling opportunity. A single write can still stall reads
                // until io_timeout; uncertain writes always end this session.
                _ = tokio::task::yield_now(), if !pending_unsubscriptions.is_empty() => {
                    let count = config.subscription_batch.min(pending_unsubscriptions.len());
                    let (removals, revisions): (Vec<_>, Vec<_>) = pending_unsubscriptions.drain(..count).unzip();
                    let payload = protocol.subscription(SubscriptionOp::Remove, &removals)?;
                    if let Err(error) = io(socket.send(Message::Text(payload.into())), self.write_timeout(), &mut self.stop).await {
                        break 'session Ok(SessionEnd::Ended(format!("unsubscribe uncertain: {error}")));
                    }
                    for id in &removals { requested.remove(id); }
                    self.output.send(ingress::Message::SubscriptionsRequested {
                        session: self.session, revisions, add: vec![], remove: removals,
                    })?;
                }
                _ = tokio::task::yield_now(), if !pending_subscriptions.is_empty() && pending_unsubscriptions.is_empty() => {
                    let count = config.subscription_batch.min(pending_subscriptions.len());
                    let additions: Vec<_> = pending_subscriptions.drain(..count).collect();
                    let payload = protocol.subscription(if initial { SubscriptionOp::Initial } else { SubscriptionOp::Add }, &additions)?;
                    if let Err(error) = io(socket.send(Message::Text(payload.into())), self.write_timeout(), &mut self.stop).await {
                        break 'session Ok(SessionEnd::Ended(format!("subscribe uncertain: {error}")));
                    }
                    initial = false;
                    requested.extend(additions.iter().cloned());
                    let revisions = additions.iter().map(|id| self.desired[id]).collect();
                    self.output.send(ingress::Message::SubscriptionsRequested {
                        session: self.session, revisions, add: additions, remove: vec![],
                    })?;
                }
                _ = heartbeat.tick() => {
                    let Some(heartbeat) = protocol.heartbeat() else { continue };
                    if last_pong.elapsed() > config.pong_timeout { break 'session Ok(SessionEnd::Ended("application heartbeat timeout".into())); }
                    if let Err(error) = io(socket.send(Message::Text(heartbeat.into())), self.write_timeout(), &mut self.stop).await {
                        break 'session Ok(SessionEnd::Ended(format!("heartbeat write failed: {error}")));
                    }
                }
            }
        };
        // Include shutdown that interrupted a write, not only the receive loop.
        if *self.stop.borrow() {
            self.report.requested_at_cutoff = requested.len();
        }
        result
    }
}
