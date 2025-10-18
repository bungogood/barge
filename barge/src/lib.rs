use flatbuffers::root;
use rand::Rng;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::{net::UdpSocket, sync::oneshot};
use tracing::{error, info, warn};

mod message_generated;
use message_generated::barge as fb;

pub struct Proposal {
    pub data: Vec<u8>,
    pub notify: Option<oneshot::Sender<BargeResult<()>>>,
}

/// Errors returned by barge operations such as `Barge::propose`.
#[derive(Debug)]
pub enum BargeError {
    /// Request rejected because this node is not the leader.
    /// `leader` may contain an optional hint (e.g. leader URL or address).
    NotLeader { leader: Option<String> },
    /// Transport or channel error when sending a proposal to the runtime.
    Transport(String),
    /// Internal error (e.g. notification channel closed).
    Internal(String),
}

type BargeResult<T> = Result<T, BargeError>;

impl std::fmt::Display for BargeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BargeError::NotLeader { leader } => match leader {
                Some(l) => write!(f, "not leader; leader hint={}", l),
                None => write!(f, "not leader"),
            },
            BargeError::Transport(e) => write!(f, "transport error: {}", e),
            BargeError::Internal(e) => write!(f, "internal error: {}", e),
        }
    }
}

#[derive(Clone)]
pub struct Barge {
    propose_tx: mpsc::Sender<Proposal>,
}

impl Barge {
    pub fn new(addr: SocketAddrV4) -> Self {
        let id = uuid::Uuid::new_v4();
        let (config, barge) = BargeConfig::new(id, addr, vec![]);
        tokio::spawn(async move {
            let mut core = BargeCore::new(config);
            let _ = core.run().await;
        });
        barge
    }

    pub fn join(addr: SocketAddrV4, join_addrs: Vec<SocketAddrV4>) -> Self {
        let id = uuid::Uuid::new_v4();
        let (config, barge) = BargeConfig::new(id, addr, join_addrs);
        tokio::spawn(async move {
            let mut core = BargeCore::new(config);
            let _ = core.run().await;
        });
        barge
    }

    fn create(propose_tx: mpsc::Sender<Proposal>) -> Self {
        Self { propose_tx }
    }

    pub async fn propose(&self, data: Vec<u8>) -> BargeResult<()> {
        let (notify_tx, notify_rx) = oneshot::channel();
        let proposal = Proposal {
            data,
            notify: Some(notify_tx),
        };
        self.propose_tx
            .send(proposal)
            .await
            .map_err(|e| BargeError::Transport(format!("send error: {}", e)))?;
        info!("Proposal sent, waiting for result...");
        let result = notify_rx
            .await
            .map_err(|e| BargeError::Internal(format!("oneshot recv error: {}", e)))?;
        info!("Proposal result received");
        result
    }
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum BargeRole {
    Follower,
    Leader,
    Candidate,
    Learner,
    Pending,
}

struct BargeConfig {
    id: uuid::Uuid,
    addr: SocketAddrV4,
    bootstrap_nodes: Vec<SocketAddrV4>,
    propose_tx: mpsc::Sender<Proposal>,
    propose_rx: mpsc::Receiver<Proposal>,
}

impl BargeConfig {
    fn new(
        id: uuid::Uuid,
        addr: SocketAddrV4,
        bootstrap_nodes: Vec<SocketAddrV4>,
    ) -> (Self, Barge) {
        let (propose_tx, propose_rx) = mpsc::channel(1024);
        let barge = Barge::create(propose_tx.clone());
        let config = Self {
            id,
            addr,
            bootstrap_nodes,
            propose_tx,
            propose_rx,
        };
        (config, barge)
    }
}

struct Inflight {
    count: usize,
    notify: Option<oneshot::Sender<BargeResult<()>>>,
}

impl Inflight {
    fn new(notify: oneshot::Sender<BargeResult<()>>) -> Self {
        Self {
            count: 1,
            notify: Some(notify),
        }
    }
}

type NodeId = u64;

struct BargeConstants {
    election_timeout_min: u64,
    election_timeout_max: u64,
    heartbeat_interval: u64,
}

impl BargeConstants {
    fn new() -> Self {
        Self {
            election_timeout_min: 1000,
            election_timeout_max: 3000,
            heartbeat_interval: 50,
        }
    }

    fn rand_election_duration(&self) -> Duration {
        let mut rng = rand::rng();
        let millis = rng.random_range(self.election_timeout_min..self.election_timeout_max);
        Duration::from_millis(millis)
    }

    fn rand_election_deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.rand_election_duration()
    }

    fn heartbeat_duration(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval)
    }
}

struct NodeState {
    id: uuid::Uuid,
    role: BargeRole,
    addr: SocketAddrV4,
    append_index: u64,
    commit_index: u64,
}

impl NodeState {
    pub fn details(&self) -> fb::NodeDetails {
        fb::NodeDetails::new(
            &uuid_to_fb(self.id),
            self.addr.ip().to_bits(),
            self.addr.port(),
        )
    }
}

struct BargeCore<'a> {
    bootstrap_nodes: Vec<SocketAddrV4>,
    consts: BargeConstants,
    role: BargeRole,
    propose_tx: mpsc::Sender<Proposal>,
    propose_rx: mpsc::Receiver<Proposal>,
    id: uuid::Uuid,
    addr: SocketAddrV4,
    nodes: BTreeMap<uuid::Uuid, NodeState>, // node address to next index
    term: u64,
    append_index: u64,
    commit_index: u64,
    builder: flatbuffers::FlatBufferBuilder<'a>,
    inflight: BTreeMap<u64, Inflight>,
    votes_received: usize,
    voted_for: Option<uuid::Uuid>,
    election_deadline: Option<tokio::time::Instant>,
}

impl<'a> BargeCore<'a> {
    fn new(config: BargeConfig) -> Self {
        let consts = BargeConstants::new();
        let nodes = BTreeMap::from_iter(
            config.bootstrap_nodes.iter().map(|&addr| (addr, 0u64)), // next index starts at 0
        );
        let role = if nodes.is_empty() {
            BargeRole::Leader
        } else {
            BargeRole::Pending
        };
        Self {
            bootstrap_nodes: config.bootstrap_nodes,
            consts,
            role,
            id: config.id,
            addr: config.addr,
            nodes: BTreeMap::new(),
            propose_tx: config.propose_tx,
            propose_rx: config.propose_rx,
            term: 0,
            append_index: 0,
            commit_index: 0,
            votes_received: 0,
            builder: flatbuffers::FlatBufferBuilder::new(),
            inflight: BTreeMap::new(),
            voted_for: None,
            election_deadline: None,
        }
    }

    fn proposer(&self) -> Barge {
        Barge::create(self.propose_tx.clone())
    }

    async fn start_up(&mut self, socket: &UdpSocket, buf: &mut [u8]) -> anyhow::Result<()> {
        if self.role != BargeRole::Pending {
            return Ok(());
        }

        let id = uuid_to_fb(self.id);
        let details = fb::NodeDetails::new(&id, self.addr.ip().to_bits(), self.addr.port());

        let req = fb::JoinReq::create(
            &mut self.builder,
            &fb::JoinReqArgs {
                prev_index: 0,
                prev_term: 0,
                node: Some(&details),
            },
        );
        self.build_message(fb::Event::JoinReq, req.as_union_value());

        for node in self.bootstrap_nodes.iter() {
            socket.send_to(self.builder.finished_data(), node).await?;
        }
        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut buf = [0u8; 65536];
        let socket = UdpSocket::bind(self.addr).await?;

        self.start_up(&socket, &mut buf).await?;

        let proposal_limit = 1;
        let mut proposal_buf = Vec::with_capacity(proposal_limit);

        loop {
            let deadline = self.election_deadline;
            tokio::select! {
                _ = self.collect_proposals(&mut proposal_buf, proposal_limit, Duration::from_micros(250)) => {
                    if !proposal_buf.is_empty() {
                        self.propose(&mut proposal_buf, &socket).await?;
                        proposal_buf.clear();
                    }
                }

                Ok((len, addr)) = socket.recv_from(&mut buf) => {
                    self.handle_message(&buf[..len], &addr, &socket).await?;
                }

                _ = Self::election_wait(deadline) => {
                    self.start_election(&socket).await?;
                }
            }
        }
    }

    async fn election_wait(deadline: Option<tokio::time::Instant>) {
        match deadline {
            Some(d) => tokio::time::sleep_until(d).await,
            None => futures::future::pending::<()>().await,
        }
    }

    async fn start_election(&mut self, socket: &UdpSocket) -> anyhow::Result<()> {
        self.term += 1;
        self.role = BargeRole::Candidate;
        self.votes_received = 1;
        self.voted_for = Some(self.id);
        info!("Becoming candidate for term {}", self.term);
        let election_req = fb::ElectionReq::create(
            &mut self.builder,
            &fb::ElectionReqArgs {
                term: self.term,
                candidate_id: Some(&uuid_to_fb(self.id)),
                prev_index: self.append_index,
                prev_term: self.term,
            },
        );
        self.build_message(fb::Event::ElectionReq, election_req.as_union_value());

        for (_, node) in &self.nodes {
            info!("Sending ElectionReq to {}", node.addr);
            let _len = socket
                .send_to(self.builder.finished_data(), node.addr)
                .await?;
        }
        self.builder.reset();
        self.election_deadline = Some(self.consts.rand_election_deadline());
        Ok(())
    }

    async fn collect_proposals(&mut self, buf: &mut Vec<Proposal>, max: usize, timeout: Duration) {
        buf.clear();
        let deadline = Instant::now() + timeout;

        if let Some(p) = self.propose_rx.recv().await {
            buf.push(p);
        } else {
            return;
        }

        while buf.len() < max {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            let recv_fut = self.propose_rx.recv();
            match tokio::time::timeout(remaining, recv_fut).await {
                Ok(Some(p)) => buf.push(p),
                _ => break,
            }
        }
    }

    async fn handle_message(
        &mut self,
        buf: &[u8],
        addr: &SocketAddr,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        let msg = root::<fb::Message>(buf)?;
        let event_type = msg.event_type();
        let skt = socket;

        match event_type {
            fb::Event::ElectionReq => self.handle_election_req(&msg, addr, skt).await?,
            fb::Event::ElectionRes => self.handle_election_res(&msg, addr)?,
            fb::Event::AppendEntriesReq => self.handle_append_entries_req(&msg, addr, skt).await?,
            fb::Event::AppendEntriesRes => self.handle_append_entries_res(&msg, addr)?,
            fb::Event::JoinReq => self.handle_join_req(&msg, addr, skt).await?,
            fb::Event::JoinRes => self.handle_join_res(&msg, addr, skt).await?,
            _ => error!(%addr, ?event_type, "Unknown or unhandled message"),
        }

        Ok(())
    }

    async fn handle_election_req(
        &mut self,
        msg: &fb::Message<'_>,
        addr: &SocketAddr,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        let req = msg.event_as_election_req().unwrap();
        let vote_granted = if self.term < req.term() {
            info!(%addr, term = req.term(), "Updating term from ElectionReq");
            self.term = req.term();
            self.role = BargeRole::Follower;
            self.election_deadline = Some(self.consts.rand_election_deadline());
            true
        } else {
            warn!(%addr, term = req.term(), "Stale ElectionReq");
            false
        };
        let res = fb::ElectionRes::create(
            &mut self.builder,
            &fb::ElectionResArgs {
                term: self.term,
                vote_granted,
            },
        );
        self.build_message(Event::ElectionRes, res.as_union_value());
        socket.send_to(self.builder.finished_data(), addr).await?;
        Ok(())
    }

    fn handle_election_res(
        &mut self,
        msg: &fb::Message<'_>,
        addr: &SocketAddr,
    ) -> anyhow::Result<()> {
        let res = msg.event_as_election_res().unwrap();
        if self.role != BargeRole::Candidate {
            warn!(%addr, "Received ElectionRes but not a candidate");
            return Ok(());
        } else if self.term < res.term() {
            info!(%addr, term = res.term(), "Updating term from ElectionRes");
            self.term = res.term();
            self.role = BargeRole::Follower;
            self.election_deadline = Some(self.consts.rand_election_deadline());
            return Ok(());
        } else if self.term > res.term() {
            warn!(%addr, term = res.term(), "Stale ElectionRes");
            return Ok(());
        } else if res.vote_granted() {
            self.votes_received += 1;
            info!(%addr, term = res.term(), vote_granted = res.vote_granted(), "ElectionRes");
        } else {
            warn!(%addr, term = res.term(), vote_granted = res.vote_granted(), "ElectionRes");
        }
        Ok(())
    }

    async fn handle_append_entries_req(
        &mut self,
        msg: &fb::Message<'_>,
        addr: &SocketAddr,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        let req = msg.event_as_append_entries_req().unwrap();
        if self.term > req.term() {
            warn!(%addr, term = req.term(), "Stale AppendEntriesReq");
            return Ok(());
        } else if self.term < req.term() {
            info!(%addr, term = req.term(), "Updating term from AppendEntriesReq");
            self.term = req.term();
            self.role = BargeRole::Follower;
        }

        let num = req.entries().map_or(0, |e| e.len());
        self.append_index += num as u64;
        info!(%addr, term = req.term(), entries = num, index = self.append_index, prev = req.prev_index(), "AppendEntriesReq");
        let success = true; // TODO: actually handle log replication
        let append_index = req.prev_index() + req.entries().map_or(0, |e| e.len()) as u64;
        let res = fb::AppendEntriesRes::create(
            &mut self.builder,
            &fb::AppendEntriesResArgs {
                term: req.term(),
                append_index,
                commit_index: 0,
                success,
            },
        );
        self.build_message(fb::Event::AppendEntriesRes, res.as_union_value());
        socket.send_to(self.builder.finished_data(), addr).await?;
        self.builder.reset();
        Ok(())
    }

    fn handle_append_entries_res(
        &mut self,
        msg: &fb::Message<'_>,
        addr: &SocketAddr,
    ) -> anyhow::Result<()> {
        let res = msg.event_as_append_entries_res().unwrap();
        if !res.success() {
            warn!("AppendEntriesRes indicates failure");
            return Ok(());
        }
        let num_nodes = self.nodes.len() + 1; // +1 for self
        let sender = fb_to_uuid(msg.sender().unwrap());
        let sender = self
            .nodes
            .get_mut(&sender)
            .ok_or_else(|| anyhow::anyhow!("AppendEntriesRes from unknown node: {}", addr))?;
        let append_index = res.append_index();
        info!("AppendEntriesRes for index {}", append_index);
        // for entry in .. if count = len(nodes) + 1 then notify success and remove from inflight
        for i in sender.append_index + 1..=append_index {
            if let Some(inflight) = self.inflight.get_mut(&i) {
                inflight.count += 1;
                if inflight.count >= num_nodes {
                    let notify = std::mem::replace(&mut inflight.notify, None);
                    if let Some(notify) = notify {
                        info!("Proposal for index {} committed", i);
                        let _ = notify.send(Ok(()));
                    }
                    self.inflight.remove(&i);
                }
            }
        }
        Ok(())
    }

    async fn handle_join_req(
        &mut self,
        msg: &fb::Message<'_>,
        addr: &SocketAddr,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        let req = msg.event_as_join_req().unwrap();
        let node = req.node().unwrap(); // TODO: This could error
        let node_addr = SocketAddrV4::new(Ipv4Addr::from_bits(node.ip()), node.port());
        let id = fb_to_uuid(node.uuid());
        info!(%addr, "Has Joined as a Learner");
        self.nodes.insert(
            id,
            NodeState {
                id,
                role: BargeRole::Learner,
                addr: node_addr,
                append_index: req.prev_index(),
                commit_index: req.prev_index(),
            },
        );
        let leader = self.leader().map(|l| l.details());
        let res = fb::JoinRes::create(
            &mut self.builder,
            &fb::JoinResArgs {
                success: true,
                leader: leader.as_ref(),
                message: None,
            },
        );
        self.build_message(Event::JoinRes, res.as_union_value());
        socket.send_to(self.builder.finished_data(), addr).await?;
        Ok(())
    }

    async fn handle_join_res(
        &mut self,
        msg: &fb::Message<'_>,
        addr: &SocketAddr,
        _socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        let res = msg.event_as_join_res().unwrap();
        if res.success() {
            info!(%addr, "Accepted as Join Learner");
            self.role = BargeRole::Learner;
        } else {
            warn!(%addr, "Join Request Failed");
        }
        Ok(())
    }

    async fn propose(
        &mut self,
        proposals: &mut Vec<Proposal>,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        info!(
            "Proposing {} entries: {}",
            proposals.len(),
            self.propose_rx.len()
        );

        let entries: Vec<_> = proposals
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let entry_index = self.append_index + i as u64 + 1;
                let data = self.builder.create_vector(&p.data);
                let cmd =
                    fb::Command::create(&mut self.builder, &fb::CommandArgs { data: Some(data) });
                fb::LogEntry::create(
                    &mut self.builder,
                    &fb::LogEntryArgs {
                        term: self.term,
                        index: entry_index,
                        data_type: fb::Entry::Command,
                        data: Some(cmd.as_union_value()),
                    },
                )
            })
            .collect();

        let entries = self.builder.create_vector(&entries);

        let req = fb::AppendEntriesReq::create(
            &mut self.builder,
            &fb::AppendEntriesReqArgs {
                term: 0,
                commit_index: 0,
                prev_index: self.append_index,
                prev_term: 0,
                entries: Some(entries),
            },
        );
        self.build_message(fb::Event::AppendEntriesReq, req.as_union_value());
        for (_, node) in self.nodes.iter() {
            info!(
                "Sending {} bytes to {}...",
                self.builder.finished_data().len(),
                node.addr
            );
            let _len = socket
                .send_to(self.builder.finished_data(), node.addr)
                .await?;
        }
        self.builder.reset();

        for proposal in proposals.iter_mut() {
            let notify = std::mem::replace(&mut proposal.notify, None);
            if let Some(notify) = notify {
                let inflight = Inflight::new(notify);
                self.append_index += 1;
                self.inflight.insert(self.append_index, inflight);
            }
        }
        Ok(())
    }

    fn leader(&self) -> Option<&NodeState> {
        self.nodes
            .iter()
            .find(|(_, state)| state.role == BargeRole::Leader)
            .map(|(_, state)| state)
    }

    fn build_message(
        &mut self,
        event_type: fb::Event,
        event: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
    ) {
        let msg = fb::Message::create(
            &mut self.builder,
            &fb::MessageArgs {
                sender: Some(&uuid_to_fb(self.id)),
                timestamp: timestamp_nanos(),
                event_type,
                event: Some(event.as_union_value()),
            },
        );
        self.builder.finish(msg, None);
    }
}

fn timestamp_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

fn fb_to_uuid(id: &fb::Uuid) -> uuid::Uuid {
    uuid::Uuid::from_bytes(id.bytes().into())
}

fn uuid_to_fb(id: uuid::Uuid) -> fb::Uuid {
    fb::Uuid::new(id.as_bytes())
}

// Re-export generated FlatBuffers module so consumers of the library can use it if needed
pub use message_generated::*;

use crate::message_generated::barge::Event;
