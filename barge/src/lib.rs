use flatbuffers::root;
use rand::seq::index;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::{net::UdpSocket, sync::oneshot};
use tracing::{info, warn};

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
    pub fn new(id: u64, port: u16) -> Self {
        let (config, barge) = BargeConfig::new(id, port, vec![]);
        tokio::spawn(async move {
            let mut core = BargeCore::new(config);
            let _ = core.run().await;
        });
        barge
    }

    pub fn join(id: u64, port: u16, addr: SocketAddrV4) -> Self {
        let (config, barge) = BargeConfig::new(id, port, vec![addr]);
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
}

struct BargeConfig {
    id: u64,
    port: u16,
    nodes: Vec<SocketAddrV4>,
    propose_tx: mpsc::Sender<Proposal>,
    propose_rx: mpsc::Receiver<Proposal>,
}

impl BargeConfig {
    fn new(id: u64, port: u16, nodes: Vec<SocketAddrV4>) -> (Self, Barge) {
        let (propose_tx, propose_rx) = mpsc::channel(1024);
        let barge = Barge::create(propose_tx.clone());
        let config = Self {
            id,
            port,
            nodes,
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

struct BargeCore<'a> {
    role: BargeRole,
    propose_tx: mpsc::Sender<Proposal>,
    propose_rx: mpsc::Receiver<Proposal>,
    id: u64,
    port: u16,
    nodes: Vec<SocketAddrV4>,
    index: u64,
    builder: flatbuffers::FlatBufferBuilder<'a>,
    inflight: BTreeMap<u64, Inflight>,
}

impl<'a> BargeCore<'a> {
    fn new(config: BargeConfig) -> Self {
        Self {
            role: BargeRole::Leader,
            id: config.id,
            port: config.port,
            nodes: config.nodes,
            propose_tx: config.propose_tx,
            propose_rx: config.propose_rx,
            index: 0,
            builder: flatbuffers::FlatBufferBuilder::new(),
            inflight: BTreeMap::new(),
        }
    }

    fn proposer(&self) -> Barge {
        Barge::create(self.propose_tx.clone())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut buf = [0u8; 65536];
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, self.port)).await?;

        let proposal_limit = 100;
        let mut proposal_buf = Vec::with_capacity(proposal_limit);

        loop {
            tokio::select! {
                _ = self.collect_proposals(&mut proposal_buf, proposal_limit, Duration::from_micros(100)) => {
                    if !proposal_buf.is_empty() {
                        self.propose(&mut proposal_buf, &socket).await?;
                        proposal_buf.clear();
                    }
                }

                Ok((len, addr)) = socket.recv_from(&mut buf) => {
                    self.handle_message(&buf[..len], &addr, &socket).await?;
                }
            }
        }
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

        match event_type {
            fb::Event::ElectionReq => {
                let req = msg.event_as_election_req().unwrap();
                info!(%addr, term = req.term(), "ElectionReq");
            }
            fb::Event::AppendEntriesReq => {
                let req = msg.event_as_append_entries_req().unwrap();
                let num = req.entries().map_or(0, |e| e.len());
                self.index += num as u64;
                info!(%addr, term = req.term(), entries = num, index = self.index, len = buf.len(), "AppendEntriesReq");
                // immediately reply with success for now
                self.handle_append_entries_request(&req, addr, socket)
                    .await?;
            }
            fb::Event::AppendEntriesRes => {
                let res = msg.event_as_append_entries_res().unwrap();
                info!(%addr, term = res.term(), index = res.append_index(), success = res.success(), "AppendEntriesRes");
                self.handle_append_entries_response(&res).await?;
            }
            _ => {
                warn!(%addr, "Unknown or unhandled message");
            }
        }

        Ok(())
    }

    async fn handle_append_entries_request(
        &mut self,
        req: &fb::AppendEntriesReq<'_>,
        addr: &SocketAddr,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        let success = true; // TODO: actually handle log replication
        let append_index = req.prev_index() + req.entries().map_or(0, |e| e.len()) as u64;
        let reply = fb::AppendEntriesRes::create(
            &mut self.builder,
            &fb::AppendEntriesResArgs {
                term: req.term(),
                append_index,
                commit_index: 0,
                success,
            },
        );
        let msg = fb::Message::create(
            &mut self.builder,
            &fb::MessageArgs {
                sender: self.id,
                timestamp: 0,
                event_type: fb::Event::AppendEntriesRes,
                event: Some(reply.as_union_value()),
            },
        );
        self.builder.finish(msg, None);
        socket.send_to(self.builder.finished_data(), addr).await?;
        self.builder.reset();
        Ok(())
    }

    async fn handle_append_entries_response(
        &mut self,
        res: &fb::AppendEntriesRes<'_>,
    ) -> anyhow::Result<()> {
        if !res.success() {
            warn!("AppendEntriesRes indicates failure");
            return Ok(());
        }
        let append_index = res.append_index();
        info!("AppendEntriesRes for index {}", append_index);
        // for entry in .. if count = len(nodes) + 1 then notify success and remove from inflight
        if let Some(inflight) = self.inflight.get_mut(&append_index) {
            inflight.count += 1;
            if inflight.count >= self.nodes.len() + 1 {
                let notify = std::mem::replace(&mut inflight.notify, None);
                if let Some(notify) = notify {
                    let _ = notify.send(Ok(()));
                }
                self.inflight.remove(&append_index);
            }
        }
        Ok(())
    }

    async fn propose(
        &mut self,
        proposals: &mut Vec<Proposal>,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        if self.role != BargeRole::Leader {
            for proposal in proposals.iter_mut() {
                let notify = std::mem::replace(&mut proposal.notify, None);
                if let Some(notify) = notify {
                    let _ = notify.send(Err(BargeError::NotLeader {
                        leader: None, // TODO: populate with real leader hint
                    }));
                }
            }
            return Ok(());
        }

        let entries: Vec<_> = proposals
            .iter()
            .map(|p| {
                let data = self.builder.create_vector(&p.data);
                fb::LogEntry::create(
                    &mut self.builder,
                    &fb::LogEntryArgs {
                        term: 0,
                        index: 0,
                        data: Some(data),
                    },
                )
            })
            .collect();

        let entries = self.builder.create_vector(&entries);

        let append_entries_req = fb::AppendEntriesReq::create(
            &mut self.builder,
            &fb::AppendEntriesReqArgs {
                term: 0,
                commit_index: 0,
                prev_index: self.index,
                prev_term: 0,
                entries: Some(entries),
            },
        );
        let msg = fb::Message::create(
            &mut self.builder,
            &fb::MessageArgs {
                sender: self.id,
                timestamp: 0,
                event_type: fb::Event::AppendEntriesReq,
                event: Some(append_entries_req.as_union_value()),
            },
        );
        self.builder.finish(msg, None);
        for node in &self.nodes {
            info!(
                "Sending {} bytes to {}...",
                self.builder.finished_data().len(),
                node
            );
            let _len = socket.send_to(self.builder.finished_data(), node).await?;
        }
        self.builder.reset();

        for proposal in proposals.iter_mut() {
            let notify = std::mem::replace(&mut proposal.notify, None);
            if let Some(notify) = notify {
                let inflight = Inflight::new(notify);
                self.index += 1;
                self.inflight.insert(self.index, inflight);
            }
        }
        Ok(())
    }
}

// Re-export generated FlatBuffers module so consumers of the library can use it if needed
pub use message_generated::*;
