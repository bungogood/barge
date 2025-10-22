use crate::fb;
use crate::store::LogStore;
use flatbuffers::root;
use rand::Rng;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::{net::ToSocketAddrs, net::UdpSocket, sync::oneshot};
use tracing::{error, info, warn};

pub(crate) struct Proposal {
    pub(crate) control: bool,
    pub(crate) data: Vec<u8>,
    pub(crate) notify: Option<oneshot::Sender<BargeResult<()>>>,
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
    pub fn new(id: Option<u16>, dir: PathBuf, addr: SocketAddrV4) -> Self {
        let (config, barge) = BargeConfig::new(dir, id, addr, vec![]);
        tokio::spawn(async move {
            let mut core = BargeCore::new(config);
            let _ = core.run().await;
        });
        barge
    }

    pub fn join(
        id: Option<u16>,
        dir: PathBuf,
        addr: SocketAddrV4,
        join_addrs: Vec<SocketAddrV4>,
    ) -> Self {
        let (config, barge) = BargeConfig::new(dir, id, addr, join_addrs);
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
            control: false,
            data,
            notify: Some(notify_tx),
        };
        self.propose_tx
            .send(proposal)
            .await
            .map_err(|e| BargeError::Transport(format!("send error: {}", e)))?;
        let result = notify_rx
            .await
            .map_err(|e| BargeError::Internal(format!("oneshot recv error: {}", e)))?;
        result
    }
}

struct BargeConfig {
    dir: PathBuf,
    id: Option<u16>,
    addr: SocketAddrV4,
    bootstrap_nodes: Vec<SocketAddrV4>,
    propose_tx: mpsc::Sender<Proposal>,
    propose_rx: mpsc::Receiver<Proposal>,
}

impl BargeConfig {
    fn new(
        dir: PathBuf,
        id: Option<u16>,
        addr: SocketAddrV4,
        bootstrap_nodes: Vec<SocketAddrV4>,
    ) -> (Self, Barge) {
        let (propose_tx, propose_rx) = mpsc::channel(1024);
        let barge = Barge::create(propose_tx.clone());
        let config = Self {
            dir,
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

type NodeId = u16;

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

struct BargeCore<'a> {
    append_buf: Vec<u8>,
    store: LogStore,
    bootstrap_nodes: Vec<SocketAddrV4>,
    consts: BargeConstants,
    propose_tx: mpsc::Sender<Proposal>,
    propose_rx: mpsc::Receiver<Proposal>,
    addr: SocketAddrV4,
    builder: flatbuffers::FlatBufferBuilder<'a>,
    inflight: BTreeMap<u64, Inflight>,
    votes_received: usize,
    election_deadline: Option<tokio::time::Instant>,
}

impl<'a> BargeCore<'a> {
    fn new(config: BargeConfig) -> Self {
        let consts = BargeConstants::new();
        let nodes = BTreeMap::from_iter(
            config.bootstrap_nodes.iter().map(|&addr| (addr, 0u64)), // next index starts at 0
        );

        let store = match LogStore::init(&config.dir, None) {
            Ok(store) => {
                info!("Using exsiting LogStore: {:?}", store.metadata());
                store
            }
            Err(_) => {
                let mut store = LogStore::init(
                    &config.dir,
                    Some(fb::NodeDetails::new(
                        config.id.unwrap_or(0),
                        config.addr.ip().to_bits(),
                        config.addr.port(),
                        false,
                    )),
                )
                .expect("failed to create new store");
                info!("Creating new LogStore");
                store
                // LogStore::new(&config.dir, id).unwrap()
            }
        };

        Self {
            append_buf: Vec::with_capacity(65536),
            store,
            bootstrap_nodes: config.bootstrap_nodes,
            consts,
            addr: config.addr,
            propose_tx: config.propose_tx,
            propose_rx: config.propose_rx,
            votes_received: 0,
            builder: flatbuffers::FlatBufferBuilder::new(),
            inflight: BTreeMap::new(),
            election_deadline: None,
        }
    }

    fn proposer(&self) -> Barge {
        Barge::create(self.propose_tx.clone())
    }

    async fn start_up(&mut self, socket: &UdpSocket, buf: &mut [u8]) -> anyhow::Result<()> {
        if self.store.role() != fb::Role::Pending {
            return Ok(());
        }

        let details = fb::NodeDetails::new(
            self.store.id(),
            self.addr.ip().to_bits(),
            self.addr.port(),
            false,
        );

        let req = fb::JoinReq::create(
            &mut self.builder,
            &fb::JoinReqArgs {
                prev_index: 0,
                prev_term: 0,
                node: Some(&details),
            },
        );
        build_message(
            &mut self.builder,
            fb::Event::JoinReq,
            req.as_union_value(),
            self.store.id(),
        );

        for node in self.bootstrap_nodes.iter() {
            socket.send_to(self.builder.finished_data(), node).await?;
        }
        self.builder.reset();
        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut buf = [0u8; 65536];
        let socket = UdpSocket::bind(self.addr).await?;
        let socket = Arc::new(socket);

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
                    self.handle_message(&buf[..len], to_ipv4(addr), socket.clone()).await?;
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
        self.votes_received = 1;
        self.store.start_election()?;
        info!("Becoming candidate for term {}", self.store.term());
        let election_req = fb::ElectionReq::create(
            &mut self.builder,
            &fb::ElectionReqArgs {
                term: self.store.term(),
                candidate_id: self.store.id(),
                prev_index: self.store.append_index(),
                prev_term: self.store.term(), // TODO: unsure why its required
            },
        );

        self.broadcast_message(
            fb::Event::ElectionReq,
            election_req.as_union_value(),
            socket,
        )
        .await?;
        self.builder.reset();
        self.election_deadline = Some(self.consts.rand_election_deadline());
        Ok(())
    }

    // TODO maybe return an error
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
        addr: SocketAddrV4,
        socket: Arc<UdpSocket>,
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
        addr: SocketAddrV4,
        socket: Arc<UdpSocket>,
    ) -> anyhow::Result<()> {
        let req = msg.event_as_election_req().unwrap();
        let vote_granted = if self.store.term() < req.term() {
            info!(%addr, term = req.term(), "Updating term from ElectionReq");
            self.store.vote_for(req.term(), req.candidate_id())?;
            self.election_deadline = Some(self.consts.rand_election_deadline());
            true
        } else {
            warn!(%addr, term = req.term(), "Stale ElectionReq");
            false
        };
        let res = fb::ElectionRes::create(
            &mut self.builder,
            &fb::ElectionResArgs {
                term: self.store.term(),
                vote_granted,
            },
        );
        self.send_message(fb::Event::ElectionRes, res.as_union_value(), &socket, &addr)
            .await?;
        self.builder.reset();
        Ok(())
    }

    fn handle_election_res(
        &mut self,
        msg: &fb::Message<'_>,
        addr: SocketAddrV4,
    ) -> anyhow::Result<()> {
        let res = msg.event_as_election_res().unwrap();
        if self.store.role() != fb::Role::Candidate {
            warn!(%addr, "Received ElectionRes but not a candidate");
            return Ok(());
        } else if self.store.term() < res.term() {
            info!(%addr, term = res.term(), "Updating term from ElectionRes");
            self.store.new_term(res.term(), msg.sender())?;
            self.election_deadline = Some(self.consts.rand_election_deadline());
            return Ok(());
        } else if self.store.term() > res.term() {
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
        addr: SocketAddrV4,
        socket: Arc<UdpSocket>,
    ) -> anyhow::Result<()> {
        let req = msg.event_as_append_entries_req().unwrap();
        let mut success = true;
        if self.store.term() > req.term() {
            warn!(%addr, term = req.term(), "Stale AppendEntriesReq");
            success = false;
        } else if self.store.term() < req.term() {
            info!(%addr, term = req.term(), "Updating term from AppendEntriesReq");
            self.store.new_term(req.term(), msg.sender())?;
        }

        // TODO might need to unset the previous one
        if success {
            // self.leader = self.nodes.get_mut(&msg.sender()).unwrap().role = fb::Role::Leader;
            success = self
                .store
                .append_entries(req.entries(), req.data().bytes())
                .is_ok();
        }

        let res = fb::AppendEntriesRes::create(
            &mut self.builder,
            &fb::AppendEntriesResArgs {
                term: self.store.term(),
                append_index: self.store.append_index(),
                commit_index: self.store.commit_index(),
                success,
            },
        );
        self.send_message(
            fb::Event::AppendEntriesRes,
            res.as_union_value(),
            &socket,
            &addr,
        )
        .await?;
        self.builder.reset();
        Ok(())
    }

    fn handle_append_entries_res(
        &mut self,
        msg: &fb::Message<'_>,
        addr: SocketAddrV4,
    ) -> anyhow::Result<()> {
        let res = msg.event_as_append_entries_res().unwrap();
        if !res.success() {
            warn!("AppendEntriesRes indicates failure");
            return Ok(());
        }
        // let quorum = self.quorum();
        // let sender = self
        //     .nodes
        //     .get_mut(&msg.sender())
        //     .ok_or_else(|| anyhow::anyhow!("AppendEntriesRes from unknown node: {}", addr))?;
        // info!("AppendEntriesRes for index {}", res.append_index());
        // // for entry in .. if count = len(nodes) + 1 then notify success and remove from inflight
        // if voting_role(sender.role) {
        //     for i in sender.append_index + 1..=res.append_index() {
        //         if let Some(inflight) = self.inflight.get_mut(&i) {
        //             inflight.count += 1;
        //             if inflight.count >= quorum {
        //                 self.store.commit_entries(i as u64)?;
        //                 let notify = std::mem::replace(&mut inflight.notify, None);
        //                 if let Some(notify) = notify {
        //                     info!("Proposal for index {} committed", i);
        //                     let _ = notify.send(Ok(()));
        //                 }
        //                 self.inflight.remove(&i);
        //             }
        //         }
        //     }
        // };
        // sender.append_index = res.append_index();
        // sender.commit_index = res.commit_index();
        Ok(())
    }

    async fn handle_join_req(
        &mut self,
        msg: &fb::Message<'_>,
        addr: SocketAddrV4,
        socket: Arc<UdpSocket>,
    ) -> anyhow::Result<()> {
        let req = msg.event_as_join_req().unwrap();
        let node = req.node(); // TODO: This could error

        let pending = if !self.store.is_leader() {
            info!(%addr, "Requested to join cluster");
            false
        } else if self.store.is_member(node) {
            info!(%addr, "Requested to join cluster");
            true
        } else if self.store.is_pending(node) {
            info!(%addr, "Requested to join cluster");
            true
        } else {
            info!(%addr, "Requested to join cluster");
            let add_node = fb::AddNode::create(
                &mut self.builder,
                &fb::AddNodeArgs {
                    node: Some(&node.clone()),
                },
            );
            let control = fb::ControlEntry::create(
                &mut self.builder,
                &fb::ControlEntryArgs {
                    control_type: fb::ControlMessage::AddNode,
                    control: Some(add_node.as_union_value()),
                },
            );
            self.builder.finish(control, None);
            let proposal = Proposal {
                control: true,
                data: self.builder.finished_data().to_vec(),
                notify: None,
            };
            self.builder.reset();
            self.propose_tx.send(proposal).await.is_ok();
            self.store.add_pending(node.clone());
            true
        };

        let res = fb::JoinAck::create(
            &mut self.builder,
            &fb::JoinAckArgs {
                pending,
                leader: self.store.leader().as_ref(),
            },
        );
        build_message(
            &mut self.builder,
            fb::Event::JoinRes,
            res.as_union_value(),
            self.store.id(),
        );
        socket
            .send_to(self.builder.finished_data(), addr)
            .await
            .unwrap();
        self.builder.reset();

        Ok(())
    }

    async fn handle_join_res(
        &mut self,
        msg: &fb::Message<'_>,
        addr: SocketAddrV4,
        _socket: Arc<UdpSocket>,
    ) -> anyhow::Result<()> {
        let res = msg.event_as_join_res().unwrap();
        info!(%addr, "Accepted as Join Learner ID: {}", res.id());
        self.store.become_learner(res.id());
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

        let (prev_index, prev_term) = self.store.prev_index_term();
        let (entries, data) = self.store.append_proposals(proposals).unwrap();
        let entries = self.builder.create_vector(entries);
        let data = self.builder.create_vector(data);

        if self.store.only_node() {
            self.store.commit_entries(self.store.append_index())?;
            for proposal in proposals.iter_mut() {
                let notify = std::mem::replace(&mut proposal.notify, None);
                if let Some(notify) = notify {
                    notify.send(Ok(()));
                }
            }
            return Ok(());
        }

        let req = fb::AppendEntriesReq::create(
            &mut self.builder,
            &fb::AppendEntriesReqArgs {
                term: 0,
                commit_index: 0,
                prev_index,
                prev_term,
                entries: Some(entries),
                data: Some(data),
            },
        );

        self.broadcast_message(fb::Event::AppendEntriesReq, req.as_union_value(), socket)
            .await?;
        self.builder.reset();

        for (offset, proposal) in proposals.iter_mut().enumerate() {
            let notify = std::mem::replace(&mut proposal.notify, None);
            if let Some(notify) = notify {
                let inflight = Inflight::new(notify);
                let idx = prev_index + offset as u64 + 1;
                self.inflight.insert(idx, inflight);
            }
        }
        Ok(())
    }

    async fn broadcast_message(
        &mut self,
        event_type: fb::Event,
        event: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
        build_message(&mut self.builder, event_type, event, self.store.id());
        for (_id, addr) in self.store.iter_nodes() {
            socket.send_to(self.builder.finished_data(), addr).await?;
        }
        Ok(())
    }

    async fn send_message(
        &mut self,
        event_type: fb::Event,
        event: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
        socket: &UdpSocket,
        addr: &impl ToSocketAddrs,
    ) -> anyhow::Result<()> {
        build_message(&mut self.builder, event_type, event, self.store.id());
        socket.send_to(self.builder.finished_data(), addr).await?;
        Ok(())
    }
}

fn build_message(
    builder: &mut flatbuffers::FlatBufferBuilder,
    event_type: fb::Event,
    event: flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
    sender: NodeId,
) {
    let msg = fb::Message::create(
        builder,
        &fb::MessageArgs {
            sender,
            timestamp: timestamp_nanos(),
            event_type,
            event: Some(event.as_union_value()),
        },
    );
    builder.finish(msg, None);
}

fn to_ipv4(addr: SocketAddr) -> SocketAddrV4 {
    match addr {
        SocketAddr::V4(addr) => addr,
        SocketAddr::V6(_) => unimplemented!("IPv6 Not Supported"),
    }
}

fn timestamp_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}
