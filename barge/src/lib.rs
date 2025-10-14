use flatbuffers::root;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc};
use tokio::{net::UdpSocket, sync::oneshot};
use tracing::{info, warn};

mod message_generated;
use message_generated::barge as fb;

pub struct Proposal {
    pub data: Vec<u8>,
    pub notify: Option<oneshot::Sender<Result<(), ()>>>,
}

#[derive(Clone)]
pub struct Barge {
    propose_tx: mpsc::Sender<Proposal>,
}

impl Barge {
    pub fn new(id: u64, port: u16) -> Self {
        let mut core = BargeCore::new(id, port, vec![]);
        let barge = core.proposer();
        tokio::spawn(async move {
            let _ = core.run().await;
        });
        barge
    }

    pub fn join(id: u64, port: u16, addr: SocketAddrV4) -> Self {
        let mut core = BargeCore::new(id, port, vec![addr]);
        let barge = core.proposer();
        tokio::spawn(async move {
            let _ = core.run().await;
        });
        barge
    }

    fn create(propose_tx: mpsc::Sender<Proposal>) -> Self {
        Self { propose_tx }
    }

    pub async fn propose(&self, data: Vec<u8>) -> anyhow::Result<()> {
        let (notify_tx, notify_rx) = oneshot::channel();
        let proposal = Proposal {
            data,
            notify: Some(notify_tx),
        };
        self.propose_tx.send(proposal).await?;
        info!("Proposal sent, waiting for result...");
        let result = notify_rx.await?;
        info!("Proposal result received");
        result.map_err(|_| anyhow::anyhow!("Proposal failed"))
    }
}

enum BargeRole {
    Follower,
    Leader,
    Candidate,
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
    inflight: BTreeMap<u64, Notify>,
}

impl<'a> BargeCore<'a> {
    fn new(id: u64, port: u16, nodes: Vec<SocketAddrV4>) -> Self {
        let (propose_tx, propose_rx) = mpsc::channel(1024);
        Self {
            role: BargeRole::Leader,
            id,
            propose_tx,
            propose_rx,
            port,
            nodes,
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
        let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.port);
        let socket = UdpSocket::bind(addr).await?;

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
                    self.handle_message(&buf[..len], addr)?;
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

    fn handle_message(&mut self, buf: &[u8], addr: SocketAddr) -> anyhow::Result<()> {
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
            }
            _ => {
                warn!(%addr, "Unknown or unhandled message");
            }
        }

        Ok(())
    }

    async fn propose(
        &mut self,
        proposals: &mut Vec<Proposal>,
        socket: &UdpSocket,
    ) -> anyhow::Result<()> {
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
                prev_index: 0,
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
                notify
                    .send(Ok(()))
                    .map_err(|_| anyhow::anyhow!("Failed to send proposal notification"))?;
            }
        }
        Ok(())
    }
}

// Re-export generated FlatBuffers module so consumers of the library can use it if needed
pub use message_generated::*;
