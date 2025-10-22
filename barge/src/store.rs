use crate::message_generated::barge::{ControlEntry, NodeDetails};
use crate::{Proposal, fb};

use flatbuffers::{UnionWIPOffset, Vector, WIPOffset, root};
use memmap2::{MmapMut, MmapOptions};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::ops::{Deref, DerefMut};
use std::slice;
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    usize,
};
use tracing::{debug, error, info, warn};

const METADATA_FILE: &str = "metadata.bin";
const INDEX_FILE: &str = "index.bin";
const DATA_FILE: &str = "data.bin";

const IE: usize = size_of::<fb::IndexEntry>();

type NodeId = u16;

struct NodeState {
    id: NodeId,
    addr: SocketAddrV4,
    promoted: bool,
    append_index: u64,
    commit_index: u64,
}

impl NodeState {
    pub fn details(&self) -> fb::NodeDetails {
        fb::NodeDetails::new(
            self.id,
            self.addr.ip().to_bits(),
            self.addr.port(),
            self.promoted,
        )
    }
}

struct VolatileState {
    replay: bool,
    id: NodeId,
    role: fb::Role,
    leader: Option<NodeId>,
    nodes: BTreeMap<NodeId, NodeState>,
    pending_nodes: Vec<NodeDetails>,
}

impl VolatileState {
    fn apply_control(&mut self, control: &fb::ControlEntry) {
        let control_type = control.control_type();
        match control_type {
            fb::ControlMessage::AddNode => {
                let add_node = control.control_as_add_node().unwrap();
                let node = add_node.node();
                if self.id == node.id() {
                    info!("Added self");
                    return;
                }
                self.nodes.insert(
                    node.id(),
                    NodeState {
                        id: node.id(),
                        addr: SocketAddrV4::new(Ipv4Addr::from_bits(node.ip()), node.port()),
                        promoted: false,
                        append_index: 0,
                        commit_index: 0,
                    },
                );
            }
            fb::ControlMessage::PromoteNode => {
                let promote_node = control.control_as_promote_node().unwrap();
                if promote_node.id() == self.id {
                    info!("Promoted");
                    self.role = fb::Role::Follower;
                    return;
                }
                match self.nodes.get_mut(&promote_node.id()) {
                    Some(node) => node.promoted = true,
                    None => error!("Cannot promote node {}: Not Found", promote_node.id()),
                }
            }
            fb::ControlMessage::RemoveNode => {
                let remove_node = control.control_as_promote_node().unwrap();
                if remove_node.id() == self.id {
                    warn!("This node has been removed (node {})", remove_node.id())
                } else if self.nodes.remove(&remove_node.id()).is_none() {
                    error!("Cannot remove node {}: Not Found", remove_node.id())
                } else {
                    info!("Node removed (node {})", remove_node.id())
                }
            }
            _ => error!(?control_type, "Unknown or unhandled control message"),
        }
    }
}

pub struct LogStore {
    metadata: MmapMetadata,
    index_log: MmapLog,
    data_log: MmapLog,
    volatile: VolatileState,
}

impl LogStore {
    pub fn init(dir: &PathBuf, new_details: Option<NodeDetails>) -> io::Result<Self> {
        if new_details.is_some() {
            fs::create_dir_all(dir)?;
        }

        let metadata = MmapMetadata::init(dir.join(METADATA_FILE), new_details.is_some())?;
        let index_log = MmapLog::init(dir.join(INDEX_FILE), new_details.is_some())?;
        let data_log = MmapLog::init(dir.join(DATA_FILE), new_details.is_some())?;

        let volatile = VolatileState {
            replay: true,
            id: 0,
            role: fb::Role::Pending,
            leader: None,
            nodes: BTreeMap::new(),
            pending_nodes: Vec::new(),
        };

        let mut store = Self {
            metadata,
            index_log,
            data_log,
            volatile,
        };

        store.set_mmap_ptrs();
        if let Some(details) = new_details {
            store.set_new_cluster_messages(details)?;
        }
        Ok(store)
    }

    fn set_mmap_ptrs(&mut self) {
        let index_ptr = self.metadata.append_index() as usize * IE;
        self.index_log.set_ptr(index_ptr);
        if let Some(last) = self.get_index_entry(self.metadata.append_index() - 1) {
            let data_ptr = last.offset() as usize + last.size() as usize;
            self.data_log.set_ptr(data_ptr);
        }
    }

    fn set_new_cluster_messages(&mut self, details: fb::NodeDetails) -> io::Result<()> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let mut num_messages = 0;
        let add_node = fb::AddNode::create(
            &mut builder,
            &fb::AddNodeArgs {
                node: Some(&details),
            },
        );
        self.add_new_cluster_control(
            &mut builder,
            fb::ControlMessage::AddNode,
            add_node.as_union_value(),
        )?;
        num_messages += 1;
        let promote_node =
            fb::PromoteNode::create(&mut builder, &fb::PromoteNodeArgs { id: details.id() });
        self.add_new_cluster_control(
            &mut builder,
            fb::ControlMessage::PromoteNode,
            promote_node.as_union_value(),
        )?;
        num_messages += 1;
        self.data_log.flush()?;
        self.index_log.flush()?;
        self.metadata.set_append_index(num_messages);
        self.commit_entries(num_messages)?;
        self.metadata.set_term(1);
        self.become_leader();
        self.metadata.flush()?;
        Ok(())
    }

    fn add_new_cluster_control(
        &mut self,
        builder: &mut flatbuffers::FlatBufferBuilder,
        control_type: fb::ControlMessage,
        control: WIPOffset<UnionWIPOffset>,
    ) -> io::Result<()> {
        let control_add_node = fb::ControlEntry::create(
            builder,
            &fb::ControlEntryArgs {
                control_type,
                control: Some(control),
            },
        );
        builder.finish(control_add_node, None);
        self.append_data(true, builder.finished_data())?;
        builder.reset();
        Ok(())
    }

    fn replay(&mut self) {
        for idx in 0..=self.commit_index() {
            self.apply(idx);
        }
        self.volatile.replay = false;
    }

    fn apply(&mut self, idx: u64) {
        let entry = self.get_index_entry(idx).expect("Cannot Get Index Entry");

        let entry_data = self
            .data_log
            .get(entry.offset() as usize, entry.size() as usize)
            .unwrap();

        if entry.control() {
            let control = root::<fb::ControlEntry>(entry_data).unwrap();
            self.volatile.apply_control(&control);
        };
    }

    pub fn append_entries<'a>(
        &mut self,
        entires: Vector<'a, fb::IndexEntry>,
        data: &[u8],
    ) -> io::Result<()> {
        assert!(!entires.is_empty(), "Appending Empty Entries");
        let next_append_index = self.append_index() + entires.len() as u64;
        self.index_log.extend_from_slice(entires.bytes())?;
        self.data_log.extend_from_slice(data)?;
        self.index_log.flush()?;
        self.data_log.flush()?;
        self.metadata.set_append_index(next_append_index);
        self.metadata.flush()
    }

    pub fn append_proposals(
        &mut self,
        proposals: &Vec<Proposal>,
    ) -> io::Result<(&[fb::IndexEntry], &[u8])> {
        let append_index = self.append_index();
        let next_append_index = append_index + proposals.len() as u64;
        info!(%append_index, %next_append_index, "Appending Entries");
        for proposal in proposals {
            self.append_data(proposal.control, &proposal.data)?;
        }
        self.index_log.flush()?;
        self.data_log.flush()?;
        self.metadata.set_append_index(next_append_index);
        self.metadata.flush()?;

        Ok(self
            .get_entries(append_index, proposals.len() as u64)
            .unwrap())
    }

    pub fn append_data(&mut self, control: bool, data: &[u8]) -> io::Result<()> {
        let entry_index = self.append_index() + 1;
        let entry_term = self.term();
        let sender = self.id();
        let offset = self.data_log.ptr;

        self.data_log.extend_from_slice(data)?;
        let entry = self.extend_entry_mut()?;
        entry.set_index(entry_index);
        entry.set_term(entry_term);
        entry.set_sender(sender);
        entry.set_control(control);
        entry.set_offset(offset as u64);
        entry.set_size(data.len() as u32);

        log_entry(entry, data, "Appending");

        Ok(())
    }

    pub fn commit_entries(&mut self, index: u64) -> io::Result<()> {
        let prev_commit = self.metadata.commit_index();
        info!(%prev_commit, commit_index = index, append_index = self.append_index(), "Committing");
        let (entries, data) = self
            .get_entries(prev_commit, index - prev_commit)
            .expect("Fetching Committed Entries");
        for entry in entries {
            // find its data
            let buf =
                &data[entry.offset() as usize..entry.offset() as usize + entry.size() as usize];
            log_entry(entry, buf, "Committing");
        }
        self.metadata.set_commit_index(index);
        self.metadata.flush()
    }

    pub fn start_election(&mut self) -> io::Result<()> {
        let next_term = self.term() + 1;
        let vote = self.id();
        self.volatile.role = fb::Role::Candidate;
        self.volatile.leader = Some(self.id());
        self.metadata.set_term(next_term);
        self.metadata.set_voted_for(vote);
        self.metadata.flush()
    }

    pub fn vote_for(&mut self, term: u64, candidate: NodeId) -> io::Result<()> {
        if self.volatile.role == fb::Role::Leader || self.volatile.role == fb::Role::Candidate {
            self.volatile.role = fb::Role::Follower;
        }
        self.metadata.set_term(term);
        self.metadata.set_voted_for(candidate);
        self.metadata.flush()
    }

    pub fn new_term(&mut self, term: u64, id: NodeId) -> io::Result<()> {
        if self.volatile.role == fb::Role::Leader || self.volatile.role == fb::Role::Candidate {
            self.volatile.role = fb::Role::Follower;
        }
        self.volatile.leader = Some(id);
        self.metadata.set_term(term);
        self.metadata.flush()
    }

    pub fn become_leader(&mut self) {
        info!("Become Leader");
        self.volatile.role = fb::Role::Leader;
    }

    pub fn become_learner(&mut self, id: NodeId) {
        self.volatile.role = fb::Role::Learner;
        self.volatile.id = id;
    }

    pub fn is_member(&self, joiner: &fb::NodeDetails) -> bool {
        self.volatile
            .nodes
            .iter()
            .find(|(_, n)| n.addr.ip().to_bits() == joiner.ip() && n.addr.port() == joiner.port())
            .is_some()
    }

    pub fn is_pending(&self, joiner: &fb::NodeDetails) -> bool {
        self.volatile
            .pending_nodes
            .iter()
            .find(|n| n.ip() == joiner.ip() && n.port() == joiner.port())
            .is_some()
    }

    pub fn add_pending(&mut self, pending: fb::NodeDetails) {
        self.volatile.pending_nodes.push(pending);
    }

    pub fn is_leader(&self) -> bool {
        match self.volatile.leader {
            Some(id) => self.volatile.id == id,
            None => false,
        }
    }

    pub fn id(&self) -> NodeId {
        self.volatile.id
    }

    pub fn role(&self) -> fb::Role {
        self.volatile.role
    }

    pub fn voted_for(&self) -> NodeId {
        self.metadata.voted_for()
    }

    pub fn term(&self) -> u64 {
        self.metadata.term()
    }

    pub fn commit_index(&self) -> u64 {
        self.metadata.commit_index()
    }

    pub fn append_index(&self) -> u64 {
        self.metadata.append_index()
    }

    pub fn leader(&self) -> Option<fb::NodeDetails> {
        self.volatile
            .leader
            .map(|l| self.volatile.nodes[&l].details())
    }

    pub fn metadata(&self) -> &fb::Metadata {
        &self.metadata
    }

    pub fn only_node(&self) -> bool {
        self.volatile.nodes.is_empty()
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = (NodeId, SocketAddrV4)> {
        self.volatile.nodes.values().map(|v| (v.id, v.addr))
    }

    pub fn prev_index_term(&self) -> (u64, u64) {
        match self.get_index_entry(self.append_index()) {
            Some(entry) => (entry.index(), entry.term()),
            None => (0, 0),
        }
    }

    fn get_index_entry<'a>(&'a self, idx: u64) -> Option<&'a fb::IndexEntry> {
        if idx == 0 {
            return None;
        }
        let offset = (idx - 1) as usize * IE;
        self.index_log.get_fixed::<IE>(offset).map(|buf| buf.into())
    }

    fn get_index<'a>(&'a self, idx: u64) -> Option<(&'a fb::IndexEntry, &'a [u8])> {
        self.get_index_entry(idx).and_then(|entry| {
            self.data_log
                .get(entry.offset() as usize, entry.size() as usize)
                .map(|data| (entry, data))
        })
    }

    pub fn get_entries<'a>(
        &'a self,
        offset: u64,
        num: u64,
    ) -> Option<(&'a [fb::IndexEntry], &'a [u8])> {
        if offset + num > self.append_index() {
            return None;
        }

        let (index_start, data_start) = if offset == 0 {
            (0, 0)
        } else {
            let first = self.get_index_entry(offset).unwrap();
            (offset as usize * IE, first.offset() as usize)
        };

        let index_size = num as usize * IE;
        let index_bytes = self
            .index_log
            .get(index_start, index_size)
            .expect("Getting Logged Index Block");

        assert!(index_bytes.len() % IE == 0);
        let index_ptr = index_bytes.as_ptr() as *const fb::IndexEntry;
        let index_len = index_bytes.len() / IE;
        let entires = unsafe { slice::from_raw_parts(index_ptr, index_len) };

        let last = self
            .get_index_entry(offset + num)
            .expect("Getting Logged Index");
        let data_size = last.offset() as usize + last.size() as usize - data_start as usize;
        let data = self
            .data_log
            .get(data_start, data_size)
            .expect("Getting Logged Data Block");

        Some((entires, data))
    }

    fn extend_entry_mut<'a>(&'a mut self) -> io::Result<&'a mut fb::IndexEntry> {
        let buf = self.index_log.extend_fixed_mut::<IE>()?;
        Ok(buf.into())
    }
}

struct MmapLog {
    file: File,
    mmap: MmapMut,
    ptr: usize,
}

impl MmapLog {
    fn init(path: impl AsRef<Path>, create_new: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(create_new)
            .open(path)?;
        file.lock()?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { file, mmap, ptr: 0 })
    }

    fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    fn grow_and_remap(&mut self, required: usize) -> io::Result<()> {
        // Exponential growth: double until big enough
        let mut new_capacity = self.mmap.len().max(1 as usize);
        while new_capacity < required {
            new_capacity *= 2;
        }
        debug!(%required, %new_capacity, "Resizing mmap Started");
        self.file.set_len(new_capacity as u64)?;
        self.mmap = unsafe { MmapOptions::new().len(new_capacity).map_mut(&self.file)? };
        debug!(%required, %new_capacity, "Resizing mmap Complete");
        Ok(())
    }

    fn get<'a>(&'a self, offset: usize, size: usize) -> Option<&'a [u8]> {
        if offset + size > self.ptr {
            return None;
        }
        Some(&self.mmap[offset..offset + size])
    }

    fn get_fixed<'a, const N: usize>(&'a self, offset: usize) -> Option<&'a [u8; N]> {
        if offset + N > self.ptr {
            return None;
        }
        self.mmap[offset..offset + N].try_into().ok()
    }

    fn extend_from_slice(&mut self, data: &[u8]) -> io::Result<()> {
        if self.ptr + data.len() > self.mmap.len() {
            self.grow_and_remap(self.ptr + data.len())?;
        }

        self.mmap[self.ptr..self.ptr + data.len()].copy_from_slice(data);
        self.ptr += data.len();
        Ok(())
    }

    fn extend_fixed_mut<'a, const N: usize>(&'a mut self) -> io::Result<&'a mut [u8; N]> {
        if self.ptr + N > self.mmap.len() {
            self.grow_and_remap(self.ptr + N)?;
        }
        // SAFETY:
        // - offset is in bounds.
        // - &mut self guarantees unique access, so aliasing rules are upheld.
        let ptr = self.mmap.as_mut_ptr();
        let buf = unsafe { &mut *(ptr.add(self.ptr) as *mut [u8; N]) };
        self.ptr += N;
        Ok(buf)
    }

    fn set_ptr(&mut self, ptr: usize) {
        self.ptr = ptr;
    }

    // fn get_fixed_mut<'a, const N: usize>(&'a mut self, offset: usize) -> Option<&'a mut [u8; N]> {
    //     if offset + N > self.mmap.len() {
    //         return None;
    //     }
    //     // SAFETY:
    //     // - offset is in bounds.
    //     // - &mut self guarantees unique access, so aliasing rules are upheld.
    //     let ptr = self.mmap.as_mut_ptr();
    //     unsafe { Some(&mut *(ptr.add(offset) as *mut [u8; N])) }
    // }
}

pub struct MmapMetadata {
    mmap: MmapMut,
    metadata: fb::Metadata,
}

impl Deref for MmapMetadata {
    type Target = fb::Metadata;
    fn deref(&self) -> &Self::Target {
        &self.metadata
    }
}

impl DerefMut for MmapMetadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.metadata
    }
}

impl MmapMetadata {
    pub fn init(path: impl AsRef<Path>, create_new: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(create_new)
            .open(path)?;
        file.set_len(size_of::<fb::Metadata>() as u64)?;
        file.lock()?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let mmap_ptr = mmap.as_ptr() as *const fb::Metadata;
        let metadata = unsafe { *mmap_ptr };
        Ok(Self { mmap, metadata })
    }

    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

fn log_entry(entry: &fb::IndexEntry, data: &[u8], msg: &str) {
    let entry_str = format!(
        "{{ Index: {}, Term: {}, Sender: {} }}",
        entry.index(),
        entry.term(),
        entry.sender(),
    );
    let data_str = if entry.control() {
        let control = root::<ControlEntry>(data).unwrap();
        match control.control_type() {
            fb::ControlMessage::AddNode => format!("{:?}", control.control_as_add_node().unwrap()),
            fb::ControlMessage::PromoteNode => {
                format!("{:?}", control.control_as_promote_node().unwrap())
            }
            fb::ControlMessage::RemoveNode => {
                format!("{:?}", control.control_as_remove_node().unwrap())
            }
            _ => format!("Unkown Control Message"),
        }
    } else if let Ok(data) = str::from_utf8(data) {
        data.to_string()
    } else {
        "<BIN DATA>".to_string()
    };
    info!("{} {}: {}", entry_str, msg, data_str);
}

impl<'a> From<&'a mut [u8; IE]> for &'a mut fb::IndexEntry {
    fn from(buf: &'a mut [u8; IE]) -> Self {
        let buf_ptr = buf.as_mut_ptr() as *mut fb::IndexEntry;
        unsafe { &mut *buf_ptr }
    }
}

impl From<&[u8; IE]> for &fb::IndexEntry {
    fn from(buf: &[u8; IE]) -> Self {
        let buf_ptr = buf.as_ptr() as *const fb::IndexEntry;
        unsafe { &*buf_ptr }
    }
}
