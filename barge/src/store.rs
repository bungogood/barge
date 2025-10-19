use crate::{Proposal, fb};

use flatbuffers::Vector;
use memmap2::{MmapMut, MmapOptions};
use std::ops::{Deref, DerefMut};
use std::slice;
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    usize,
};
use tracing::{debug, info};

const METADATA_FILE: &str = "metadata.bin";
const INDEX_FILE: &str = "index.bin";
const DATA_FILE: &str = "data.bin";

const IE: usize = size_of::<fb::IndexEntry>();

pub struct LogStore {
    metadata: MmapMetadata,
    index_log: MmapLog,
    data_log: MmapLog,
}

impl LogStore {
    pub fn init(dir: &PathBuf, create_new: bool) -> io::Result<Self> {
        if create_new {
            fs::create_dir_all(dir)?;
        }

        let metadata = MmapMetadata::init(dir.join(METADATA_FILE), create_new)?;
        let index_log = MmapLog::init(dir.join(INDEX_FILE), create_new)?;
        let data_log = MmapLog::init(dir.join(DATA_FILE), create_new)?;

        let mut store = Self {
            metadata,
            index_log,
            data_log,
        };

        store.set_mmap_ptrs();
        Ok(store)
    }

    fn set_mmap_ptrs(&mut self) {
        let index_ptr = self.metadata.append_index() as usize * IE;
        self.index_log.set_ptr(index_ptr);
        if let Some(last) = self.get_index(self.metadata.append_index() - 1) {
            let data_ptr = last.offset() as usize + last.size() as usize;
            self.data_log.set_ptr(data_ptr);
        }
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
        let offset = self.append_index();
        let next_append_index = offset + proposals.len() as u64;
        let mut data_offset = self.data_log.ptr;
        info!(
            prev_index = offset,
            append_index = offset + proposals.len() as u64,
            "Appending Entries"
        );
        let mut entry_index = self.append_index() + 1;
        let entry_term = self.term();
        let sender = self.id();
        for proposal in proposals {
            self.data_log.extend_from_slice(&proposal.data)?;
            let entry = self.extend_entry_mut()?;
            entry.set_index(entry_index);
            entry.set_term(entry_term);
            entry.set_sender(sender);
            entry.set_control(proposal.control);
            entry.set_offset(data_offset as u64);
            entry.set_size(proposal.data.len() as u32);
            data_offset += proposal.data.len();
            entry_index += 1;

            let data = str::from_utf8(&proposal.data).unwrap();
            info!(?entry, %data, "Appending");
        }
        self.index_log.flush()?;
        self.data_log.flush()?;
        self.metadata.set_append_index(next_append_index);
        self.metadata.flush()?;

        Ok(self.get_entries(offset, proposals.len() as u64).unwrap())
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
            let data = str::from_utf8(buf).unwrap();
            info!(?entry, %data, "Commiting");
        }
        self.metadata.set_commit_index(index);
        self.metadata.flush()
    }

    pub fn start_election(&mut self) -> io::Result<()> {
        let next_term = self.term() + 1;
        let vote = self.id();
        self.metadata.set_term(next_term);
        self.metadata.set_voted_for(vote);
        self.metadata.set_role(fb::Role::Candidate);
        self.metadata.flush()
    }

    pub fn vote_for(&mut self, term: u64, candidate: u16) -> io::Result<()> {
        if self.role() == fb::Role::Leader || self.role() == fb::Role::Candidate {
            self.metadata.set_role(fb::Role::Follower);
        }
        self.metadata.set_term(term);
        self.metadata.set_voted_for(candidate);
        self.metadata.flush()
    }

    pub fn new_term(&mut self, term: u64) -> io::Result<()> {
        if self.role() == fb::Role::Leader || self.role() == fb::Role::Candidate {
            self.metadata.set_role(fb::Role::Follower);
        }
        self.metadata.set_term(term);
        self.metadata.flush()
    }

    pub fn become_learner(&mut self) -> io::Result<()> {
        self.metadata.set_role(fb::Role::Learner);
        self.metadata.flush()
    }

    pub fn new_node(&mut self, id: u16, role: fb::Role) -> io::Result<()> {
        self.metadata.set_id(id);
        self.metadata.set_role(role);
        self.metadata.flush()
    }

    pub fn id(&self) -> u16 {
        self.metadata.id()
    }

    pub fn role(&self) -> fb::Role {
        self.metadata.role()
    }

    pub fn voted_for(&self) -> u16 {
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

    pub fn metadata(&self) -> &fb::Metadata {
        &self.metadata
    }

    pub fn prev_index_term(&self) -> (u64, u64) {
        match self.get_index(self.append_index()) {
            Some(entry) => (entry.index(), entry.term()),
            None => (0, 0),
        }
    }

    fn get_index<'a>(&'a self, idx: u64) -> Option<&'a fb::IndexEntry> {
        if idx == 0 {
            return None;
        }
        let offset = (idx - 1) as usize * IE;
        self.index_log.get_fixed::<IE>(offset).map(|buf| buf.into())
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
            let first = self.get_index(offset).unwrap();
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

        let last = self.get_index(offset + num).expect("Getting Logged Index");
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
