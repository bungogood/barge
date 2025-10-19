use crate::{Proposal, fb, message_generated::barge::IndexEntry};

use flatbuffers::Vector;
use memmap2::{MmapMut, MmapOptions};
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
    pub metadata: fb::Metadata,
    metadata_mmap: MmapMut,
    index_log: MmapLog,
    data_log: MmapLog,
}

impl LogStore {
    pub fn new(dir: &PathBuf, id: u16) -> anyhow::Result<Self> {
        let mut store = Self::open_mmaps(dir, true)
            .map_err(|e| anyhow::anyhow!("failed to create log: {:?}", e))?;
        store.metadata.set_id(id);
        store.write_metadata();
        Ok(store)
    }

    pub fn load(dir: &PathBuf) -> anyhow::Result<Self> {
        Self::open_mmaps(dir, false).map_err(|e| anyhow::anyhow!("failed to create log: {:?}", e))
    }

    fn open_mmaps(dir: &PathBuf, create_new: bool) -> io::Result<Self> {
        fs::create_dir_all(dir)?;

        let (metadata_mmap, metadata_file) = Self::open_mmap_file(
            dir.join(METADATA_FILE),
            create_new,
            Some(size_of::<fb::Metadata>()),
        )?;

        let metadata = fb::Metadata(
            metadata_mmap[..]
                .try_into()
                .expect("Metadata File Corupted"),
        );

        let index_ptr = metadata.append_index() as usize * IE;

        let index_log = if create_new {
            MmapLog::new(dir.join(INDEX_FILE))?
        } else {
            MmapLog::open(dir.join(INDEX_FILE), index_ptr)?
        };

        let data_ptr = if metadata.append_index() == 0 {
            0
        } else {
            let index_last = (metadata.append_index() - 1) as usize * IE;
            let buf = index_log.get_fixed::<IE>(index_last).unwrap();
            let last_entry = fb::IndexEntry(*buf);
            last_entry.offset() + last_entry.size() as u64
        };

        let data_log = if create_new {
            MmapLog::new(dir.join(DATA_FILE))?
        } else {
            MmapLog::open(dir.join(DATA_FILE), data_ptr as usize)?
        };

        Ok(Self {
            metadata,
            metadata_mmap,
            index_log,
            data_log,
        })
    }

    fn open_mmap_file(
        path: impl AsRef<Path>,
        create_new: bool,
        len: Option<usize>,
    ) -> io::Result<(MmapMut, File)> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(create_new)
            .open(path)?;

        if let Some(len) = len {
            file.set_len(len as u64)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok((mmap, file))
    }

    fn write_metadata(&mut self) {
        self.metadata_mmap.copy_from_slice(&self.metadata.0);
        self.metadata_mmap.flush().unwrap(); // durable write
    }

    pub fn append_entries<'a>(
        &mut self,
        entires: Vector<'a, fb::IndexEntry>,
        data: &[u8],
    ) -> io::Result<()> {
        assert!(!entires.is_empty(), "Appending Empty Entries");
        self.index_log.extend_from_slice(entires.bytes())?;
        self.data_log.extend_from_slice(data)?;
        self.index_log.flush()?;
        self.data_log.flush()?;
        self.metadata
            .set_append_index(self.append_index() + entires.len() as u64);
        self.write_metadata();
        Ok(())
    }

    pub fn append_proposals(
        &mut self,
        proposals: &Vec<Proposal>,
    ) -> io::Result<(&[IndexEntry], &[u8])> {
        let offset = self.append_index();
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
        self.metadata
            .set_append_index(self.append_index() + proposals.len() as u64);
        self.write_metadata();

        Ok(self.get_entries(offset, proposals.len() as u64).unwrap())
    }

    pub fn commit_entries(&mut self, index: u64) {
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
        self.write_metadata();
    }

    pub fn start_election(&mut self) {
        self.metadata.set_term(self.term() + 1);
        self.metadata.set_voted_for(self.id());
        self.metadata.set_role(fb::Role::Candidate);
        self.write_metadata();
    }

    pub fn vote_for(&mut self, term: u64, candidate: u16) {
        if self.role() == fb::Role::Leader || self.role() == fb::Role::Candidate {
            self.metadata.set_role(fb::Role::Follower);
        }
        self.metadata.set_term(term);
        self.metadata.set_voted_for(candidate);
        self.write_metadata();
    }

    pub fn new_term(&mut self, term: u64) {
        if self.role() == fb::Role::Leader || self.role() == fb::Role::Candidate {
            self.metadata.set_role(fb::Role::Follower);
        }
        self.metadata.set_term(term);
        self.write_metadata();
    }

    pub fn become_learner(&mut self) {
        self.metadata.set_role(fb::Role::Learner);
        self.write_metadata();
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

    pub fn prev_index_term(&self) -> (u64, u64) {
        match self.get_index(self.append_index()) {
            Some(entry) => (entry.index(), entry.term()),
            None => (0, 0),
        }
    }

    fn extend_entry_mut<'a>(&'a mut self) -> io::Result<&'a mut fb::IndexEntry> {
        let buf = self.index_log.extend_fixed_mut::<IE>()?;
        Ok(unsafe { &mut *(buf.as_ptr() as *mut fb::IndexEntry) })
    }

    fn get_index<'a>(&'a self, idx: u64) -> Option<&'a fb::IndexEntry> {
        if idx == 0 {
            return None;
        }
        let offset = (idx - 1) as usize * IE;
        self.index_log
            .get_fixed::<IE>(offset)
            .map(|buf| unsafe { &*(buf.as_ptr() as *mut fb::IndexEntry) })
    }

    pub fn get_entries<'a>(
        &'a self,
        offset: u64,
        num: u64,
    ) -> Option<(&'a [IndexEntry], &'a [u8])> {
        if offset + num > self.append_index() {
            return None;
        }

        let (index_start, data_start) = if offset == 0 {
            (0, 0)
        } else {
            let first = self.get_index(offset).unwrap();
            ((offset) as usize * IE, first.offset() as usize)
        };

        let index_end = (offset + num) as usize * IE;
        let index_bytes = &self.index_log.mmap[index_start..index_end];
        assert!(index_bytes.len() % IE == 0);
        let index_ptr = index_bytes.as_ptr() as *const IndexEntry;
        let index_len = index_bytes.len() / IE;
        let entires = unsafe { slice::from_raw_parts(index_ptr, index_len) };

        let last = self.get_index(offset + num).unwrap();
        let data_end = last.offset() as usize + last.size() as usize;
        let data = &self.data_log.mmap[data_start..data_end];
        Some((entires, data))
    }
}

struct MmapLog {
    file: File,
    mmap: MmapMut,
    ptr: usize,
}

impl MmapLog {
    fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { file, mmap, ptr: 0 })
    }

    fn open(path: impl AsRef<Path>, ptr: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { file, mmap, ptr })
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
        if offset + size > self.mmap.len() {
            return None;
        }
        Some(&self.mmap[offset..offset + size])
    }

    fn get_fixed<'a, const N: usize>(&'a self, offset: usize) -> Option<&'a [u8; N]> {
        if offset + N > self.mmap.len() {
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
