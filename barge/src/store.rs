use crate::fb;

use flatbuffers::{Push, root};
use memmap2::MmapMut;
use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
};
use tracing::info;

pub struct LogStore {
    metadata: fb::Metadata,
    metadata_mmap: MmapMut,
    index_mmap: MmapMut,
    log_mmap: MmapMut,
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

        let metadata_mmap =
            Self::open_mmap_file(dir.join("meta.bin"), create_new, Some(fb::Metadata::size()))?;
        let index_mmap = Self::open_mmap_file(
            dir.join("index.bin"),
            create_new,
            Some(fb::Metadata::size()),
        )?;
        let log_mmap =
            Self::open_mmap_file(dir.join("log.bin"), create_new, Some(fb::Metadata::size()))?;

        let metadata = if create_new {
            fb::Metadata::new(0, 0, 0, 0, 0)
        } else {
            fb::Metadata(
                metadata_mmap[..]
                    .try_into()
                    .expect("Metadata File Corupted"),
            )
        };

        Ok(Self {
            metadata,
            metadata_mmap,
            index_mmap,
            log_mmap,
        })
    }

    fn open_mmap_file(
        path: impl AsRef<Path>,
        create_new: bool,
        len: Option<usize>,
    ) -> io::Result<MmapMut> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(create_new)
            .open(path)?;

        if let Some(len) = len {
            file.set_len(len as u64)?;
        }

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(mmap)
    }

    fn write_metadata(&mut self) {
        self.metadata_mmap.copy_from_slice(&self.metadata.0);
        self.metadata_mmap.flush().unwrap(); // durable write
    }

    pub fn id(&self) -> u16 {
        self.metadata.id()
    }
}
