//! Cold tier : fichier append + index mémoire des offsets (NVMe-friendly).

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
struct Loc {
    offset: u64,
    len: u32,
}

pub struct ColdStore {
    path: PathBuf,
    index: Mutex<HashMap<Vec<u8>, Loc>>,
    file: Mutex<Option<File>>,
}

impl ColdStore {
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("ferredis.cold");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            path,
            index: Mutex::new(HashMap::new()),
            file: Mutex::new(Some(file)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> usize {
        self.index.lock().len()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        let mut file_g = self.file.lock();
        let f = file_g.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "cold store closed")
        })?;
        let offset = f.seek(SeekFrom::End(0))?;
        let klen = key.len() as u32;
        let vlen = value.len() as u32;
        f.write_all(&klen.to_le_bytes())?;
        f.write_all(&vlen.to_le_bytes())?;
        f.write_all(key)?;
        f.write_all(value)?;
        f.flush()?;
        self.index.lock().insert(
            key.to_vec(),
            Loc {
                offset: offset + 8 + klen as u64,
                len: vlen,
            },
        );
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> std::io::Result<Option<Bytes>> {
        let loc = {
            let idx = self.index.lock();
            match idx.get(key) {
                Some(l) => l.clone(),
                None => return Ok(None),
            }
        };
        let mut file_g = self.file.lock();
        let f = file_g.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "cold store closed")
        })?;
        f.seek(SeekFrom::Start(loc.offset))?;
        let mut buf = vec![0u8; loc.len as usize];
        f.read_exact(&mut buf)?;
        Ok(Some(Bytes::from(buf)))
    }

    pub fn remove(&self, key: &[u8]) -> bool {
        self.index.lock().remove(key).is_some()
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.index.lock().contains_key(key)
    }
}
