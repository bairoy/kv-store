//! Full compaction of SSTables, with crash recovery via a marker file.

use super::*;

pub(super) const COMPACTION_MARKER: &str = "compaction.marker";

impl LsmStore {
    /// Merge all current SSTables into one new SSTable.
    ///
    /// This first implementation performs compaction explicitly.
    ///
    /// Newest SSTable wins.
    ///
    /// Tombstones are removed because we compact ALL SSTables,
    /// so there is no older SSTable left that could contain the
    /// value shadowed by the tombstone.
    pub fn compact(&mut self) -> io::Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }

        let old_tables = self.sstables.clone();

        let new_path = self.next_sstable_path();

        // -------------------------------------------------------
        // Phase 1:
        // Write a durable compaction marker BEFORE creating the
        // replacement SSTable.
        //
        // If we crash after this point, recovery knows whether
        // the new table was successfully installed.
        // -------------------------------------------------------

        let old_names: Vec<String> = old_tables
            .iter()
            .filter_map(|table| {
                table
                    .path()
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
            .collect();

        let new_name = new_path
            .file_name()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "new SSTable path has no filename",
                )
            })?
            .to_string_lossy()
            .to_string();

        self.write_compaction_marker(&new_name, &old_names)?;

        // -------------------------------------------------------
        // Phase 2:
        // Merge the SSTables.
        // -------------------------------------------------------

        let mut seen = HashSet::new();
        let mut merged = BTreeMap::new();

        // sstables = oldest → newest
        //
        // Therefore iterate backwards.
        for table in old_tables.iter().rev() {
            let entries = table.read_all()?;

            for (key, value) in entries {
                // If we've already encountered this key in a
                // newer SSTable, the newer version wins.
                if !seen.insert(key.clone()) {
                    continue;
                }

                match value {
                    Some(value) => {
                        merged.insert(key, Some(value));
                    }

                    None => {
                        // Tombstone:
                        //
                        // We intentionally do NOT write it into
                        // the compacted table because ALL older
                        // SSTables are part of this compaction.
                    }
                }
            }
        }

        // -------------------------------------------------------
        // Phase 3:
        // Create and fsync the new SSTable.
        // -------------------------------------------------------

        let new_table = Sstable::create_from_entries(&new_path, merged.iter())?;

        // Ensure the directory metadata is durable on Unix.
        sync_directory(&self.sstable_dir)?;

        // -------------------------------------------------------
        // Phase 4:
        // Delete old SSTables.
        //
        // This happens ONLY after the new SSTable is durable.
        // -------------------------------------------------------

        for table in &old_tables {
            fs::remove_file(table.path())?;
        }

        sync_directory(&self.sstable_dir)?;

        // -------------------------------------------------------
        // Phase 5:
        // Remove the marker.
        //
        // Compaction is now complete.
        // -------------------------------------------------------

        let marker_path = self.marker_path();

        if marker_path.exists() {
            fs::remove_file(&marker_path)?;
        }

        sync_directory(&self.sstable_dir)?;

        self.sstables = vec![new_table];

        self.next_sstable_id += 1;

        Ok(())
    }

    pub(super) fn marker_path(&self) -> PathBuf {
        self.sstable_dir.join(COMPACTION_MARKER)
    }

    pub(super) fn write_compaction_marker(
        &self,
        new_name: &str,
        old_names: &[String],
    ) -> io::Result<()> {
        let marker_path = self.marker_path();
        let temp_path = self.sstable_dir.join("compaction.marker.tmp");

        {
            let mut file = BufWriter::new(File::create(&temp_path)?);

            writeln!(file, "NEW\t{new_name}")?;

            for old_name in old_names {
                writeln!(file, "OLD\t{old_name}")?;
            }

            file.flush()?;
            file.get_ref().sync_all()?;
        }

        fs::rename(&temp_path, &marker_path)?;

        sync_directory(&self.sstable_dir)?;

        Ok(())
    }

    /// Recover a compaction interrupted by a crash.
    ///
    /// Cases:
    ///
    /// 1. Marker exists, new SSTable doesn't exist:
    ///    compaction never completed → keep old tables.
    ///
    /// 2. Marker exists, new SSTable exists:
    ///    new table is durable → finish deleting old tables.
    pub(super) fn recover_compaction(dir: &Path) -> io::Result<()> {
        let marker_path = dir.join(COMPACTION_MARKER);

        if !marker_path.exists() {
            return Ok(());
        }

        let file = File::open(&marker_path)?;
        let reader = BufReader::new(file);

        let mut new_name = None;
        let mut old_names = Vec::new();

        for line in reader.lines() {
            let line = line?;

            let parts: Vec<&str> = line.splitn(2, '\t').collect();

            match parts.as_slice() {
                ["NEW", name] => {
                    new_name = Some((*name).to_string());
                }

                ["OLD", name] => {
                    old_names.push((*name).to_string());
                }

                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid compaction marker",
                    ));
                }
            }
        }

        let new_name = new_name.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "compaction marker has no NEW entry",
            )
        })?;

        let new_path = dir.join(&new_name);

        if new_path.exists() {
            // New SSTable is durable.
            //
            // Finish deleting old SSTables.
            for old_name in old_names {
                let old_path = dir.join(old_name);

                if old_path.exists() {
                    fs::remove_file(old_path)?;
                }
            }
        }

        // If new table doesn't exist, the compaction never
        // reached the durable-output stage. Leave old tables.
        //
        // In either case the marker is no longer needed.
        fs::remove_file(marker_path)?;

        sync_directory(dir)?;

        Ok(())
    }
}
