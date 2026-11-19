//! Implementações de `EntrySource`/`EntrySink` para RocksDB.
//!
//! As chamadas de leitura são as mesmas do `exporter.rs` (que já compila no projeto). As de escrita
//! (`WriteBatch`, `write_opt`, `flush`, `compact_range`) são da API padrão do crate `rocksdb`
//! com tuning compatível com Libplanet C# (idêntico ao pruner.rs).

use anyhow::{Context, Result};
use rocksdb::{BlockBasedOptions, Options, ReadOptions, WriteBatch, WriteOptions, DB};
use std::path::Path;

use crate::gc_scan::{EntrySink, EntrySource, KvBatch};

/// Lê um `states/` (normalmente o do checkpoint) em modo somente leitura.
pub struct RocksSource {
    db: DB,
}

impl RocksSource {
    pub fn open(states_path: &Path) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(false);
        opts.set_advise_random_on_open(false);
        opts.set_max_open_files(500);

        // format_version 5: compatibilidade com o Libplanet (igual ao exporter.rs)
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_format_version(5);
        opts.set_block_based_table_factory(&block_opts);

        let db = DB::open_for_read_only(&opts, states_path, false)
            .with_context(|| format!("Failed to open states/ at: {}", states_path.display()))?;
        Ok(Self { db })
    }
}

impl EntrySource for RocksSource {
    fn scan(&self, f: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
        let mut read_opts = ReadOptions::default();
        read_opts.set_readahead_size(32 * 1024 * 1024);
        read_opts.set_verify_checksums(false);
        read_opts.fill_cache(false); // varredura de uma passada: não polui o block cache

        let mut iter = self.db.raw_iterator_opt(read_opts);
        iter.seek_to_first();
        while iter.valid() {
            let key = iter.key().expect("valid() true implies key exists");
            let value = iter.value().expect("valid() true implies value exists");
            f(key, value)?;
            iter.next();
        }
        iter.status().map_err(anyhow::Error::msg).context("Iterator failed mid-scan")?;
        Ok(())
    }
}

/// Grava os nós vivos num `states_gc/` novo. Sem WAL (o `finish` faz flush e compactação).
pub struct RocksSink {
    db: DB,
    wopts: WriteOptions,
}

impl RocksSink {
    pub fn create(dest: &Path) -> Result<Self> {
        if dest.exists() {
            std::fs::remove_dir_all(dest)
                .with_context(|| format!("Falha ao limpar states_gc/: {}", dest.display()))?;
        }
        std::fs::create_dir_all(dest)
            .with_context(|| format!("Falha ao criar states_gc/: {}", dest.display()))?;

        let mut opts = Options::default();
        opts.create_if_missing(true);

        // ── Compatibilidade com Libplanet C# (idêntico ao pruner.rs) ──
        opts.set_soft_pending_compaction_bytes_limit(1_000_000_000_000);
        opts.set_hard_pending_compaction_bytes_limit(1_038_176_821_042);

        // ── FORMAT_VERSION 5 (CRÍTICO!) ──────────────────────────────
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_format_version(5);
        opts.set_block_based_table_factory(&block_opts);

        // Bulk write tuning
        opts.set_write_buffer_size(256 * 1024 * 1024);
        opts.set_max_write_buffer_number(6);
        opts.set_min_write_buffer_number_to_merge(2);
        opts.set_level_zero_file_num_compaction_trigger(8);
        opts.set_level_zero_slowdown_writes_trigger(20);
        opts.set_level_zero_stop_writes_trigger(36);
        opts.set_target_file_size_base(256 * 1024 * 1024);
        opts.set_max_open_files(-1);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_max_background_jobs(8);

        // ── BULK LOAD MODE: manual (compatível com Libplanet) ───────
        opts.set_disable_auto_compactions(true);

        let db = DB::open(&opts, dest)
            .with_context(|| format!("Failed to create states_gc at: {}", dest.display()))?;
        let mut wopts = WriteOptions::default();
        wopts.disable_wal(true);
        Ok(Self { db, wopts })
    }
}

impl EntrySink for RocksSink {
    fn write(&mut self, batch: &KvBatch) -> Result<()> {
        let mut wb = WriteBatch::default();
        for (k, v) in batch.iter() {
            wb.put(k, v);
        }
        self.db.write_opt(wb, &self.wopts).context("Falha ao gravar lote no states_gc")?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.db.flush().context("Falha no flush do states_gc")?;
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }
}
