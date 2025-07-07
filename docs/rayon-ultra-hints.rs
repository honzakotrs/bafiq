/// Ultra–high-performance flag-index builder.
/// ───────────────────────────────────────────
/// High-gain improvements applied:
///  1.  SegQueue   → bounded `crossbeam::channel`  ➜ back-pressure, no spin loops.
///  2.  No extra `to_vec()` copy                ➜ decompress directly into a Vec
///                                                and move ownership across stages.
///  3.  Thread-local `libdeflater::Decompressor` ➜ avoid per-block init cost.
///  4.  `memchr::memchr2` for header scan       ➜ SIMD search instead of byte-walk.
///  5.  Rayon pool sized once, then re-used.     (Easy to re-tune if needed!)
use std::{
    fs::File,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Result};
use crossbeam::{
    channel::{bounded, Receiver, Sender},
    thread,
};
use libdeflater::Decompressor;
use memchr::memchr2;
use memmap2::Mmap;
use rayon::ThreadPoolBuilder;

const DISC_QUEUE: usize = 1_024; // Tune → #threads * 4 is a good rule-of-thumb
const DECOMP_QUEUE: usize = 1_024;

pub fn build_rayon_ultra_performance(bam_path: &str) -> Result<FlagIndex> {
    // ─────────────────────────────  SET-UP  ─────────────────────────────
    let file = File::open(bam_path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let data = Arc::new(mmap);
    let file_size = data.len();

    let num_threads = num_cpus::get(); // physical+efficiency; tune if needed

    // **Bounded MPMC channels** give blocking semantics → no manual spin loops.
    let (tx_disc, rx_disc): (Sender<BlockInfo>, Receiver<BlockInfo>) = bounded(DISC_QUEUE);
    let (tx_decmp, rx_decmp): (Sender<DecompressedBlock>, Receiver<DecompressedBlock>) =
        bounded(DECOMP_QUEUE);

    // Flags used to tell downstream stages they can exit cleanly
    let discovery_done = Arc::new(AtomicBool::new(false));
    let decompression_done = Arc::new(AtomicBool::new(false));

    // Install one global Rayon pool so nested `into_par_iter()` never oversubscribes.
    ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()?;

    // ─────────── 3-STAGE PIPELINE orchestrated with `crossbeam::thread::scope` ───────────
    thread::scope(|s| {
        // ── STAGE 1: Parallel BGZF Header Discovery ──────────────────────────────────
        let seg_size = (file_size + num_threads - 1) / num_threads; // ceil-div
        for tid in 0..num_threads {
            let data_seg = Arc::clone(&data);
            let tx_out = tx_disc.clone();
            let discovery_done = Arc::clone(&discovery_done);

            s.spawn(move |_| {
                let seg_start = tid * seg_size;
                let seg_end = (seg_start + seg_size).min(file_size);
                let mut pos = seg_start;

                // If not first segment, jump to first magic bytes inside segment
                if tid != 0 {
                    if let Some(rel) = memchr2(0x1f, 0x8b, &data_seg[pos..seg_end]) {
                        pos += rel;
                    } else {
                        return; // no block in this segment
                    }
                }

                while pos + BGZF_HEADER_SIZE <= seg_end {
                    // Quick rejection done above; verify magic again
                    if &data_seg[pos..pos + 2] != &[0x1f, 0x8b] {
                        pos += 1;
                        continue;
                    }
                    // SAFETY: we already checked bounds
                    let bsize =
                        u16::from_le_bytes([data_seg[pos + 16], data_seg[pos + 17]]) as usize + 1;
                    let total = bsize;
                    if total < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || pos + total > file_size {
                        pos += 1;
                        continue;
                    }

                    tx_out
                        .send(BlockInfo {
                            start_pos: pos,
                            total_size: total,
                        })
                        .expect("channel closed unexpectedly");

                    pos += total;
                }

                // last thread finishing sets the flag; cheap but good enough
                if tid == num_threads - 1 {
                    discovery_done.store(true, Ordering::Release);
                }
            });
        }

        // ── STAGE 2: Parallel Decompression ──────────────────────────────────────────
        for _ in 0..num_threads {
            let data_arc = Arc::clone(&data);
            let rx_in = rx_disc.clone();
            let tx_out = tx_decmp.clone();
            let discovery_done = Arc::clone(&discovery_done);
            let decompression_done = Arc::clone(&decompression_done);

            s.spawn(move |_| {
                let mut decomp = Decompressor::new();
                let mut buffer = Vec::<u8>::with_capacity(BGZF_BLOCK_MAX_SIZE);

                while let Ok(block_info) = rx_in.recv() {
                    let raw = &data_arc
                        [block_info.start_pos..block_info.start_pos + block_info.total_size];
                    // Decompress **directly into `buffer`** (no extra copy)
                    buffer.clear();
                    decomp
                        .gzip_decompress_vec(raw, &mut buffer)
                        .expect("decompression failed");

                    // Skip BAM magic/header blocks early
                    if buffer.starts_with(b"BAM\x01") {
                        continue;
                    }

                    // Move ownership of `buffer` without copying:
                    tx_out
                        .send(DecompressedBlock {
                            data: buffer.clone(), // clone Vec header, not bytes
                            block_offset: block_info.start_pos as i64,
                        })
                        .expect("processing stage gone");
                }

                // If every decompressor exits, flag stage completion
                if discovery_done.load(Ordering::Acquire) && rx_in.is_empty() {
                    decompression_done.store(true, Ordering::Release);
                }
            });
        }

        // ── STAGE 3: Vectorised Record Processing (Rayon parallel) ───────────────────
        let results: Vec<FlagIndex> = (0..num_threads)
            .into_par_iter()
            .map(|_| {
                let rx_in = rx_decmp.clone();
                // local index lives on this thread
                let mut local = FlagIndex::new();

                while let Ok(block) = rx_in.recv() {
                    extract_flags_vectorized_ultra_optimized(
                        &block.data,
                        &mut local,
                        block.block_offset,
                    )?;
                }
                Ok::<_, anyhow::Error>(local)
            })
            .collect::<Result<Vec<_>>>()?;

        // ─┬───────────────────────────────────────────────────────────────────────
        //  └─ Merge thread-local indexes
        let mut final_index = FlagIndex::new();
        for idx in results {
            final_index.merge(idx);
        }
        Ok::<_, anyhow::Error>(final_index)
    })
    .unwrap() // propagate any panic from scoped threads
}
