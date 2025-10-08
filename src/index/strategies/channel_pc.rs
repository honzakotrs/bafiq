use anyhow::Result;
use crossbeam::channel::unbounded;
use libdeflater::Decompressor;
use memmap2::Mmap;

use std::fs::File;
use std::sync::Arc;

use super::IndexingStrategy;
use crate::FlagIndex;
use crate::bgzf::{BGZF_BLOCK_MAX_SIZE, BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE, is_bgzf_header};
use crate::index::discovery::{
    IncompleteHeader, extract_flags_from_block_pooled, process_incomplete_with_compressed_block,
};

pub struct ChannelProducerConsumerStrategy;

impl IndexingStrategy for ChannelProducerConsumerStrategy {
    fn build(&self, bam_path: &str) -> Result<FlagIndex> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);
        // Use unbounded channels to eliminate receiver mutex contention
        // Send: (block_index, start_pos, total_size, block_offset)
        let (sender, receiver) = unbounded::<(usize, usize, usize, i64)>();
        // Channel for incomplete headers from consumers
        let (incomplete_sender, incomplete_receiver) = unbounded::<(usize, IncompleteHeader)>();
        let num_threads = rayon::current_num_threads();

        crossbeam::thread::scope(|s| {
            // Producer thread: discovers complete BGZF blocks and streams them immediately
            let data_producer = Arc::clone(&data);
            s.spawn(move |_| {
                let mut pos = 0;
                let data_len = data_producer.len();
                let mut block_index = 0;

                while pos < data_len {
                    if pos + BGZF_HEADER_SIZE > data_len {
                        break;
                    }
                    let header = &data_producer[pos..pos + BGZF_HEADER_SIZE];
                    // Validate GZIP magic
                    if !is_bgzf_header(header) {
                        eprintln!("Invalid GZIP header at position {}", pos);
                        break;
                    }
                    // Extract block size
                    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                    let total_size = bsize + 1;
                    // Validate block size
                    if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE {
                        eprintln!("Invalid BGZF block size: {}", total_size);
                        break;
                    }
                    if pos + total_size > data_len {
                        break; // Incomplete block at end
                    }
                    // Send block immediately: (block_index, start_pos, total_size, block_offset)
                    if sender.send((block_index, pos, total_size, pos as i64)).is_err() {
                        break; // Receivers hung up
                    }
                    pos += total_size;
                    block_index += 1;
                }
                drop(sender);
            });
            // Consumer threads: process blocks as they arrive
            let results: Vec<_> = (0..num_threads)
                .map(|thread_id| {
                    let rx = receiver.clone();
                    let incomplete_tx = incomplete_sender.clone();
                    let data_worker = Arc::clone(&data);
                    s.spawn(move |_| -> FlagIndex {
                        let mut local_index = FlagIndex::new();
                        // Use thread-local buffers to avoid per-block allocations
                        thread_local! {
                            static BUFFER: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; BGZF_BLOCK_MAX_SIZE]);
                            static DECOMPRESSOR: std::cell::RefCell<Decompressor> = std::cell::RefCell::new(Decompressor::new());
                        }
                        while let Ok((block_idx, start_pos, total_size, block_offset)) = rx.recv() {
                            let block = &data_worker[start_pos..start_pos + total_size];
                            let result = BUFFER.with(|buf| {
                                DECOMPRESSOR.with(|decomp| {
                                    let mut buffer = buf.borrow_mut();
                                    let mut decompressor = decomp.borrow_mut();
                                    extract_flags_from_block_pooled(
                                        block,
                                        &mut local_index,
                                        block_offset,
                                        &mut buffer,
                                        &mut decompressor,
                                    )
                                })
                            });
                            match result {
                                Ok((_count, Some(incomplete))) => {
                                    // Send incomplete header for later processing
                                    let _ = incomplete_tx.send((block_idx, incomplete));
                                }
                                Err(e) => {
                                    eprintln!("Thread {}: Block processing error: {}", thread_id, e);
                                }
                                _ => {}
                            }
                        }
                        local_index
                    })
                })
                .collect();

            // Drop the sender so incomplete_receiver will finish
            drop(incomplete_sender);

            // Combine results using parallel merge tree
            let mut local_indexes: Vec<FlagIndex> = results.into_iter().map(|handle| handle.join().unwrap()).collect();

            // Collect all incomplete headers
            let incomplete_headers: Vec<(usize, IncompleteHeader)> = incomplete_receiver.iter().collect();

            // Process boundary records if any exist
            if !incomplete_headers.is_empty() {
                // First, rebuild block list to access adjacent blocks
                let mut blocks = Vec::new();
                let mut pos = 0;
                let data_len = data.len();

                while pos < data_len {
                    if pos + BGZF_HEADER_SIZE > data_len {
                        break;
                    }
                    let header = &data[pos..pos + BGZF_HEADER_SIZE];
                    if !is_bgzf_header(header) {
                        break;
                    }
                    let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                    let total_size = bsize + 1;
                    if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > BGZF_BLOCK_MAX_SIZE {
                        break;
                    }
                    if pos + total_size > data_len {
                        break;
                    }
                    blocks.push((pos, total_size));
                    pos += total_size;
                }

                // Process incomplete headers with next blocks
                let mut boundary_index = FlagIndex::new();

                for (block_idx, incomplete) in incomplete_headers {
                    if block_idx + 1 < blocks.len() {
                        let (next_start, next_size) = blocks[block_idx + 1];
                        let next_block_data = &data[next_start..next_start + next_size];
                        let (block_start, _) = blocks[block_idx];

                        // Use shared helper to decompress and process
                        match process_incomplete_with_compressed_block(&incomplete, next_block_data, block_start as i64) {
                            Ok(index) => boundary_index.merge(index),
                            Err(e) => eprintln!("Warning: Failed to process boundary record: {}", e),
                        }
                    }
                }

                local_indexes.push(boundary_index);
            }

            let final_index = FlagIndex::merge_parallel(local_indexes);

            Ok(final_index)
        })
        .unwrap()
    }
}
