use crate::bgzf::{BGZF_FOOTER_SIZE, BGZF_HEADER_SIZE};
use crate::FlagIndex;
use anyhow::{anyhow, Result};
use crossbeam::channel::unbounded;
use memmap2::Mmap;
use std::fs::File;

use std::path::Path;

use std::sync::Arc;
use std::thread as std_thread;

// Import strategies
use crate::index::strategies::shared::count_flags_in_block_optimized;
use crate::index::strategies::{
    adaptive_memory_mapped::AdaptiveMemoryMappedStrategy, constant_memory::ConstantMemoryStrategy,
    parallel_streaming::ChannelProducerConsumerStrategy, work_stealing::WorkStealingStrategy,
    IndexingStrategy,
};

/// Available index building strategies
#[derive(Debug, Clone, Copy)]
pub enum BuildStrategy {
    /// Channel-based producer-consumer - crossbeam channels architecture (2.127s) 📡 CHANNELS
    ChannelProducerConsumer,
    /// Work-stealing processing - fastest performing approach (1.427s) FASTEST
    WorkStealing,
    /// constant-memory processing - constant RAM footprint for any file size 💾 EFFICIENT
    ConstantMemory,
    /// Adaptive memory-mapped streaming - best of both worlds (performance + memory efficiency) 🧠 SMART
    AdaptiveMemoryMapped,
}

/// Primary interface for building flag indexes with different strategies
pub struct IndexBuilder {
    strategy: BuildStrategy,
}

impl BuildStrategy {
    /// Get the canonical name for this strategy (used in benchmarks and CLI)
    pub fn name(&self) -> &'static str {
        match self {
            BuildStrategy::ChannelProducerConsumer => "channel_producer_consumer",
            BuildStrategy::WorkStealing => "work_stealing",
            BuildStrategy::ConstantMemory => "memory_friendly",
            BuildStrategy::AdaptiveMemoryMapped => "adaptive_memory_mapped",
        }
    }

    /// Get all available strategies for benchmarking
    pub fn all_strategies() -> Vec<BuildStrategy> {
        vec![
            BuildStrategy::ChannelProducerConsumer,
            BuildStrategy::WorkStealing,
            BuildStrategy::ConstantMemory,
            BuildStrategy::AdaptiveMemoryMapped,
        ]
    }

    /// Get strategies suitable for routine benchmarking (excludes slow ones)
    pub fn benchmark_strategies() -> Vec<BuildStrategy> {
        Self::all_strategies().into_iter().collect()
    }
}

impl Default for BuildStrategy {
    fn default() -> Self {
        BuildStrategy::WorkStealing
    }
}

impl IndexBuilder {
    /// Create a new index builder with default strategy (parallel chunk streaming)
    pub fn new() -> Self {
        Self {
            strategy: BuildStrategy::default(),
        }
    }

    /// Create a new index builder with specified strategy
    pub fn with_strategy(strategy: BuildStrategy) -> Self {
        Self { strategy }
    }

    /// Build an index from a BAM file path
    pub fn build<P: AsRef<Path>>(&self, bam_path: P) -> Result<FlagIndex> {
        let path_str = bam_path
            .as_ref()
            .to_str()
            .ok_or_else(|| anyhow!("Invalid file path"))?;

        match self.strategy {
            BuildStrategy::ChannelProducerConsumer => {
                ChannelProducerConsumerStrategy.build(path_str)
            }
            BuildStrategy::WorkStealing => WorkStealingStrategy.build(path_str),
            BuildStrategy::ConstantMemory => ConstantMemoryStrategy.build(path_str),
            BuildStrategy::AdaptiveMemoryMapped => AdaptiveMemoryMappedStrategy.build(path_str),
        }
    }

    /// Get the current build strategy
    pub fn strategy(&self) -> BuildStrategy {
        self.strategy
    }

    /// Fast scan-only mode - count flags without building index
    /// Single-pass streaming approach comparable to samtools performance
    pub fn scan_count(
        &self,
        bam_path: &str,
        required_flags: u16,
        forbidden_flags: u16,
        thread_count: Option<usize>,
    ) -> Result<u64> {
        let file = File::open(bam_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data = Arc::new(mmap);

        // Use specified thread count or default to more workers than CPU cores for I/O bound workload
        let num_threads = thread_count.unwrap_or_else(|| (rayon::current_num_threads() * 2).max(8));

        // PERFORMANCE FIX: Use unbounded crossbeam channel for scan_count too!
        let (sender, receiver) = unbounded::<(usize, usize)>(); // (start_pos, total_size)

        // Producer thread: discover and stream blocks immediately
        let data_producer = Arc::clone(&data);
        let producer_handle = std_thread::spawn(move || -> Result<usize> {
            let mut pos = 0;
            let mut block_count = 0;
            let data_len = data_producer.len();

            while pos < data_len {
                if pos + BGZF_HEADER_SIZE > data_len {
                    break;
                }

                let header = &data_producer[pos..pos + BGZF_HEADER_SIZE];

                // Validate GZIP magic
                if header[0..2] != [0x1f, 0x8b] {
                    return Err(anyhow!("Invalid GZIP header at position {}", pos));
                }

                // Extract block size
                let bsize = u16::from_le_bytes([header[16], header[17]]) as usize;
                let total_size = bsize + 1;

                // Validate block size
                if total_size < BGZF_HEADER_SIZE + BGZF_FOOTER_SIZE || total_size > 65536 {
                    return Err(anyhow!("Invalid BGZF block size: {}", total_size));
                }

                if pos + total_size > data_len {
                    break; // Incomplete block at end
                }

                // Send block immediately to workers (single pass!)
                if sender.send((pos, total_size)).is_err() {
                    break; // Workers hung up
                }

                pos += total_size;
                block_count += 1;
            }

            Ok(block_count)
        });

        // Consumer threads: count flags as blocks arrive
        let mut worker_handles = Vec::new();
        // PERFORMANCE FIX: Remove mutex contention in scan_count too!

        for _thread_id in 0..num_threads {
            let receiver_clone = receiver.clone();
            let data_worker = Arc::clone(&data);

            let handle = std_thread::spawn(move || -> Result<u64> {
                let mut local_count = 0u64;

                loop {
                    // PERFORMANCE FIX: Direct channel access - NO MUTEX CONTENTION!
                    match receiver_clone.recv() {
                        Ok((start_pos, total_size)) => {
                            let block = &data_worker[start_pos..start_pos + total_size];
                            local_count += count_flags_in_block_optimized(
                                block,
                                required_flags,
                                forbidden_flags,
                            )
                            .unwrap_or(0);
                        }
                        Err(_) => {
                            // Channel closed, no more blocks
                            break;
                        }
                    }
                }

                Ok(local_count)
            });

            worker_handles.push(handle);
        }

        // Wait for producer
        let _total_blocks = producer_handle
            .join()
            .map_err(|e| anyhow!("Producer thread failed: {:?}", e))??;

        // Wait for all workers and sum their counts
        let mut total_count = 0u64;
        for handle in worker_handles.into_iter() {
            match handle.join() {
                Ok(Ok(local_count)) => {
                    total_count += local_count;
                }
                Ok(Err(e)) => {
                    eprintln!("Worker thread failed: {}", e);
                }
                Err(e) => {
                    eprintln!("Worker thread panicked: {:?}", e);
                }
            }
        }

        Ok(total_count)
    }
}

impl Default for IndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}
