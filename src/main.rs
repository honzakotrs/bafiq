use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand, ValueEnum};
use rayon::ThreadPoolBuilder;
use rust_htslib::bam::{Format, Read as BamRead, Writer};
use std::path::{Path, PathBuf};

use bafiq::{
    view::fast_count::scan_count, BuildStrategy, IndexBuilder, IndexManager, SerializableIndex,
};

/// CLI-friendly strategy names that map to BuildStrategy
#[derive(Debug, Clone, ValueEnum)]
pub enum CliStrategy {
    /// Channel-based producer-consumer - crossbeam channels architecture (2.127s)
    #[value(name = "channel-producer-consumer")]
    ChannelProducerConsumer,
    /// Work-stealing processing - fastest performing approach (1.427s)
    #[value(name = "work-stealing")]
    WorkStealing,
    /// constant-memory processing - constant RAM footprint for any file size
    #[value(name = "constant-memory")]
    ConstantMemory,
}

impl From<CliStrategy> for BuildStrategy {
    fn from(cli_strategy: CliStrategy) -> Self {
        match cli_strategy {
            CliStrategy::ChannelProducerConsumer => BuildStrategy::ChannelProducerConsumer,
            CliStrategy::WorkStealing => BuildStrategy::WorkStealing,
            CliStrategy::ConstantMemory => BuildStrategy::ConstantMemory,
        }
    }
}

/// Parse flag values supporting hex (0x4), decimal (4), and binary (0b100) formats like samtools
fn parse_flag_value(flag_str: &str) -> Result<u16> {
    let flag_str = flag_str.trim();

    if flag_str.starts_with("0x") || flag_str.starts_with("0X") {
        // Hexadecimal format
        let hex_part = &flag_str[2..];
        u16::from_str_radix(hex_part, 16).map_err(|_| anyhow!("Invalid hex format: {}", flag_str))
    } else if flag_str.starts_with("0b") || flag_str.starts_with("0B") {
        // Binary format
        let bin_part = &flag_str[2..];
        u16::from_str_radix(bin_part, 2).map_err(|_| anyhow!("Invalid binary format: {}", flag_str))
    } else {
        // Decimal format
        flag_str
            .parse::<u16>()
            .map_err(|_| anyhow!("Invalid decimal format: {}", flag_str))
    }
}

/// Shared flag filtering options used across multiple commands
/// Supports samtools-style -f/-F flags plus all 12 BAM flags as named options
#[derive(Debug, Parser)]
pub struct FlagFilter {
    /// Include only reads with *all* bits in this INT set (like samtools -f)
    /// Supports hex (0x4), decimal (4), and binary (0b100) formats
    #[arg(short = 'f', long = "include-flags", value_name = "INT")]
    pub include_flags: Option<String>,

    /// Exclude reads with *any* bits in this INT (like samtools -F)
    /// Supports hex (0x4), decimal (4), and binary (0b100) formats
    #[arg(short = 'F', long = "exclude-flags", value_name = "INT")]
    pub exclude_flags: Option<String>,

    // Individual flag options (combine with numeric flags)
    /// Include only paired reads (0x1)
    #[arg(long = "paired")]
    pub paired: bool,

    /// Include only properly paired reads (0x2)
    #[arg(long = "proper-pair")]
    pub proper_pair: bool,

    /// Include only unmapped reads (0x4)
    #[arg(long = "unmapped")]
    pub unmapped: bool,

    /// Include only reads with unmapped mate (0x8)
    #[arg(long = "mate-unmapped")]
    pub mate_unmapped: bool,

    /// Include only reads on reverse strand (0x10)
    #[arg(long = "reverse")]
    pub reverse: bool,

    /// Include only reads with mate on reverse strand (0x20)
    #[arg(long = "mate-reverse")]
    pub mate_reverse: bool,

    /// Include only first in pair reads (0x40)
    #[arg(long = "first-in-pair")]
    pub first_in_pair: bool,

    /// Include only second in pair reads (0x80)
    #[arg(long = "second-in-pair")]
    pub second_in_pair: bool,

    /// Include only secondary alignments (0x100)
    #[arg(long = "secondary")]
    pub secondary: bool,

    /// Include only QC failed reads (0x200)
    #[arg(long = "qc-fail")]
    pub qc_fail: bool,

    /// Include only duplicate reads (0x400)
    #[arg(long = "duplicate")]
    pub duplicate: bool,

    /// Include only supplementary alignments (0x800)
    #[arg(long = "supplementary")]
    pub supplementary: bool,

    // Negation flags (exclude specific types)
    /// Exclude paired reads (equivalent to -F 0x1)
    #[arg(long = "not-paired")]
    pub not_paired: bool,

    /// Exclude properly paired reads (equivalent to -F 0x2)
    #[arg(long = "not-proper-pair")]
    pub not_proper_pair: bool,

    /// Exclude unmapped reads, i.e., only mapped reads (equivalent to -F 0x4)
    #[arg(long = "mapped")]
    pub mapped: bool,

    /// Exclude reads with unmapped mate (equivalent to -F 0x8)
    #[arg(long = "mate-mapped")]
    pub mate_mapped: bool,

    /// Exclude reads on reverse strand (equivalent to -F 0x10)
    #[arg(long = "forward")]
    pub forward: bool,

    /// Exclude reads with mate on reverse strand (equivalent to -F 0x20)
    #[arg(long = "mate-forward")]
    pub mate_forward: bool,

    /// Exclude duplicates (equivalent to -F 0x400)
    #[arg(long = "non-duplicate")]
    pub non_duplicate: bool,
}

impl FlagFilter {
    /// Combine the integer flags and named flags into `(required_bits, forbidden_bits)`.
    /// Samtools logic:
    ///   - "-f X" means all bits in X must be set.
    ///   - "-F X" means none of the bits in X may be set.
    pub fn gather_bits(&self) -> Result<(u16, u16)> {
        let mut required_bits = 0u16;
        let mut forbidden_bits = 0u16;

        // Handle numeric flags
        if let Some(include_str) = &self.include_flags {
            required_bits |= parse_flag_value(include_str)?;
        }
        if let Some(exclude_str) = &self.exclude_flags {
            forbidden_bits |= parse_flag_value(exclude_str)?;
        }

        // Handle named flags
        if self.paired {
            required_bits |= 0x1;
        }
        if self.proper_pair {
            required_bits |= 0x2;
        }
        if self.unmapped {
            required_bits |= 0x4;
        }
        if self.mate_unmapped {
            required_bits |= 0x8;
        }
        if self.reverse {
            required_bits |= 0x10;
        }
        if self.mate_reverse {
            required_bits |= 0x20;
        }
        if self.first_in_pair {
            required_bits |= 0x40;
        }
        if self.second_in_pair {
            required_bits |= 0x80;
        }
        if self.secondary {
            required_bits |= 0x100;
        }
        if self.qc_fail {
            required_bits |= 0x200;
        }
        if self.duplicate {
            required_bits |= 0x400;
        }
        if self.supplementary {
            required_bits |= 0x800;
        }

        // Handle negation flags (exclude specific types)
        if self.not_paired {
            forbidden_bits |= 0x1;
        }
        if self.not_proper_pair {
            forbidden_bits |= 0x2;
        }
        if self.mapped {
            forbidden_bits |= 0x4;
        }
        if self.mate_mapped {
            forbidden_bits |= 0x8;
        }
        if self.forward {
            forbidden_bits |= 0x10;
        }
        if self.mate_forward {
            forbidden_bits |= 0x20;
        }
        if self.non_duplicate {
            forbidden_bits |= 0x400;
        }

        Ok((required_bits, forbidden_bits))
    }
}

/// Each subcommand (view/query) can specify:
/// - A single BAM/CRAM input
/// - Optional `-f INT` and `-F INT` for required/forbidden bits (samtools-style)
/// - Individual named flags for all 12 BAM flags
/// - Named flags combine with numeric flags for maximum flexibility
#[derive(Debug, Parser)]
pub struct SharedArgs {
    /// Flag filtering options
    #[command(flatten)]
    pub flags: FlagFilter,

    /// Force rebuild index (ignore cache) - for query command
    #[arg(long = "force-rebuild")]
    pub force_rebuild: bool,

    /// The input BAM/CRAM file
    pub input: PathBuf,
}

#[derive(Debug, Parser)]
pub struct IndexArgs {
    /// The input BAM/CRAM file
    pub input: PathBuf,

    /// Index building strategy
    #[arg(
        long = "strategy",
        value_enum,
        default_value = "work-stealing",
        help = "Index building strategy to use (default: work-stealing for maximum performance)"
    )]
    pub strategy: CliStrategy,

    /// Enable index compression (slower build, smaller files)
    #[arg(
        short = 'c',
        long = "compress-index",
        help = "Apply compression to reduce index file size"
    )]
    pub compress_index: bool,
}

impl SharedArgs {
    /// Combine the integer flags and named flags into `(required_bits, forbidden_bits)`.
    /// Delegates to the FlagFilter's gather_bits method.
    pub fn gather_bits(&self) -> Result<(u16, u16)> {
        self.flags.gather_bits()
    }
}

/// The top-level CLI definition with subcommands.
#[derive(Debug, Subcommand)]
enum Commands {
    /// View (i.e., retrieve/print) reads that match the given flag criteria (SAM output to stdout)
    View(SharedArgs),

    /// Build the index for the given BAM/CRAM file
    Index(IndexArgs),

    /// Query BAM file with automatic caching
    Query(SharedArgs),

    /// Fast count (like samtools view -c -f 0x4) - no index building
    FastCount {
        /// BAM file to scan
        input: PathBuf,

        /// Flag filtering options
        #[command(flatten)]
        flags: FlagFilter,
    },

    /// Show index information and status
    IndexInfo(IndexArgs),
}

#[derive(Debug, Parser)]
#[command(name = "bafiq", author, version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,

    /// Maximum number of threads to use for parallel processing
    /// (default: use all available cores)
    #[arg(long = "threads", short = 't', global = true)]
    threads: Option<usize>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Set global thread limit if specified
    if let Some(threads) = cli.threads {
        ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()
            .map_err(|e| anyhow!("Failed to set thread limit: {}", e))?;
        eprintln!("Thread limit set to: {}", threads);
    }

    match cli.cmd {
        Commands::View(args) => cmd_view(args, cli.threads),
        Commands::Index(args) => cmd_index(args),
        Commands::Query(args) => cmd_query(args),
        Commands::FastCount { input, flags } => cmd_fast_count(input, flags, cli.threads),
        Commands::IndexInfo(args) => cmd_index_info(args),
    }
}

/// Get the index path by appending ".bfi" to the original path
fn get_index_path(input: &Path) -> PathBuf {
    let mut index_path = input.to_path_buf();
    let extension = index_path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("");

    // Append .bfi to the original extension (.bam.bfi or .cram.bfi)
    index_path.set_extension(format!("{}.bfi", extension));
    index_path
}

/// `bafiq index <input.bam>`
/// Now uses streaming parallel processing by default for optimal performance
fn cmd_index(args: IndexArgs) -> Result<()> {
    let index_path = get_index_path(&args.input);
    let strategy: BuildStrategy = args.strategy.into();

    eprintln!("Building index using strategy: {:?}...", strategy);
    eprintln!("   Input: {:?}", args.input);
    eprintln!("   Output: {:?}", index_path);

    if args.compress_index {
        eprintln!("   Compression: ENABLED (slower build, smaller files)");

        // Use IndexManager with compression enabled
        let index_manager = IndexManager::new();
        let input_str = args
            .input
            .to_str()
            .ok_or_else(|| anyhow!("Invalid file path"))?;
        let serializable_index =
            index_manager.load_or_build_with_compression(input_str, true, true)?; // force rebuild with compression

        // Index is already saved by IndexManager
        let format_info = serializable_index.get_format_info();
        eprintln!("Index built and saved: {:?}", index_path);
        eprintln!(
            "   Compression ratio: {:.2}x",
            format_info.compression_ratio
        );
    } else {
        eprintln!("   Compression: DISABLED (faster build, larger files)");

        // Use the correct strategy from CLI argument
        let builder = IndexBuilder::with_strategy(strategy);
        let input_str = args
            .input
            .to_str()
            .ok_or_else(|| anyhow!("Invalid file path"))?;

        eprintln!("Force rebuilding index...");
        let uncompressed_index = builder.build(input_str)?;

        // Create and save serializable index
        let serializable_index =
            SerializableIndex::from_uncompressed(uncompressed_index, input_str)?;
        serializable_index.save_to_file(&index_path)?;

        eprintln!("Index built and saved: {:?}", index_path);
    }

    Ok(())
}

/// `bafiq view [options] <input.bam>`
/// Outputs matching reads in SAM format on stdout.
fn cmd_view(args: SharedArgs, thread_count: Option<usize>) -> Result<()> {
    let (required_bits, forbidden_bits) = args.gather_bits()?;
    let input_str = args
        .input
        .to_str()
        .ok_or_else(|| anyhow!("Invalid file path"))?;

    // Use the same unified index loading as query command (defaults to uncompressed)
    let index_manager = IndexManager::new();
    let serializable_index = index_manager.load_or_build(input_str, args.force_rebuild)?;

    // Open the file again just to extract the header
    let tmp_reader = rust_htslib::bam::Reader::from_path(&args.input)?;
    let header = rust_htslib::bam::Header::from_template(tmp_reader.header());

    // Create a SAM writer to stdout
    let mut writer = Writer::from_stdout(&header, Format::Sam)?;

    // Get the index accessor - works with both compressed and uncompressed indexes
    let index_accessor = serializable_index.get_index();

    // Retrieve matching reads using parallel processing (always enabled for best performance)
    // This works seamlessly with both compressed and uncompressed indexes!
    index_accessor.retrieve_reads_parallel(
        &args.input,
        required_bits,
        forbidden_bits,
        &mut writer,
        thread_count,
    )?;

    Ok(())
}

/// `bafiq query [options] <input.bam>`
/// Query BAM file with automatic caching and diagnostic output
fn cmd_query(args: SharedArgs) -> Result<()> {
    use std::time::Instant;

    eprintln!("Querying BAM file with automatic caching...");
    eprintln!("   Input: {:?}", args.input);

    let (required_bits, forbidden_bits) = args.gather_bits()?;

    eprintln!(
        "   Query: required=0x{:x}, forbidden=0x{:x}",
        required_bits, forbidden_bits
    );

    // Load or build index with automatic saving
    let index_manager = IndexManager::new();
    let input_str = args
        .input
        .to_str()
        .ok_or_else(|| anyhow!("Invalid file path"))?;

    let start = Instant::now();
    // For query command, we allow force rebuild through a future flag if needed
    let serializable_index = index_manager.load_or_build(input_str, args.force_rebuild)?;
    let load_time = start.elapsed();

    // Get format info
    let format_info = serializable_index.get_format_info();
    eprintln!("   Index format: {}", format_info.format_type);
    eprintln!("   Load time: {:.3}ms", load_time.as_secs_f64() * 1000.0);

    // Query the index
    let start = Instant::now();
    let index = serializable_index.get_index();
    let result = index.count(required_bits, forbidden_bits);
    let query_time = start.elapsed();

    eprintln!("Query Results:");
    eprintln!("   Matching reads: {}", result);
    eprintln!("   Query time: {:.3}ms", query_time.as_secs_f64() * 1000.0);
    eprintln!(
        "   Total time: {:.3}ms",
        (load_time + query_time).as_secs_f64() * 1000.0
    );

    Ok(())
}

fn cmd_index_info(args: IndexArgs) -> Result<()> {
    eprintln!("Index Information");
    eprintln!("{}", "=".repeat(50));

    let input_str = args
        .input
        .to_str()
        .ok_or_else(|| anyhow!("Invalid file path"))?;
    let index_manager = IndexManager::new();
    let index_path = index_manager.get_index_path(input_str);

    eprintln!("   BAM file: {}", input_str);
    eprintln!("   Index path: {}", index_path);

    // Check if BAM file exists
    if !Path::new(input_str).exists() {
        eprintln!("   BAM file does not exist");
        return Ok(());
    }

    let bam_metadata = std::fs::metadata(input_str)?;
    eprintln!(
        "   BAM size: {:.1} MB",
        bam_metadata.len() as f64 / 1_048_576.0
    );

    // Check index status
    if Path::new(&index_path).exists() {
        match SerializableIndex::load_from_file(&index_path) {
            Ok(saved_index) => {
                let format_info = saved_index.get_format_info();
                let cache_info = saved_index.get_cache_info();

                eprintln!("   Index exists");
                eprintln!("   Index format: {}", format_info.format_type);
                eprintln!(
                    "   Index compression: {:.2}x",
                    format_info.compression_ratio
                );

                match saved_index.is_stale() {
                    Ok(false) => eprintln!("   Index is fresh"),
                    Ok(true) => eprintln!("   Index is stale (BAM file was modified)"),
                    Err(e) => eprintln!("   Index validation failed: {}", e),
                }

                eprintln!(
                    "   Created: {}",
                    format_timestamp(cache_info.created_timestamp)
                );
            }
            Err(e) => {
                eprintln!("   Index file exists but failed to load: {}", e);
            }
        }
    } else {
        eprintln!("   No index found");
    }

    Ok(())
}

fn format_timestamp(timestamp: u64) -> String {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let system_time = UNIX_EPOCH + Duration::from_secs(timestamp);
    match system_time.duration_since(SystemTime::now()) {
        Ok(future) => format!("in {:.1}h", future.as_secs_f64() / 3600.0),
        Err(past) => {
            let duration = past.duration();
            if duration.as_secs() < 60 {
                format!("{:.0}s ago", duration.as_secs_f64())
            } else if duration.as_secs() < 3600 {
                format!("{:.1}m ago", duration.as_secs_f64() / 60.0)
            } else if duration.as_secs() < 86400 {
                format!("{:.1}h ago", duration.as_secs_f64() / 3600.0)
            } else {
                format!("{:.1}d ago", duration.as_secs_f64() / 86400.0)
            }
        }
    }
}

fn cmd_fast_count(input: PathBuf, flags: FlagFilter, threads: Option<usize>) -> Result<()> {
    let input_str = input.to_str().ok_or_else(|| anyhow!("Invalid file path"))?;

    // Parse flags using the shared FlagFilter logic
    let (required_flags, forbidden_flags) = flags.gather_bits()?;

    eprintln!("Fast count mode (no index building)");
    eprintln!("   File: {}", input_str);
    eprintln!("   Required flags: 0x{:x}", required_flags);
    eprintln!("   Forbidden flags: 0x{:x}", forbidden_flags);

    let start = std::time::Instant::now();
    let count = scan_count(input_str, required_flags, forbidden_flags, threads)?;
    let scan_time = start.elapsed();

    println!("{}", count);
    eprintln!("Scan completed in {:.3}s", scan_time.as_secs_f64());

    Ok(())
}
