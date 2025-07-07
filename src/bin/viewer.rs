use anyhow::Result;
use bafiq::{IndexAccessor, SerializableIndex};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bafiq-viewer")]
#[command(about = "Interactive viewer for bafiq index files (.bfi)")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// View index contents (like 'more' command)
    View(ViewArgs),
    /// Show index statistics
    Stats(StatsArgs),
}

#[derive(Args)]
struct ViewArgs {
    /// Path to the .bfi index file
    index_file: PathBuf,
    /// Show only non-empty bins
    #[arg(short, long)]
    non_empty: bool,
    /// Show details for specific flag value (hex format, e.g., 0x4)
    #[arg(short, long)]
    flag: Option<String>,
    /// Number of lines to show at once (0 for all)
    #[arg(short, long, default_value = "20")]
    lines: usize,
}

#[derive(Args)]
struct StatsArgs {
    /// Path to the .bfi index file
    index_file: PathBuf,
    /// Show top N most frequent flags
    #[arg(short, long, default_value = "10")]
    top: usize,
}

/// Load index from file and provide unified access
fn load_index(index_file: &PathBuf) -> Result<SerializableIndex> {
    SerializableIndex::load_from_file(index_file)
}

/// Helper to get bins from either compressed or uncompressed index
fn get_bins_info(index_accessor: &IndexAccessor) -> Vec<(u16, u64, Vec<(i64, u64)>)> {
    match index_accessor {
        IndexAccessor::Uncompressed(flag_index) => flag_index
            .bins()
            .iter()
            .map(|bin| (bin.bin, bin.total_reads(), bin.blocks.clone()))
            .collect(),
        IndexAccessor::Compressed(compressed_index) => {
            let mut bins = Vec::new();
            for flag in compressed_index.used_flags() {
                let blocks = compressed_index.get_bin_blocks(flag);
                let total_reads = blocks.iter().map(|(_, count)| count).sum();
                bins.push((flag, total_reads, blocks));
            }
            bins.sort_by_key(|&(flag, _, _)| flag);
            bins
        }
    }
}

/// Helper to get flag summary from either compressed or uncompressed index
fn get_flag_summary(index_accessor: &IndexAccessor) -> Vec<(u16, u64)> {
    match index_accessor {
        IndexAccessor::Uncompressed(flag_index) => flag_index.get_flag_summary(),
        IndexAccessor::Compressed(compressed_index) => {
            let mut summary = Vec::new();
            for flag in compressed_index.used_flags() {
                let blocks = compressed_index.get_bin_blocks(flag);
                let total_reads = blocks.iter().map(|(_, count)| count).sum();
                if total_reads > 0 {
                    summary.push((flag, total_reads));
                }
            }
            summary
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::View(args) => view_index(args),
        Commands::Stats(args) => show_stats(args),
    }
}

fn view_index(args: &ViewArgs) -> Result<()> {
    let serializable_index = load_index(&args.index_file)?;
    let index_accessor = serializable_index.get_index();

    if let Some(flag_str) = &args.flag {
        view_specific_flag(&index_accessor, flag_str)?;
        return Ok(());
    }

    let bins_info = get_bins_info(&index_accessor);
    let mut displayed = 0;
    let mut line_count = 0;

    println!("Index file: {}", args.index_file.display());

    // Show index format information
    let format_info = serializable_index.get_format_info();
    println!(
        "Index format: {} (compression: {:.2}x)",
        format_info.format_type, format_info.compression_ratio
    );

    println!("Total records: {}", index_accessor.total_records());
    println!("---");

    for (flag_value, total_reads, blocks) in bins_info {
        if args.non_empty && total_reads == 0 {
            continue;
        }

        println!("Flag 0x{:03x}: {} reads", flag_value, total_reads);

        let blocks_len = blocks.len();
        if total_reads > 0 {
            for (block_id, count) in &blocks {
                println!("  Block {}: {} reads", block_id, count);
            }
        }

        displayed += 1;
        line_count += if total_reads > 0 { blocks_len + 1 } else { 1 };

        if args.lines > 0 && line_count >= args.lines {
            print!("Press Enter to continue, 'q' to quit: ");
            use std::io::{self, Write};
            io::stdout().flush()?;

            let mut input = String::new();
            io::stdin().read_line(&mut input)?;

            if input.trim().eq_ignore_ascii_case("q") {
                break;
            }

            line_count = 0;
        }
    }

    println!("---");
    println!("Displayed {} flags", displayed);
    Ok(())
}

fn view_specific_flag(index_accessor: &IndexAccessor, flag_str: &str) -> Result<()> {
    let flag_value = if flag_str.starts_with("0x") || flag_str.starts_with("0X") {
        u16::from_str_radix(&flag_str[2..], 16)?
    } else {
        flag_str.parse::<u16>()?
    };

    println!("Flag 0x{:03x}:", flag_value);

    // Get blocks for this specific flag
    let blocks = match index_accessor {
        IndexAccessor::Uncompressed(flag_index) => {
            // Find the bin with this flag
            if let Some(bin) = flag_index.bins().iter().find(|b| b.bin == flag_value) {
                bin.blocks.clone()
            } else {
                Vec::new()
            }
        }
        IndexAccessor::Compressed(compressed_index) => compressed_index.get_bin_blocks(flag_value),
    };

    let total_reads: u64 = blocks.iter().map(|(_, count)| count).sum();
    println!("Total reads: {}", total_reads);

    if total_reads == 0 {
        println!("No reads with this flag combination.");
        return Ok(());
    }

    println!("Block distribution:");
    for (block_id, count) in blocks {
        println!("  Block {}: {} reads", block_id, count);
    }

    // Show flag breakdown
    println!("\nFlag bits breakdown:");
    show_flag_bits(flag_value);

    Ok(())
}

fn show_stats(args: &StatsArgs) -> Result<()> {
    let serializable_index = load_index(&args.index_file)?;
    let index_accessor = serializable_index.get_index();

    println!("Index Statistics");
    println!("================");
    println!("File: {}", args.index_file.display());

    // Show index format information
    let format_info = serializable_index.get_format_info();
    println!(
        "Index format: {} (compression: {:.2}x)",
        format_info.format_type, format_info.compression_ratio
    );

    let cache_info = serializable_index.get_cache_info();
    println!("Source BAM: {}", cache_info.source_path);
    println!("Index version: {}", cache_info.version);

    println!("Total records: {}", index_accessor.total_records());

    let bins_info = get_bins_info(&index_accessor);
    let non_empty_bins = bins_info
        .iter()
        .filter(|(_, total_reads, _)| *total_reads > 0)
        .count();
    println!("Non-empty flags: {}", non_empty_bins);

    // Get flag summary and sort by count
    let mut flag_summary = get_flag_summary(&index_accessor);
    flag_summary.sort_by(|a, b| b.1.cmp(&a.1));

    println!("\nTop {} most frequent flags:", args.top);
    println!(
        "{:<6} {:<12} {:<12} {:<8}",
        "Flag", "Count", "Percentage", "Bits"
    );
    println!("{}", "-".repeat(50));

    let total_records = index_accessor.total_records();
    for (flag, count) in flag_summary.iter().take(args.top) {
        let percentage = if total_records > 0 {
            (*count as f64 / total_records as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "{:<6} {:<12} {:<12.2} {}",
            format!("0x{:03x}", flag),
            count,
            percentage,
            format_flag_bits(*flag)
        );
    }

    // Block statistics
    println!("\nBlock Statistics:");
    let mut all_blocks = std::collections::HashSet::new();
    let mut block_counts = std::collections::HashMap::new();

    for (_, _, blocks) in bins_info {
        for (block_id, count) in blocks {
            all_blocks.insert(block_id);
            *block_counts.entry(block_id).or_insert(0u64) += count;
        }
    }

    println!("Total blocks: {}", all_blocks.len());
    if !block_counts.is_empty() {
        let min_reads = block_counts.values().min().unwrap_or(&0);
        let max_reads = block_counts.values().max().unwrap_or(&0);
        let avg_reads = if !block_counts.is_empty() {
            block_counts.values().sum::<u64>() as f64 / block_counts.len() as f64
        } else {
            0.0
        };
        println!(
            "Reads per block: min={}, max={}, avg={:.1}",
            min_reads, max_reads, avg_reads
        );
    }

    Ok(())
}

fn show_flag_bits(flag: u16) {
    println!("  Binary: {:012b}", flag);
    println!("  Bits set:");

    let flag_descriptions = [
        (0x001, "read paired"),
        (0x002, "read mapped in proper pair"),
        (0x004, "read unmapped"),
        (0x008, "mate unmapped"),
        (0x010, "read reverse strand"),
        (0x020, "mate reverse strand"),
        (0x040, "first in pair"),
        (0x080, "second in pair"),
        (0x100, "not primary alignment"),
        (0x200, "read fails platform/vendor quality checks"),
        (0x400, "read is PCR or optical duplicate"),
        (0x800, "supplementary alignment"),
    ];

    for (bit, description) in flag_descriptions {
        if flag & bit != 0 {
            println!("    0x{:03x}: {}", bit, description);
        }
    }
}

fn format_flag_bits(flag: u16) -> String {
    let mut bits = Vec::new();

    let flag_names = [
        (0x001, "paired"),
        (0x002, "proper"),
        (0x004, "unmapped"),
        (0x008, "mate_unmapped"),
        (0x010, "reverse"),
        (0x020, "mate_reverse"),
        (0x040, "first"),
        (0x080, "second"),
        (0x100, "secondary"),
        (0x200, "qcfail"),
        (0x400, "dup"),
        (0x800, "supp"),
    ];

    for (bit, name) in flag_names {
        if flag & bit != 0 {
            bits.push(name);
        }
    }

    if bits.is_empty() {
        "none".to_string()
    } else {
        bits.join(",")
    }
}
