use anyhow::Result;
use bafiq::FlagIndex;
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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::View(args) => view_index(args),
        Commands::Stats(args) => show_stats(args),
    }
}

fn view_index(args: &ViewArgs) -> Result<()> {
    let index = FlagIndex::from_file(&args.index_file)?;
    
    if let Some(flag_str) = &args.flag {
        view_specific_flag(&index, flag_str)?;
        return Ok(());
    }

    let bins = index.bins();
    let mut displayed = 0;
    let mut line_count = 0;
    
    println!("Index file: {}", args.index_file.display());
    println!("Total bins: {}", bins.len());
    println!("Total records: {}", index.total_records());
    println!("---");

    for (i, bin) in bins.iter().enumerate() {
        let total_reads = bin.total_reads();
        
        if args.non_empty && total_reads == 0 {
            continue;
        }

        let flag_value = i as u16;
        println!("Bin {}: Flag 0x{:03x} ({} reads)", i, flag_value, total_reads);
        
        if total_reads > 0 {
            if let Some(summaries) = index.bin_block_summaries(i) {
                for (block_id, count) in summaries {
                    println!("  Block {}: {} reads", block_id, count);
                }
            }
        }
        
        displayed += 1;
        line_count += if total_reads > 0 { 
            index.bin_block_summaries(i).map(|s| s.len()).unwrap_or(0) + 1
        } else { 
            1 
        };
        
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
    println!("Displayed {} bins", displayed);
    Ok(())
}

fn view_specific_flag(index: &FlagIndex, flag_str: &str) -> Result<()> {
    let flag_value = if flag_str.starts_with("0x") || flag_str.starts_with("0X") {
        u16::from_str_radix(&flag_str[2..], 16)?
    } else {
        flag_str.parse::<u16>()?
    };

    let bin_idx = (flag_value & 0xFFF) as usize;
    let bin = &index.bins()[bin_idx];
    let total_reads = bin.total_reads();

    println!("Flag 0x{:03x} (bin {}):", flag_value, bin_idx);
    println!("Total reads: {}", total_reads);
    
    if total_reads == 0 {
        println!("No reads with this flag combination.");
        return Ok(());
    }

    println!("Block distribution:");
    if let Some(summaries) = index.bin_block_summaries(bin_idx) {
        for (block_id, count) in summaries {
            println!("  Block {}: {} reads", block_id, count);
        }
    }

    // Show flag breakdown
    println!("\nFlag bits breakdown:");
    show_flag_bits(flag_value);

    Ok(())
}

fn show_stats(args: &StatsArgs) -> Result<()> {
    let index = FlagIndex::from_file(&args.index_file)?;
    
    println!("Index Statistics");
    println!("================");
    println!("File: {}", args.index_file.display());
    println!("Total records: {}", index.total_records());
    println!("Total bins: {}", index.bins().len());
    
    // Count non-empty bins
    let non_empty_bins = index.bins().iter().filter(|bin| bin.total_reads() > 0).count();
    println!("Non-empty bins: {}", non_empty_bins);
    println!("Empty bins: {}", index.bins().len() - non_empty_bins);
    
    // Get flag summary and sort by count
    let mut flag_summary = index.get_flag_summary();
    flag_summary.sort_by(|a, b| b.1.cmp(&a.1));
    
    println!("\nTop {} most frequent flags:", args.top);
    println!("{:<6} {:<12} {:<12} {:<8}", "Flag", "Count", "Percentage", "Bits");
    println!("{}", "-".repeat(50));
    
    let total_records = index.total_records();
    for (flag, count) in flag_summary.iter().take(args.top) {
        let percentage = if total_records > 0 {
            (*count as f64 / total_records as f64) * 100.0
        } else {
            0.0
        };
        println!("{:<6} {:<12} {:<12.2} {}", 
                 format!("0x{:03x}", flag), 
                 count, 
                 percentage,
                 format_flag_bits(*flag));
    }
    
    // Block statistics
    println!("\nBlock Statistics:");
    let mut all_blocks = std::collections::HashSet::new();
    let mut block_counts = std::collections::HashMap::new();
    
    for (i, _bin) in index.bins().iter().enumerate() {
        if let Some(summaries) = index.bin_block_summaries(i) {
            for (block_id, count) in summaries {
                all_blocks.insert(*block_id);
                *block_counts.entry(*block_id).or_insert(0u64) += count;
            }
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
        println!("Reads per block: min={}, max={}, avg={:.1}", min_reads, max_reads, avg_reads);
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