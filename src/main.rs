use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use rust_htslib::bam::{Format, Read as BamRead, Writer};
use std::path::{Path, PathBuf};

use bafiq::FlagIndex;
mod tmp;

/// Named flags that map to specific bits in the SAM flag.
/// We follow common bits used by samtools:
///   - read-unmapped (0x4)
///   - mate-unmapped (0x8)
///   - pcr-dup (0x400)
///   - secondary (0x100)
/// etc.
#[derive(Debug, Clone, Copy)]
struct NamedFlag {
    pub name: &'static str,
    /// Bits to *require* if user passes `--flag <NAME>`
    pub bits_set: u16,
    /// Bits to *forbid* if user passes `--no-flag <NAME>`
    pub bits_forbid: u16,
}

static NAMED_FLAGS: &[NamedFlag] = &[
    NamedFlag {
        name: "read-unmapped",
        bits_set: 0x4,
        bits_forbid: 0x4,
    },
    NamedFlag {
        name: "mate-unmapped",
        bits_set: 0x8,
        bits_forbid: 0x8,
    },
    NamedFlag {
        name: "pcr-dup",
        bits_set: 0x400,
        bits_forbid: 0x400,
    },
    NamedFlag {
        name: "secondary",
        bits_set: 0x100,
        bits_forbid: 0x100,
    },
    // Add others as needed, e.g. "supplementary" => 0x800, "proper-pair" => 0x2, etc.
];

fn find_named_flag(name: &str) -> Option<NamedFlag> {
    NAMED_FLAGS.iter().copied().find(|nf| nf.name == name)
}

/// Each subcommand (count/view) can specify:
/// - A single BAM/CRAM input
/// - Optional `-f INT` and `-F INT` for required/forbidden bits
/// - Repeated `--flag <NAME>` or `--no-flag <NAME>`
#[derive(Debug, Parser)]
pub struct SharedArgs {
    /// Include only reads with *all* bits in this INT set (like samtools -f)
    #[arg(short = 'f', long = "include-flags", default_value = "0")]
    pub include_flags: u16,

    /// Exclude reads with *any* bits in this INT (like samtools -F)
    #[arg(short = 'F', long = "exclude-flags", default_value = "0")]
    pub exclude_flags: u16,

    /// Named flags to require (e.g. --flag read-unmapped)
    #[arg(long = "flag", value_name = "NAME", num_args = 0..)]
    pub flags_to_include: Vec<String>,

    /// Named flags to exclude (e.g. --no-flag read-unmapped)
    #[arg(long = "no-flag", value_name = "NAME", num_args = 0..)]
    pub flags_to_exclude: Vec<String>,

    /// The input BAM/CRAM file
    pub input: PathBuf,
}

#[derive(Debug, Parser)]
pub struct IndexArgs {
    /// The input BAM/CRAM file
    pub input: PathBuf,
}

impl SharedArgs {
    /// Combine the integer flags and named flags into `(required_bits, forbidden_bits)`.
    /// Samtools logic:
    ///   - "-f X" means all bits in X must be set.
    ///   - "-F X" means none of the bits in X may be set.
    ///
    /// Then we also merge in the named flags (like `--flag read-unmapped` => 0x4 in required_bits).
    pub fn gather_bits(&self) -> Result<(u16, u16)> {
        let mut required_bits = self.include_flags;
        let mut forbidden_bits = self.exclude_flags;

        // Merge named flags to include
        for name in &self.flags_to_include {
            if let Some(nf) = find_named_flag(name) {
                required_bits |= nf.bits_set;
            } else {
                return Err(anyhow!("Unrecognized named flag: {}", name));
            }
        }

        // Merge named flags to exclude
        for name in &self.flags_to_exclude {
            if let Some(nf) = find_named_flag(name) {
                forbidden_bits |= nf.bits_forbid;
            } else {
                return Err(anyhow!("Unrecognized named flag: {}", name));
            }
        }

        Ok((required_bits, forbidden_bits))
    }
}

/// The top-level CLI definition with two subcommands: `count` and `view`.
#[derive(Debug, Subcommand)]
enum Commands {
    /// Count how many reads match the given flag criteria
    Count(SharedArgs),

    /// View (i.e., retrieve/print) reads that match the given flag criteria (SAM output to stdout)
    View(SharedArgs),
    /// Build the index for the given BAM/CRAM file
    Index(IndexArgs),
    /// Build the index for the given BAM/CRAM file
    Tmp(IndexArgs),
}

#[derive(Debug, Parser)]
#[command(name = "bafiq", author, version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Commands::Count(args) => cmd_count(args),
        Commands::View(args) => cmd_view(args),
        Commands::Index(args) => cmd_index(args),
        Commands::Tmp(args) => cmd_tmp(args),
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

fn cmd_tmp(args: IndexArgs) -> Result<()> {
    // let block_count = tmp::count_blocks(&args.input)?;
    // println!("Block count: {}", block_count);
    let record_count = tmp::count_bam_records_in_bam_file_minimal(&args.input.to_str().unwrap())?;
    println!("Record count: {}", record_count);
    Ok(())
}

/// `bafiq index <input.bam>`
fn cmd_index(args: IndexArgs) -> Result<()> {
    let index_path = get_index_path(&args.input);
    let index = FlagIndex::from_path(&args.input)?;
    eprintln!("Saving index to: {:?}", index_path);
    index.save_to_file(&index_path)?;
    Ok(())
}

/// `bafiq count [options] <input.bam>`
fn cmd_count(args: SharedArgs) -> Result<()> {
    let (required_bits, forbidden_bits) = args.gather_bits()?;
    let index_path = get_index_path(&args.input);

    if !index_path.exists() {
        cmd_index(IndexArgs { input: args.input })?;
    };

    eprintln!("Loading existing index from: {:?}", index_path);
    let index = FlagIndex::from_file(&index_path)?;

    let count = index.count(required_bits, forbidden_bits);
    println!(
        "Count of reads [required=0x{:X}, forbidden=0x{:X}]: {}",
        required_bits, forbidden_bits, count
    );

    Ok(())
}

/// `bafiq view [options] <input.bam>`
/// Outputs matching reads in SAM format on stdout.
fn cmd_view(args: SharedArgs) -> Result<()> {
    let (required_bits, forbidden_bits) = args.gather_bits()?;
    let index_path = get_index_path(&args.input);

    let index = if index_path.exists() {
        eprintln!("Loading existing index from: {:?}", index_path);
        FlagIndex::from_file(&index_path)?
    } else {
        eprintln!("Building index from file: {:?}", args.input);
        let index = FlagIndex::from_path(&args.input)?;
        eprintln!("Saving index to: {:?}", index_path);
        index.save_to_file(&index_path)?;
        index
    };

    // Open the file again just to extract the header
    let tmp_reader = rust_htslib::bam::Reader::from_path(&args.input)?;
    let header = rust_htslib::bam::Header::from_template(tmp_reader.header());
    // let header = tmp_reader.header().to_owned();

    // Create a SAM writer to stdout
    let mut writer = Writer::from_stdout(&header, Format::Sam)?;

    // Retrieve matching reads
    index.retrieve_reads(&args.input, required_bits, forbidden_bits, &mut writer)?;

    Ok(())
}
