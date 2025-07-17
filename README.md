# bafiq

BAM Flag Index Query

## Usage

Count reads with the unmapped flag and is not a PCR duplicate:

```bash
bafiq count -f 0x4 -F 0x400 my.bam
```

The same as above but using named flags:

```bash
bafiq count --flag read-unmapped --no-flag pcr-dup my.bam
```

Store filtered reads to a subset BAM file:

```bash
bafiq view -f 0x4 -F 0x400 my.bam | samtools view -bS -o unmapped-no-dup.bam
```

## Quick Start (Recommended)

Install [`just`](https://github.com/casey/just) for the best development experience:

```bash
cargo install just
```

Then use the project's built-in commands:

```bash
# Build the project (handles macOS setup automatically)
just build

# See all available commands
just help
```

## Benchmarking

Run comprehensive benchmarks with statistical analysis:

```bash
# Index building benchmarks (multiple strategies vs samtools)
just bench-index --bam my.bam --threads 2,4,8 --samples 5 --clear-cache

# View operation benchmarks (bafiq vs samtools)  
just bench-view --bam my.bam --flags 0x4,0x2 --threads 2,4 --samples 3

# Get help for specific benchmark types
just bench-index --help
just bench-view --help
```

## Legacy Commands

For backwards compatibility, environment variable-based commands are available:

```bash
# Legacy index benchmarks 
BENCH_THREADS="2,max" BAFIQ_TEST_BAM=/my.bam just bench-index-orig

# Legacy view benchmarks (supports multiple combinations)
BENCH_THREADS="2,max" BAFIQ_FLAGS="0x4,0x10,0x2" BAFIQ_SOURCE_BAMS="hg38.chr22.bam,chr1.bam" just bench-view-orig
```

## Manual Build

```bash
just build
```
