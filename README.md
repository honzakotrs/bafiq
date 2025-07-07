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

# Run interactive benchmark
just bench-interactive /path/to/test.bam

# See all available commands
just help
```

## Manual Build

```bash
cargo build --release
```

## Performance Testing & Development

bafiq includes comprehensive performance testing tools for validating optimizations:

### Interactive Benchmarking (Preferred)

```bash
# Using just (recommended)
just bench /path/to/test.bam
```

### Automated Performance Gate

```bash
# Using just (recommended)
export BAFIQ_TEST_BAM=/path/to/test.bam
just bench

# Or manually
export BAFIQ_TEST_BAM=/path/to/test.bam
cargo bench
```

The performance gate requires the low-level approach to be at least 20% faster than rust-htslib while maintaining 100% correctness.

## Data for benchmarking

- WGS 30x, short-reads: https://ftp.ncbi.nlm.nih.gov/ReferenceSamples/giab/data/NA12878/NIST_NA12878_HG001_HiSeq_300x/
- 

## Version history

### 0.0.5

- added multiple strategies to build the index (stream, chunk, parellel)
- introduced 3 layers of index compression to push down the final size (actually sped up the index due to reduced unused flag space)
- added benchmarking via Criterion

### 0.0.4

- Use libdeflater for decompression to achieve 2x speedup in BAM record counting / reading