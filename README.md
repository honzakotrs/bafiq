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

# Run `bafiq index` benchmark with different indexing strategies
# alongside `bafiq fast-scan` and `samtools -c`
# it repeats the run for each number of threads

BENCH_THREADS="2,max" BAFIQ_TEST_BAM=/my.bam just bench

# Run `bafiq view` benchmark; will run for each combination of values of set env
# variables, comma separated

BENCH_THREADS="2,max" BAFIQ_FLAGS="0x4,0x10,0x2" BAFIQ_SOURCE_BAMS="hg38.chr22.bam,chr1.bam" just bench-view

# See all available commands
just help
```

## Manual Build

```bash
just build
```
