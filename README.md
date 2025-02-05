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

## Build

```bash
cargo build --release
```

## Version history

### 0.0.4

- Use libdeflater for decompression to achieve 2x speedup in BAM record counting / reading
