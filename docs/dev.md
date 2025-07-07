# Development Notes

Build command

```bash
cargo build -r --target-dir /Volumes/T7-2000/bafiq
```

Run chr22

```bash
cd /Volumes/T7-2000/bafiq
release/bafiq index data/hg38.chr22.bam
```

Benchmarking

```bash
time release/bafiq index data/hg38.chr22.bam
time release/bafiq count -f 4 data/hg38.chr22.bam
```
