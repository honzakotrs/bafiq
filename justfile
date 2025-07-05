# Bafiq Build Commands
# Install just: cargo install just
# Run: just <command>


# Build the project
build:
    @echo "Building bafiq..."
    cargo build --release

# Run index build benchmarks (fast simple timing by default)
bench:
    #!/usr/bin/env bash
    if [ -z "${BAFIQ_TEST_BAM:-}" ]; then
        echo "Set BAFIQ_TEST_BAM environment variable to run benchmarks"
        echo "   Example: export BAFIQ_TEST_BAM=/path/to/test.bam"
        echo "   Then run: just bench"
    else
        echo "Running fast index build benchmarks..."
        echo "   Simple timing mode for quick iteration (sequential strategy muted)"
        echo "   Optional: Set BAFIQ_BENCH_SEQUENTIAL=1 to include slow sequential strategy"
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo "   On Linux, run with sudo for full cache clearing: sudo -E just bench"
        fi
        cargo bench --bench index_build_bench
    fi

# Run comprehensive index build benchmarks with detailed Criterion analysis
bench-full:
    #!/usr/bin/env bash
    if [ -z "${BAFIQ_TEST_BAM:-}" ]; then
        echo "Set BAFIQ_TEST_BAM environment variable to run benchmarks"
        echo "   Example: export BAFIQ_TEST_BAM=/path/to/test.bam"
        echo "   Then run: just bench-full"
    else
        echo "Running detailed Criterion index build benchmarks..."
        echo "   Statistical analysis with confidence intervals + CSV export (slower)"
        echo "   Sequential strategy muted for faster iteration (set BAFIQ_BENCH_SEQUENTIAL=1 to include)"
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo "   On Linux, run with sudo for full cache clearing: sudo -E just bench-full"
        fi
        BAFIQ_USE_CRITERION=1 cargo bench --bench index_build_bench
    fi

# Run tests
test:
    @echo "Running tests..."
    cargo test

# Clean build artifacts
clean:
    cargo clean

# Benchmark view performance: bafiq vs samtools
bench-view:
    #!/usr/bin/env bash
    if [ -z "${BAFIQ_TEST_BAM:-}" ]; then
        echo "Set BAFIQ_TEST_BAM environment variable to run view benchmarks"
        echo "   Example: export BAFIQ_TEST_BAM=/path/to/test.bam"
        echo "   Then run: just bench-view"
    else
        echo "Running view performance benchmark..."
        echo "   BAM file: $BAFIQ_TEST_BAM"
        echo "   Query: unmapped reads (-f 0x4 / -f 4)"
        echo ""
        
        # Build bafiq first
        echo "Building bafiq..."
        cargo build --release
        
        # Create temp directory
        TEMP_DIR=$(mktemp -d)
        trap "rm -rf $TEMP_DIR" EXIT
        
        # Check if samtools is available
        if ! command -v samtools &> /dev/null; then
            echo "⚠️  samtools not found in PATH. Install with:"
            echo "   macOS: brew install samtools"
            echo "   Linux: sudo apt-get install samtools"
            exit 1
        fi
        
        echo "🔧 Building index if needed..."
        time ./target/release/bafiq index "$BAFIQ_TEST_BAM" --strategy rayon-wait-free
        
        echo ""
        echo "🏁 BENCHMARK: View unmapped reads"
        echo "================================================"
        
        # Run samtools view (baseline) - include headers for fair comparison
        echo "⏱️  Running samtools view..."
        time samtools view -h -f 0x4 "$BAFIQ_TEST_BAM" > "$TEMP_DIR/out.samtools.sam"
        SAMTOOLS_COUNT=$(wc -l < "$TEMP_DIR/out.samtools.sam")
        SAMTOOLS_READS=$(grep -v "^@" "$TEMP_DIR/out.samtools.sam" | wc -l)
        echo "   Samtools found: $SAMTOOLS_READS reads (total: $SAMTOOLS_COUNT lines with headers)"
        
        echo ""
        
        # Run bafiq view (sequential)
        echo "⚡ Running bafiq view (sequential)..."
        time ./target/release/bafiq view -f 4 "$BAFIQ_TEST_BAM" > "$TEMP_DIR/out.bafiq.sam"
        BAFIQ_COUNT=$(wc -l < "$TEMP_DIR/out.bafiq.sam")
        BAFIQ_READS=$(grep -v "^@" "$TEMP_DIR/out.bafiq.sam" | wc -l)
        echo "   bafiq found: $BAFIQ_READS reads (total: $BAFIQ_COUNT lines with headers)"
        
        echo ""
        
        # Run bafiq view (parallel)
        echo "🚀 Running bafiq view --parallel..."
        time ./target/release/bafiq view --parallel -f 4 "$BAFIQ_TEST_BAM" > "$TEMP_DIR/out.bafiq.parallel.sam"
        BAFIQ_PARALLEL_COUNT=$(wc -l < "$TEMP_DIR/out.bafiq.parallel.sam")
        BAFIQ_PARALLEL_READS=$(grep -v "^@" "$TEMP_DIR/out.bafiq.parallel.sam" | wc -l)
        echo "   bafiq parallel found: $BAFIQ_PARALLEL_READS reads (total: $BAFIQ_PARALLEL_COUNT lines with headers)"
        
        echo ""
        echo "📊 RESULTS:"
        echo "================================================"
        if [ "$SAMTOOLS_READS" -eq "$BAFIQ_READS" ] && [ "$SAMTOOLS_READS" -eq "$BAFIQ_PARALLEL_READS" ]; then
            echo "✅ Output verification: PASSED ($SAMTOOLS_READS reads)"
            echo "   All tools found identical number of reads"
        else
            echo "❌ Output verification: FAILED"
            echo "   samtools: $SAMTOOLS_READS reads"
            echo "   bafiq sequential: $BAFIQ_READS reads"
            echo "   bafiq parallel: $BAFIQ_PARALLEL_READS reads"
        fi
        
        # Quick content comparison (first 10 lines)
        echo ""
        echo "🔍 Content comparison (first 10 lines):"
        if head -10 "$TEMP_DIR/out.samtools.sam" | diff - <(head -10 "$TEMP_DIR/out.bafiq.sam") > /dev/null; then
            echo "✅ Content sample matches"
        else
            echo "❌ Content sample differs"
            echo "   Run 'diff $TEMP_DIR/out.samtools.sam $TEMP_DIR/out.bafiq.sam' for details"
        fi
        
        echo ""
        echo "📁 Output files saved to: $TEMP_DIR"
        echo "   samtools: $TEMP_DIR/out.samtools.sam"
        echo "   bafiq sequential: $TEMP_DIR/out.bafiq.sam"
        echo "   bafiq parallel: $TEMP_DIR/out.bafiq.parallel.sam"
        
        # Keep temp directory for manual inspection
        trap - EXIT
        echo "   (Directory preserved for manual inspection)"
    fi

# Show available commands
help:
    @just --list 

 