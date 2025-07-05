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

# Show available commands
help:
    @just --list 

 