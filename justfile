# Bafiq Build Commands
# Install just: cargo install just
# Run: just <command>


# Build the project
build:
    @echo "Building bafiq..."
    cargo build --release

# Run tests
test:
    @echo "Running tests..."
    cargo test

# Clean build artifacts
clean:
    cargo clean

# Run index benchmarks with Rust-style arguments (replaces old bench command)
bench-index *ARGS:
    #!/usr/bin/env bash
    echo "Building unified benchmark..."
    cargo bench --bench unified --no-run
    echo "Running index benchmark with arguments: {{ARGS}}"
    BENCH_PATH=$(cargo bench --bench unified --no-run 2>&1 | grep "Executable" | sed 's/.*(\(.*\))/\1/')
    if [ -n "$BENCH_PATH" ] && [ -f "$BENCH_PATH" ]; then
        "$BENCH_PATH" index {{ARGS}}
    else
        echo "❌ Could not find benchmark executable at: $BENCH_PATH"
        exit 1
    fi

# Run view benchmarks with Rust-style arguments
bench-view *ARGS:
    #!/usr/bin/env bash
    echo "Building unified benchmark..."
    cargo bench --bench unified --no-run
    echo "Running view benchmark with arguments: {{ARGS}}"
    BENCH_PATH=$(cargo bench --bench unified --no-run 2>&1 | grep "Executable" | sed 's/.*(\(.*\))/\1/')
    if [ -n "$BENCH_PATH" ] && [ -f "$BENCH_PATH" ]; then
        "$BENCH_PATH" view {{ARGS}}
    else
        echo "❌ Could not find benchmark executable at: $BENCH_PATH"
        exit 1
    fi

# Show available commands
help:
    @just --list 

 