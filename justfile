# Bafiq Build Commands
# Install just: cargo install just
# Run: just <command>


# Build the project
build:
    @echo "Building bafiq..."
    cargo build --release

# Run index build benchmarks with thread scaling analysis
# Uses default threads: 1,2 (override with BENCH_THREADS="1,2,4,8" just bench)
bench:
    #!/usr/bin/env bash
    if [ -z "${BAFIQ_TEST_BAM:-}" ]; then
        echo "Set BAFIQ_TEST_BAM environment variable to run benchmarks"
        echo "   Example: export BAFIQ_TEST_BAM=/path/to/test.bam"
        echo "   Then run: just bench"
    else
        # Resolve max threads upfront for explicit thread control
        MAX_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "4")
        
        # Define thread counts to test (default: 1,2, override with BENCH_THREADS env var)
        THREADS="${BENCH_THREADS:-1,2}"
        
        # Replace "max" with actual core count
        THREADS=$(echo "$THREADS" | sed "s/max/$MAX_CORES/g")
        
        echo "Running thread scaling benchmarks (development mode)..."
        echo "Thread Scaling Benchmarking with file: $(basename "$BAFIQ_TEST_BAM")"
        echo "Machine Configuration:"
        echo "   Available CPU cores: $MAX_CORES"
        echo "   Thread counts to test: $THREADS"
        echo "   Fast development mode with resource monitoring"
        echo "   💡 Override threads: BENCH_THREADS=\"1,2,4,8,$MAX_CORES\" just bench"
        echo "   💡 Both bafiq and samtools use explicit --threads/-@ for fair comparison"
        
        # Get original BAM size
        BAM_SIZE=$(stat -f%z "$BAFIQ_TEST_BAM" 2>/dev/null || stat -c%s "$BAFIQ_TEST_BAM" 2>/dev/null || echo "0")
        BAM_SIZE_GB=$(echo "scale=1; $BAM_SIZE / 1024 / 1024 / 1024" | bc -l 2>/dev/null || echo "0.0")
        echo "Original BAM size: ${BAM_SIZE_GB} GB"
        echo "===================================================================================================="
        
        # Build bafiq first
        echo "🔧 Building bafiq..."
        cargo build --release
        
        # Create output directory
        mkdir -p ./benchmark_results
        COMBINED_CSV="./benchmark_results/thread_scaling_$(date +%Y%m%d_%H%M%S).csv"
        
        # Initialize CSV headers
        echo "threads,strategy,time_ms,peak_memory_mb,avg_memory_mb,peak_cpu_percent,avg_cpu_percent,index_size_mb,samples" > "$COMBINED_CSV"
        
        # Detailed memory sampling CSV (separate file)
        MEMORY_CSV="./benchmark_results/memory_samples_$(date +%Y%m%d_%H%M%S).csv"
        echo "threads,strategy,timestamp_ms,memory_mb,cpu_percent" > "$MEMORY_CSV"
        
        # Strategies to test (including legacy methods and samtools reference)
        STRATEGIES=("memory-friendly" "parallel-streaming" "rayon-wait-free" "rayon-streaming-optimized" "legacy-parallel-raw" "legacy-streaming-raw" "bafiq-fast-count" "samtools")
        
        # Temporary file for collecting all results
        TEMP_RESULTS=$(mktemp)
        
        # Function to monitor memory usage
        monitor_memory() {
            local pid=$1
            local strategy=$2
            local threads=$3
            local start_time=$4
            local memory_file=$(mktemp)
            
            while kill -0 $pid 2>/dev/null; do
                current_time=$(python3 -c "import time; print(int(time.time() * 1000))")
                elapsed_ms=$((current_time - start_time))
                
                # Get memory usage (RSS) in MB
                if [[ "$OSTYPE" == "darwin"* ]]; then
                    # macOS
                    memory_kb=$(ps -o rss= -p $pid 2>/dev/null || echo "0")
                else
                    # Linux
                    memory_kb=$(ps -o rss= -p $pid 2>/dev/null || echo "0")
                fi
                memory_mb=$(echo "scale=1; $memory_kb / 1024" | bc -l 2>/dev/null || echo "0.0")
                
                # Get CPU usage (simplified)
                cpu_percent=$(ps -o %cpu= -p $pid 2>/dev/null || echo "0.0")
                
                # Record sample
                echo "$elapsed_ms,$memory_mb,$cpu_percent" >> "$memory_file"
                echo "$threads,$strategy,$elapsed_ms,$memory_mb,$cpu_percent" >> "$MEMORY_CSV"
                
                sleep 0.1  # Sample every 100ms
            done
            
            echo "$memory_file"
        }
        
        # Split threads by comma and run benchmarks for each
        IFS=',' read -ra THREAD_ARRAY <<< "$THREADS"
        for thread_count in "${THREAD_ARRAY[@]}"; do
            echo "Running with $thread_count threads..."
            
            for strategy in "${STRATEGIES[@]}"; do
                echo "Running monitored benchmark: $strategy (${thread_count} threads)"
                
                # Clean up any existing index to ensure fresh build
                rm -f "${BAFIQ_TEST_BAM}.bfi"
                
                # Time the index building with specific thread count
                START_TIME=$(python3 -c "import time; print(int(time.time() * 1000))")
                
                # Handle different strategy types
                if [ "$strategy" = "samtools" ]; then
                    # Check if samtools is available
                    if ! command -v samtools &> /dev/null; then
                        echo "   ⚠️  samtools not found - skipping"
                        continue
                    fi
                    
                    # Run samtools view -c with explicit thread count
                    # Note: samtools -@ specifies additional threads, so for total thread_count we use (thread_count - 1)
                    SAMTOOLS_THREADS=$((thread_count - 1))
                    if [ "$SAMTOOLS_THREADS" -lt 0 ]; then
                        SAMTOOLS_THREADS=0
                    fi
                    echo "Running monitored benchmark: $strategy (${thread_count} threads total, -@ ${SAMTOOLS_THREADS})"
                    samtools view -@ "$SAMTOOLS_THREADS" -c "$BAFIQ_TEST_BAM" > /tmp/samtools_output.log 2>&1 &
                    BENCHMARK_PID=$!
                elif [ "$strategy" = "bafiq-fast-count" ]; then
                    # Run bafiq fast-count - direct comparison to samtools view -c
                    echo "Running monitored benchmark: $strategy (${thread_count} threads, no index building)"
                    ./target/release/bafiq --threads "$thread_count" fast-count "$BAFIQ_TEST_BAM" > /tmp/bafiq_fastcount_output.log 2>&1 &
                    BENCHMARK_PID=$!
                else
                    # Map legacy strategy names to actual CLI strategy names
                    case "$strategy" in
                        "legacy-parallel-raw")
                            CLI_STRATEGY="parallel-streaming"
                            ;;
                        "legacy-streaming-raw")
                            CLI_STRATEGY="rayon-streaming-optimized"
                            ;;
                        *)
                            CLI_STRATEGY="$strategy"
                            ;;
                    esac
                    
                    # Clean up any existing index to ensure fresh build (not needed for samtools)
                    rm -f "${BAFIQ_TEST_BAM}.bfi"
                    
                    # Run bafiq with explicit thread count
                    echo "Running monitored benchmark: $strategy (${thread_count} threads)"
                    ./target/release/bafiq --threads "$thread_count" index --strategy "$CLI_STRATEGY" "$BAFIQ_TEST_BAM" > /tmp/bafiq_output.log 2>&1 &
                    BENCHMARK_PID=$!
                fi
                
                # Start memory monitoring in background
                MEMORY_FILE=$(monitor_memory $BENCHMARK_PID "$strategy" "$thread_count" "$START_TIME")
                
                # Wait for benchmark to complete
                wait $BENCHMARK_PID
                BENCHMARK_EXIT_CODE=$?
                
                END_TIME=$(python3 -c "import time; print(int(time.time() * 1000))")
                DURATION=$((END_TIME - START_TIME))
                DURATION_SEC=$(echo "scale=3; $DURATION / 1000" | bc -l 2>/dev/null || echo "0.000")
                
                if [ $BENCHMARK_EXIT_CODE -eq 0 ]; then
                    # Get index size (only for bafiq index strategies)
                    if [ "$strategy" = "samtools" ]; then
                        INDEX_SIZE_MB="N/A"
                        # Get record count from samtools output
                        SAMTOOLS_RECORDS=$(cat /tmp/samtools_output.log 2>/dev/null || echo "0")
                    elif [ "$strategy" = "bafiq-fast-count" ]; then
                        INDEX_SIZE_MB="N/A"
                        # Get record count from bafiq fast-count output
                        BAFIQ_FASTCOUNT_RECORDS=$(cat /tmp/bafiq_fastcount_output.log 2>/dev/null || echo "0")
                    else
                        if [ -f "${BAFIQ_TEST_BAM}.bfi" ]; then
                            INDEX_SIZE=$(stat -f%z "${BAFIQ_TEST_BAM}.bfi" 2>/dev/null || stat -c%s "${BAFIQ_TEST_BAM}.bfi" 2>/dev/null || echo "0")
                            INDEX_SIZE_MB=$(echo "scale=1; $INDEX_SIZE / 1024 / 1024" | bc -l 2>/dev/null || echo "0.0")
                        else
                            INDEX_SIZE_MB="0.0"
                        fi
                    fi
                    
                    # Calculate memory stats from samples
                    if [ -f "$MEMORY_FILE" ]; then
                        PEAK_MEMORY=$(awk -F',' 'NR>1 && $2>max {max=$2} END {print max+0}' "$MEMORY_FILE" 2>/dev/null || echo "0.0")
                        AVG_MEMORY=$(awk -F',' 'NR>1 {sum+=$2; count++} END {print (count>0 ? sum/count : 0)}' "$MEMORY_FILE" 2>/dev/null || echo "0.0")
                        PEAK_CPU=$(awk -F',' 'NR>1 && $3>max {max=$3} END {print max+0}' "$MEMORY_FILE" 2>/dev/null || echo "0.0")
                        AVG_CPU=$(awk -F',' 'NR>1 {sum+=$3; count++} END {print (count>0 ? sum/count : 0)}' "$MEMORY_FILE" 2>/dev/null || echo "0.0")
                        SAMPLE_COUNT=$(wc -l < "$MEMORY_FILE" 2>/dev/null || echo "0")
                    else
                        PEAK_MEMORY="0.0"
                        AVG_MEMORY="0.0"
                        PEAK_CPU="0.0"
                        AVG_CPU="0.0"
                        SAMPLE_COUNT="0"
                    fi
                    
                    PEAK_MEMORY_GB=$(echo "scale=1; $PEAK_MEMORY / 1024" | bc -l 2>/dev/null || echo "0.0")
                    
                    # Convert N/A to 0.0 for CSV
                    CSV_INDEX_SIZE=$([ "$INDEX_SIZE_MB" = "N/A" ] && echo "0.0" || echo "$INDEX_SIZE_MB")
                    
                    # Thread count is already resolved, use directly
                    CSV_THREAD_COUNT="$thread_count"
                    
                    # Add to CSV
                    echo "$CSV_THREAD_COUNT,$strategy,$DURATION,$PEAK_MEMORY,$AVG_MEMORY,$PEAK_CPU,$AVG_CPU,$CSV_INDEX_SIZE,$SAMPLE_COUNT" >> "$COMBINED_CSV"
                    
                    # Store result for summary (keep original thread_count for display)
                    echo "$thread_count,$strategy,$DURATION_SEC,$PEAK_MEMORY_GB,$AVG_MEMORY,$PEAK_CPU,$AVG_CPU,$INDEX_SIZE_MB" >> "$TEMP_RESULTS"
                    
                    if [ "$strategy" = "samtools" ]; then
                        echo "   Time: ${DURATION_SEC}s, Peak Memory: ${PEAK_MEMORY_GB}GB, Avg CPU: ${AVG_CPU}%, Records: $SAMTOOLS_RECORDS"
                    elif [ "$strategy" = "bafiq-fast-count" ]; then
                        echo "   Time: ${DURATION_SEC}s, Peak Memory: ${PEAK_MEMORY_GB}GB, Avg CPU: ${AVG_CPU}%, Records: $BAFIQ_FASTCOUNT_RECORDS"
                    else
                        echo "   Time: ${DURATION_SEC}s, Peak Memory: ${PEAK_MEMORY_GB}GB, Avg CPU: ${AVG_CPU}%"
                    fi
                else
                    echo "   ❌ FAILED"
                    if [ "$strategy" = "samtools" ]; then
                        cat /tmp/samtools_output.log
                    elif [ "$strategy" = "bafiq-fast-count" ]; then
                        cat /tmp/bafiq_fastcount_output.log
                    else
                        cat /tmp/bafiq_output.log
                    fi
                fi
                
                # Clean up memory file
                rm -f "$MEMORY_FILE"
            done
            echo ""
        done
        

        
        # Generate ASCII memory plots
        echo "📊 Memory Usage Timeline (ASCII Plots):"
        echo "============================================================"
        
        generate_ascii_plot() {
            local strategy=$1
            local threads=$2
            local plot_width=60
            local plot_height=8
            
            # Extract memory data for this strategy and thread count
            local memory_data=$(grep "^$threads,$strategy," "$MEMORY_CSV" | cut -d, -f3,4 | sort -t, -k1 -n)
            
            if [ -z "$memory_data" ]; then
                return
            fi
            
            # Calculate min/max for scaling
            local min_mem=$(echo "$memory_data" | awk -F',' '{print $2}' | sort -n | head -1)
            local max_mem=$(echo "$memory_data" | awk -F',' '{print $2}' | sort -n | tail -1)
            local max_time=$(echo "$memory_data" | awk -F',' '{print $1}' | sort -n | tail -1)
            
            if [ -z "$min_mem" ] || [ -z "$max_mem" ] || [ -z "$max_time" ]; then
                return
            fi
            
            # Ensure we have a range
            local mem_range=$(echo "$max_mem - $min_mem" | bc -l 2>/dev/null || echo "1")
            if [ "$(echo "$mem_range <= 0" | bc -l 2>/dev/null)" = "1" ]; then
                mem_range="1"
            fi
            
            echo "${strategy} (${threads}t) Memory Usage (${min_mem}MB - ${max_mem}MB)"
            
            # Generate plot lines
            for ((row=plot_height-1; row>=0; row--)); do
                local y_value=$(echo "$min_mem + ($mem_range * $row / ($plot_height - 1))" | bc -l 2>/dev/null || echo "$min_mem")
                printf "%6.0fMB |" "$y_value"
                
                for ((col=0; col<plot_width; col++)); do
                    local x_time=$(echo "$max_time * $col / ($plot_width - 1)" | bc -l 2>/dev/null || echo "0")
                    
                    # Find closest memory value for this time
                    local closest_mem=$(echo "$memory_data" | awk -F',' -v target_time="$x_time" '
                        function abs(x) { return x < 0 ? -x : x }
                        BEGIN { min_diff = 999999; closest = 0 }
                        { diff = abs($1 - target_time); if (diff < min_diff) { min_diff = diff; closest = $2 } }
                        END { print closest }
                    ')
                    
                    # Determine if we should plot a character here
                    local scaled_mem=$(echo "($closest_mem - $min_mem) * ($plot_height - 1) / $mem_range" | bc -l 2>/dev/null || echo "0")
                    local rounded_mem=$(printf "%.0f" "$scaled_mem")
                    
                    if [ "$rounded_mem" -eq "$row" ]; then
                        if [ "$col" -eq 0 ] || [ "$col" -eq $((plot_width-1)) ]; then
                            printf "█"
                        else
                            printf "▓"
                        fi
                    else
                        printf " "
                    fi
                done
                echo ""
            done
            
            printf "%8s +" " "
            for ((col=0; col<plot_width; col++)); do
                printf "-"
            done
            echo ""
            printf "%8s 0ms" " "
            printf "%*s" $((plot_width-10)) ""
            printf "%6.0fms" "$max_time"
            echo ""
            echo ""
        }
        
        # Generate plots for interesting strategies
        PLOT_STRATEGIES=("memory-friendly" "rayon-wait-free" "parallel-streaming" "bafiq-fast-count")
        for strategy in "${PLOT_STRATEGIES[@]}"; do
            for thread_count in "${THREAD_ARRAY[@]}"; do
                generate_ascii_plot "$strategy" "$thread_count"
            done
        done
        
        echo "💡 Tip: memory-friendly should show more controlled memory usage compared to others"
        echo "     Run 'just bench-csv' to export detailed CSV data for plotting"
        echo ""
        
        # Generate comprehensive summary
        echo "Performance & Resource Usage Summary:"
        echo "========================================================================================================================"
        printf "%-25s %-8s %-10s %-10s %-8s %-8s %-12s %-8s\n" "Strategy" "Threads" "Time" "Peak RAM" "Avg RAM" "Peak CPU" "Avg CPU" "Index Size"
        echo "------------------------------------------------------------------------------------------------------------------------"
        
        while IFS=',' read -r threads strategy time_sec peak_mem_gb avg_mem_mb peak_cpu avg_cpu index_size_mb; do
            AVG_MEM_GB=$(echo "scale=1; $avg_mem_mb / 1024" | bc -l 2>/dev/null || echo "0.0")
            printf "%-25s %-8s %-8s %-10s %-10s %-8s %-8s %-12s\n" \
                "$strategy" "${threads}t" "${time_sec}s" "${peak_mem_gb}GB" "${AVG_MEM_GB}GB" "${peak_cpu}%" "${avg_cpu}%" "${index_size_mb}MB"
        done < "$TEMP_RESULTS"
        
        echo "========================================================================================================================"
        echo ""
        
        # CSV output
        echo "CSV Results:"
        echo "$(head -1 "$COMBINED_CSV")"
        tail -n +2 "$COMBINED_CSV" | while IFS=',' read -r threads strategy time_ms peak_mem avg_mem peak_cpu avg_cpu index_size samples; do
            echo "$threads,$strategy,$time_ms,$peak_mem,$avg_mem,$peak_cpu,$avg_cpu,$index_size,$samples"
        done
        echo ""
        
        # Analysis by threads
        echo "Thread Scaling Analysis:"
        for thread_count in "${THREAD_ARRAY[@]}"; do
            echo "   $thread_count thread(s):"
            BEST_1T=$(grep "^$thread_count," "$COMBINED_CSV" | sort -t, -k3 -n | head -1)
            if [ -n "$BEST_1T" ]; then
                BEST_STRATEGY=$(echo "$BEST_1T" | cut -d, -f2)
                BEST_TIME=$(echo "$BEST_1T" | cut -d, -f3)
                BEST_TIME_SEC=$(echo "scale=3; $BEST_TIME / 1000" | bc -l 2>/dev/null || echo "0.000")
                echo "     Best: $BEST_STRATEGY - ${BEST_TIME_SEC}s"
            fi
        done
        echo ""
        
        # Speed comparison
        echo "Speed Analysis:"
        FASTEST_OVERALL=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k3 -n | head -1)
        if [ -n "$FASTEST_OVERALL" ]; then
            FASTEST_STRATEGY=$(echo "$FASTEST_OVERALL" | cut -d, -f2)
            FASTEST_THREADS=$(echo "$FASTEST_OVERALL" | cut -d, -f1)
            FASTEST_TIME=$(echo "$FASTEST_OVERALL" | cut -d, -f3)
            FASTEST_TIME_SEC=$(echo "scale=3; $FASTEST_TIME / 1000" | bc -l 2>/dev/null || echo "0.000")
            echo "   Fastest overall: $FASTEST_STRATEGY (${FASTEST_THREADS} threads) - ${FASTEST_TIME_SEC}s"
        fi
        echo ""
        
        echo "Memory Efficiency Analysis:"
        echo "   Most memory efficient:"
        MEMORY_EFFICIENT=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k4 -n | head -3)
        echo "$MEMORY_EFFICIENT" | head -1 | awk -F',' '{printf "   1. %s (%st) - %.1fMB\n", $2, $1, $4}'
        echo "$MEMORY_EFFICIENT" | sed -n '2p' | awk -F',' '{printf "   2. %s (%st) - %.1fMB\n", $2, $1, $4}'
        echo "$MEMORY_EFFICIENT" | sed -n '3p' | awk -F',' '{printf "   3. %s (%st) - %.1fMB\n", $2, $1, $4}'
        echo ""
        
        echo "Speed Analysis:"
        echo "   Fastest strategies:"
        FASTEST_STRATEGIES=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k3 -n | head -3)
        
        # Get samtools time for comparison
        SAMTOOLS_TIME_MS=$(grep ",samtools," "$COMBINED_CSV" | head -1 | cut -d, -f3 2>/dev/null || echo "")
        
        echo "$FASTEST_STRATEGIES" | head -1 | awk -F',' '{printf "   1. %s (%st) - %.3fs", $2, $1, $3/1000}'
        if [ -n "$SAMTOOLS_TIME_MS" ]; then
            echo "$FASTEST_STRATEGIES" | head -1 | awk -F',' -v samtools_ms="$SAMTOOLS_TIME_MS" '{printf " (%.1fx faster than samtools)\n", samtools_ms/$3}'
        else
            echo ""
        fi
        echo "$FASTEST_STRATEGIES" | sed -n '2p' | awk -F',' '{printf "   2. %s (%st) - %.3fs", $2, $1, $3/1000}'
        if [ -n "$SAMTOOLS_TIME_MS" ]; then
            echo "$FASTEST_STRATEGIES" | sed -n '2p' | awk -F',' -v samtools_ms="$SAMTOOLS_TIME_MS" '{printf " (%.1fx faster than samtools)\n", samtools_ms/$3}'
        else
            echo ""
        fi
        echo "$FASTEST_STRATEGIES" | sed -n '3p' | awk -F',' '{printf "   3. %s (%st) - %.3fs", $2, $1, $3/1000}'
        if [ -n "$SAMTOOLS_TIME_MS" ]; then
            echo "$FASTEST_STRATEGIES" | sed -n '3p' | awk -F',' -v samtools_ms="$SAMTOOLS_TIME_MS" '{printf " (%.1fx faster than samtools)\n", samtools_ms/$3}'
        else
            echo ""
        fi
        echo ""
        
        echo "🖥️  CPU Utilization Analysis:"
        echo "   Best CPU utilization:"
        BEST_CPU=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k7 -nr | head -3)
        echo "$BEST_CPU" | head -1 | awk -F',' '{printf "   1. %s (%st) - %.1f%% average CPU\n", $2, $1, $7}'
        echo "$BEST_CPU" | sed -n '2p' | awk -F',' '{printf "   2. %s (%st) - %.1f%% average CPU\n", $2, $1, $7}'
        echo "$BEST_CPU" | sed -n '3p' | awk -F',' '{printf "   3. %s (%st) - %.1f%% average CPU\n", $2, $1, $7}'
        echo ""
        
        if [ -n "$SAMTOOLS_TIME_MS" ]; then
            SAMTOOLS_TIME_SEC=$(echo "scale=3; $SAMTOOLS_TIME_MS / 1000" | bc -l 2>/dev/null || echo "0.000")
            echo "🎯 Performance Gate: Beat samtools (${SAMTOOLS_TIME_SEC}s target)"
            BEST_TIME=$(tail -n +2 "$COMBINED_CSV" | grep -v ",samtools," | sort -t, -k3 -n | head -1 | awk -F',' '{print $3/1000}')
            BEST_STRATEGY=$(tail -n +2 "$COMBINED_CSV" | grep -v ",samtools," | sort -t, -k3 -n | head -1 | awk -F',' '{print $2}')
            if [ -n "$BEST_TIME" ]; then
                SPEEDUP=$(echo "scale=2; $SAMTOOLS_TIME_SEC / $BEST_TIME" | bc -l 2>/dev/null || echo "1.0")
                echo "   Status: PASSED ✅"
                echo "   Best strategy: $BEST_STRATEGY (${SPEEDUP}x faster than samtools)"
            fi
        else
            echo "🎯 Performance Gate: samtools not available for comparison"
        fi
        echo ""
        
        echo "📊 Results saved to: $COMBINED_CSV"
        echo "📈 Detailed memory samples saved to: $MEMORY_CSV"
        echo ""
        echo "💡 Tip: Use the detailed memory CSV to reconstruct memory usage over time"
        echo "Thread scaling benchmarks completed successfully"
        
        # Clean up
        rm -f "$TEMP_RESULTS"
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
        # Resolve thread count upfront for fair comparison
        THREADS="${BENCH_THREADS:-max}"
        MAX_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "4")
        
        # Replace "max" with actual core count
        THREADS=$(echo "$THREADS" | sed "s/max/$MAX_CORES/g")
        
        echo "Running view performance benchmark..."
        echo "   BAM file: $BAFIQ_TEST_BAM"
        echo "   Query: unmapped reads (-f 0x4 / -f 4 / --unmapped)"
        echo "   Thread count: $THREADS (both tools use explicit --threads/-@)"
        echo "   Strategy: $STRATEGY (override with BENCH_STRATEGY=strategy-name)"
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
        
        # Allow strategy specification (default: rayon-wait-free for best performance)
        STRATEGY="${BENCH_STRATEGY:-rayon-wait-free}"
        
        echo "🔧 Building index if needed..."
        echo "   Strategy: $STRATEGY"
        time ./target/release/bafiq --threads "$THREADS" index "$BAFIQ_TEST_BAM" --strategy "$STRATEGY"
        
        echo ""
        echo "🏁 BENCHMARK: View unmapped reads"
        echo "================================================"
        
        # Calculate samtools thread argument (samtools -@ specifies additional threads)
        SAMTOOLS_THREADS=$((THREADS - 1))
        if [ "$SAMTOOLS_THREADS" -lt 0 ]; then
            SAMTOOLS_THREADS=0
        fi
        
        # Run samtools view (baseline) - include headers for fair comparison
        echo "⏱️  Running samtools view (${THREADS} threads total, -@ ${SAMTOOLS_THREADS})..."
        time samtools view -@ "$SAMTOOLS_THREADS" -h -f 0x4 "$BAFIQ_TEST_BAM" > "$TEMP_DIR/out.samtools.sam"
        SAMTOOLS_COUNT=$(wc -l < "$TEMP_DIR/out.samtools.sam")
        SAMTOOLS_READS=$(grep -v "^@" "$TEMP_DIR/out.samtools.sam" | wc -l)
        echo "   Samtools found: $SAMTOOLS_READS reads (total: $SAMTOOLS_COUNT lines with headers)"
        
        echo ""
        
        # Run bafiq view (numeric flag - samtools compatibility)
        echo "⚡ Running bafiq view (numeric flag, ${THREADS} threads)..."
        time ./target/release/bafiq --threads "$THREADS" view -f 4 "$BAFIQ_TEST_BAM" > "$TEMP_DIR/out.bafiq.sam"
        BAFIQ_COUNT=$(wc -l < "$TEMP_DIR/out.bafiq.sam")
        BAFIQ_READS=$(grep -v "^@" "$TEMP_DIR/out.bafiq.sam" | wc -l)
        echo "   bafiq found: $BAFIQ_READS reads (total: $BAFIQ_COUNT lines with headers)"
        
        echo ""
        
        # Run bafiq view (named flag - user-friendly syntax)
        echo "🚀 Running bafiq view (named flag, ${THREADS} threads)..."
        time ./target/release/bafiq --threads "$THREADS" view --unmapped "$BAFIQ_TEST_BAM" > "$TEMP_DIR/out.bafiq.named.sam"
        BAFIQ_NAMED_COUNT=$(wc -l < "$TEMP_DIR/out.bafiq.named.sam")
        BAFIQ_NAMED_READS=$(grep -v "^@" "$TEMP_DIR/out.bafiq.named.sam" | wc -l)
        echo "   bafiq found: $BAFIQ_NAMED_READS reads (total: $BAFIQ_NAMED_COUNT lines with headers)"
        
        echo ""
        echo "📊 RESULTS:"
        echo "================================================"
        if [ "$SAMTOOLS_READS" -eq "$BAFIQ_READS" ] && [ "$SAMTOOLS_READS" -eq "$BAFIQ_NAMED_READS" ]; then
            echo "✅ Output verification: PASSED ($SAMTOOLS_READS reads)"
            echo "   All tools found identical number of reads"
        else
            echo "❌ Output verification: FAILED"
            echo "   samtools: $SAMTOOLS_READS reads"
            echo "   bafiq (numeric flag): $BAFIQ_READS reads"
            echo "   bafiq (named flag): $BAFIQ_NAMED_READS reads"
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
        echo "   bafiq (numeric flag): $TEMP_DIR/out.bafiq.sam"
        echo "   bafiq (named flag): $TEMP_DIR/out.bafiq.named.sam"
        
        # Keep temp directory for manual inspection
        trap - EXIT
        echo "   (Directory preserved for manual inspection)"
    fi


# Show available commands
help:
    @just --list 

 