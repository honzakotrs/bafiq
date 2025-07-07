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
        echo "   💡 Use auto threads: BENCH_THREADS=\"auto\" just bench (no --threads/-@ parameters)"
        echo "   💡 Show memory plots: MEM_PLOTS=1 just bench"
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
        
        # Strategies to test (core strategies and reference tools)
        STRATEGIES=("adaptive-memory-mapped" "constant-memory" "channel-producer-consumer" "work-stealing" "bafiq-fast-count" "samtools")
        
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
                    
                    # Run samtools view -c with explicit thread count or auto
                    if [ "$thread_count" = "auto" ]; then
                        echo "Running monitored benchmark: $strategy (auto threads, no -@ parameter)"
                        samtools view -c "$BAFIQ_TEST_BAM" > /tmp/samtools_output.log 2>&1 &
                        BENCHMARK_PID=$!
                    else
                        # Note: samtools -@ specifies additional threads, so for total thread_count we use (thread_count - 1)
                        SAMTOOLS_THREADS=$((thread_count - 1))
                        if [ "$SAMTOOLS_THREADS" -lt 0 ]; then
                            SAMTOOLS_THREADS=0
                        fi
                        echo "Running monitored benchmark: $strategy (${thread_count} threads total, -@ ${SAMTOOLS_THREADS})"
                        samtools view -@ "$SAMTOOLS_THREADS" -c "$BAFIQ_TEST_BAM" > /tmp/samtools_output.log 2>&1 &
                        BENCHMARK_PID=$!
                    fi
                elif [ "$strategy" = "bafiq-fast-count" ]; then
                    # Run bafiq fast-count - direct comparison to samtools view -c
                    if [ "$thread_count" = "auto" ]; then
                        echo "Running monitored benchmark: $strategy (auto threads, no --threads parameter)"
                        ./target/release/bafiq fast-count "$BAFIQ_TEST_BAM" > /tmp/bafiq_fastcount_output.log 2>&1 &
                        BENCHMARK_PID=$!
                    else
                        echo "Running monitored benchmark: $strategy (${thread_count} threads, no index building)"
                        ./target/release/bafiq --threads "$thread_count" fast-count "$BAFIQ_TEST_BAM" > /tmp/bafiq_fastcount_output.log 2>&1 &
                        BENCHMARK_PID=$!
                    fi
                else
                    # Use strategy name directly
                    CLI_STRATEGY="$strategy"
                    
                    # Clean up any existing index to ensure fresh build (not needed for samtools)
                    rm -f "${BAFIQ_TEST_BAM}.bfi"
                    
                    # Run bafiq with explicit thread count or auto
                    if [ "$thread_count" = "auto" ]; then
                        echo "Running monitored benchmark: $strategy (auto threads, no --threads parameter)"
                        ./target/release/bafiq index --strategy "$CLI_STRATEGY" "$BAFIQ_TEST_BAM" > /tmp/bafiq_output.log 2>&1 &
                        BENCHMARK_PID=$!
                    else
                        echo "Running monitored benchmark: $strategy (${thread_count} threads)"
                        ./target/release/bafiq --threads "$thread_count" index --strategy "$CLI_STRATEGY" "$BAFIQ_TEST_BAM" > /tmp/bafiq_output.log 2>&1 &
                        BENCHMARK_PID=$!
                    fi
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
                    
                    # Thread count is already resolved, use directly (convert "auto" to 0 for CSV)
                    CSV_THREAD_COUNT="$thread_count"
                    if [ "$thread_count" = "auto" ]; then
                        CSV_THREAD_COUNT="0"
                    fi
                    
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
        

        
        # Generate ASCII memory plots (optional, enable with MEM_PLOTS=1)
        if [ "${MEM_PLOTS:-0}" = "1" ]; then
            echo "Memory Usage Timeline (ASCII Plots):"
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
        PLOT_STRATEGIES=("adaptive-memory-mapped" "constant-memory" "work-stealing" "channel-producer-consumer" "bafiq-fast-count")
        for strategy in "${PLOT_STRATEGIES[@]}"; do
            for thread_count in "${THREAD_ARRAY[@]}"; do
                generate_ascii_plot "$strategy" "$thread_count"
            done
        done
        
            echo "💡 Tip: constant-memory should show more controlled memory usage compared to others"
            echo "     Set MEM_PLOTS=1 to see detailed ASCII memory timeline plots"
            echo ""
        else
            echo "💡 Tip: Set MEM_PLOTS=1 to see ASCII memory timeline plots"
        fi
        
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
        
        echo " CPU Utilization Analysis:"
        echo "   Best CPU utilization:"
        BEST_CPU=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k7 -nr | head -3)
        echo "$BEST_CPU" | head -1 | awk -F',' '{printf "   1. %s (%st) - %.1f%% average CPU\n", $2, $1, $7}'
        echo "$BEST_CPU" | sed -n '2p' | awk -F',' '{printf "   2. %s (%st) - %.1f%% average CPU\n", $2, $1, $7}'
        echo "$BEST_CPU" | sed -n '3p' | awk -F',' '{printf "   3. %s (%st) - %.1f%% average CPU\n", $2, $1, $7}'
        echo ""
        
        if [ -n "$SAMTOOLS_TIME_MS" ]; then
            SAMTOOLS_TIME_SEC=$(echo "scale=3; $SAMTOOLS_TIME_MS / 1000" | bc -l 2>/dev/null || echo "0.000")
            BEST_TIME=$(tail -n +2 "$COMBINED_CSV" | grep -v ",samtools," | sort -t, -k3 -n | head -1 | awk -F',' '{print $3/1000}')
            BEST_STRATEGY=$(tail -n +2 "$COMBINED_CSV" | grep -v ",samtools," | sort -t, -k3 -n | head -1 | awk -F',' '{print $2}')
            if [ -n "$BEST_TIME" ]; then
                SPEEDUP=$(echo "scale=2; $SAMTOOLS_TIME_SEC / $BEST_TIME" | bc -l 2>/dev/null || echo "1.0")
                echo "   Status: PASSED"
                echo "   Best strategy: $BEST_STRATEGY (${SPEEDUP}x faster than samtools)"
            fi
        else
            echo "   Status: samtools not available for comparison"
        fi
        echo ""
        
        echo "Results saved to: $COMBINED_CSV"
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

# Benchmark view performance: bafiq vs samtools (vs sambamba if available) with comprehensive CSV metrics
bench-view:
    #!/usr/bin/env bash
    if [ -z "${BAFIQ_TEST_BAM:-}" ]; then
        echo "Set BAFIQ_TEST_BAM environment variable to run view benchmarks"
        echo "   Example: export BAFIQ_TEST_BAM=/path/to/test.bam"
        echo "   Then run: just bench-view"
    else
        # Resolve max threads upfront for explicit thread control
        MAX_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "4")
        
        # Define thread counts to test (default: 1,2, override with BENCH_THREADS env var)
        THREADS="${BENCH_THREADS:-1,2}"
        
        # Replace "max" with actual core count
        THREADS=$(echo "$THREADS" | sed "s/max/$MAX_CORES/g")
        
        # Define BAM files to test (default: use BAFIQ_TEST_BAM, override with BAFIQ_SOURCE_BAMS)
        if [ -n "${BAFIQ_SOURCE_BAMS:-}" ]; then
            SOURCE_BAMS="$BAFIQ_SOURCE_BAMS"
            echo "Using BAFIQ_SOURCE_BAMS (ignoring BAFIQ_TEST_BAM): $SOURCE_BAMS"
        else
            SOURCE_BAMS="$BAFIQ_TEST_BAM"
        fi
        
        # Define flags to test (default: 0x4 unmapped bit, override with BAFIQ_FLAGS)
        FLAGS="${BAFIQ_FLAGS:-0x4}"
        
        echo "Running comprehensive view performance benchmark with monitoring..."
        echo "View Performance Benchmarking parameters:"
        echo "   BAM files: $SOURCE_BAMS"
        echo "   Flags: $FLAGS"
        echo "Machine Configuration:"
        echo "   Available CPU cores: $MAX_CORES"
        echo "   Thread counts to test: $THREADS"
        echo "   Strategy: $STRATEGY (override with BENCH_STRATEGY=strategy-name)"
        echo "   CSV output and comprehensive memory monitoring"
        echo "   🔧 Will test: bafiq view (+ samtools view + sambamba view if available)"
        echo "   💡 Override threads: BENCH_THREADS=\"1,2,4,8,$MAX_CORES\" just bench-view"
        echo "   💡 Use auto threads: BENCH_THREADS=\"auto\" just bench-view"
        echo "   💡 Multiple BAMs: BAFIQ_SOURCE_BAMS=\"chr1.bam,x.bam\" just bench-view"
        echo "   💡 Multiple flags: BAFIQ_FLAGS=\"0x4,0x2,0x10\" just bench-view"
        echo "   💡 Custom flags: BAFIQ_FLAGS=\"0x100,0x200,0x400\" just bench-view"
        
        # Validate BAM files existence before starting
        for bam_file in "${BAM_ARRAY[@]}"; do
            if [ ! -f "$bam_file" ]; then
                echo "❌ BAM file not found: $bam_file"
                echo "   Please check the file path and permissions"
                exit 1
            fi
        done
        
        echo "======================================================================================================"
        
        # Build bafiq first
        echo "🔧 Building bafiq..."
        cargo build --release
        
        # Create output directory for result files
        mkdir -p ./benchmark_results
        TIMESTAMP=$(date +%Y%m%d_%H%M%S)
        COMBINED_CSV="./benchmark_results/view_performance_${TIMESTAMP}.csv"
        MEMORY_CSV="./benchmark_results/view_memory_samples_${TIMESTAMP}.csv"
        
        # Initialize CSV headers (dynamic based on single-value scenarios)
        if [ "$SINGLE_THREAD" = true ]; then
            echo "bam_file,flag,strategy,time_ms,peak_memory_mb,avg_memory_mb,peak_cpu_percent,avg_cpu_percent,index_size_mb,samples,output_lines,reads_found" > "$COMBINED_CSV"
            echo "bam_file,flag,strategy,timestamp_ms,memory_mb,cpu_percent" > "$MEMORY_CSV"
        else
            echo "bam_file,flag,threads,strategy,time_ms,peak_memory_mb,avg_memory_mb,peak_cpu_percent,avg_cpu_percent,index_size_mb,samples,output_lines,reads_found" > "$COMBINED_CSV"
            echo "bam_file,flag,threads,strategy,timestamp_ms,memory_mb,cpu_percent" > "$MEMORY_CSV"
        fi
        
        # Create temp directory for output files
        TEMP_DIR=$(mktemp -d)
        trap "rm -rf $TEMP_DIR" EXIT
        
        # Check tool availability
        SAMTOOLS_AVAILABLE=false
        SAMBAMBA_AVAILABLE=false
        
        if command -v samtools &> /dev/null; then
            SAMTOOLS_AVAILABLE=true
            SAMTOOLS_VERSION=$(samtools --version 2>/dev/null | head -1 | awk '{print $2}' || echo "unknown")
            echo "✅ samtools found: version $SAMTOOLS_VERSION"
        else
            echo "⚠️  samtools not found in PATH. Install with:"
            echo "   macOS: brew install samtools"
            echo "   Linux: sudo apt-get install samtools"
        fi
        
        if command -v sambamba &> /dev/null; then
            SAMBAMBA_AVAILABLE=true
            SAMBAMBA_VERSION=$(sambamba --version 2>/dev/null | head -1 | awk '{print $2}' || echo "unknown")
            echo "✅ sambamba found: version $SAMBAMBA_VERSION"
        else
            SAMBAMBA_AVAILABLE=false
            echo "ℹ️  sambamba not found in PATH (optional)"
            echo "   Install for additional comparison:"
            echo "   macOS: brew install sambamba"
            echo "   Linux: Download from https://github.com/biod/sambamba/releases"
        fi
        
        # Allow strategy specification (default: work-stealing for best performance)
        STRATEGY="${BENCH_STRATEGY:-work-stealing}"
        
        # Function to monitor memory usage (updated with BAM file and flag columns)
        monitor_memory() {
            local pid=$1
            local strategy=$2
            local threads=$3
            local start_time=$4
            local bam_file=$5
            local flag=$6
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
                
                # Record sample to CSV
                echo "$elapsed_ms,$memory_mb,$cpu_percent" >> "$memory_file"
                if [ "$SINGLE_THREAD" = true ]; then
                    echo "$bam_file,$flag,$strategy,$elapsed_ms,$memory_mb,$cpu_percent" >> "$MEMORY_CSV"
                else
                    echo "$bam_file,$flag,$threads,$strategy,$elapsed_ms,$memory_mb,$cpu_percent" >> "$MEMORY_CSV"
                fi
                
                sleep 0.1  # Sample every 100ms
            done
            
            echo "$memory_file"
        }
        
        # Benchmark strategies to test based on availability
        STRATEGIES=()
        if [ "$SAMTOOLS_AVAILABLE" = true ]; then
            STRATEGIES+=("samtools-view")
        fi
        STRATEGIES+=("bafiq-view")
        if [ "$SAMBAMBA_AVAILABLE" = true ]; then
            STRATEGIES+=("sambamba-view")
        fi
        
        echo ""
        echo "🧪 Tools to benchmark: ${STRATEGIES[*]}"
        if [ "$SAMTOOLS_AVAILABLE" = false ]; then
            echo "   ⚠️  samtools will be skipped (not found)"
        fi
        if [ "$SAMBAMBA_AVAILABLE" = false ]; then
            echo "   ℹ️  sambamba will be skipped (not found, optional)"
        fi
        echo ""
        
        # Temporary file for collecting all results
        TEMP_RESULTS=$(mktemp)
        
        # Split comma-separated values for loops (using explicit subshell to avoid IFS issues)
        THREAD_ARRAY=($(echo "$THREADS" | tr ',' ' '))
        BAM_ARRAY=($(echo "$SOURCE_BAMS" | tr ',' ' '))
        FLAG_ARRAY=($(echo "$FLAGS" | tr ',' ' '))
        
        # Detect single-value scenarios for simplified output
        SINGLE_THREAD=false
        SINGLE_BAM=false
        SINGLE_FLAG=false
        
        if [ ${#THREAD_ARRAY[@]} -eq 1 ]; then
            SINGLE_THREAD=true
        fi
        if [ ${#BAM_ARRAY[@]} -eq 1 ]; then
            SINGLE_BAM=true
        fi
        if [ ${#FLAG_ARRAY[@]} -eq 1 ]; then
            SINGLE_FLAG=true
        fi
        
        # Nested loops for BAM files and flags
        for bam_file in "${BAM_ARRAY[@]}"; do
            echo "=========================="
            echo "BAM FILE: $(basename "$bam_file")"
            echo "=========================="
            
            # Validate BAM file exists
            if [ ! -f "$bam_file" ]; then
                echo "❌ BAM file not found: $bam_file"
                continue
            fi
            
            # Get BAM file size for reporting
            BAM_SIZE=$(stat -f%z "$bam_file" 2>/dev/null || stat -c%s "$bam_file" 2>/dev/null || echo "0")
            BAM_SIZE_GB=$(echo "scale=1; $BAM_SIZE / 1024 / 1024 / 1024" | bc -l 2>/dev/null || echo "0.0")
            echo "BAM size: ${BAM_SIZE_GB} GB"
            
            for flag in "${FLAG_ARRAY[@]}"; do
                echo "--------"
                echo "FLAG: $flag"
                echo "--------"
                
                for thread_count in "${THREAD_ARRAY[@]}"; do
                    if [ "$SINGLE_THREAD" = false ]; then
                        echo "Running with $thread_count threads..."
                    fi
                    
                    # Convert "auto" to 0 for CSV consistency
                    CSV_THREADS="$thread_count"
                    if [ "$thread_count" = "auto" ]; then
                        CSV_THREADS="0"
                    fi
                    
                    # Check if index exists
                    if [ ! -f "${bam_file}.bfi" ]; then
                        echo "⚠️  Warning: Index not found for $(basename "$bam_file")"
                        echo "   Expected: ${bam_file}.bfi"
                        echo "   Please build index first: bafiq index \"$bam_file\""
                        continue
                    fi
            
                    echo ""
                    if [ "$SINGLE_THREAD" = true ]; then
                        echo "🏁 BENCHMARK: View flag $flag"
                    else
                        echo "🏁 BENCHMARK: View flag $flag ($thread_count threads)"
                    fi
                    echo "========================================================="
                    
                    for strategy in "${STRATEGIES[@]}"; do
                        echo "Running $strategy..."
                        
                        START_TIME=$(python3 -c "import time; print(int(time.time() * 1000))")
                        
                        case "$strategy" in
                            "samtools-view")
                                if [ "$thread_count" = "auto" ]; then
                                    if [ "$SINGLE_THREAD" = true ]; then
                                        echo "   ⏱️  samtools view"
                                    else
                                        echo "   ⏱️  samtools view (auto threads, no -@ parameter)"
                                    fi
                                    samtools view -h -f $flag "$bam_file" > "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" &
                                    BENCHMARK_PID=$!
                                else
                                    SAMTOOLS_THREADS=$((thread_count - 1))
                                    if [ "$SAMTOOLS_THREADS" -lt 0 ]; then
                                        SAMTOOLS_THREADS=0
                                    fi
                                    if [ "$SINGLE_THREAD" = true ]; then
                                        echo "   ⏱️  samtools view"
                                    else
                                        echo "   ⏱️  samtools view (${thread_count} threads total, -@ ${SAMTOOLS_THREADS})"
                                    fi
                                    samtools view -@ "$SAMTOOLS_THREADS" -h -f $flag "$bam_file" > "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" &
                                    BENCHMARK_PID=$!
                                fi
                                ;;
                            "bafiq-view")
                                if [ "$thread_count" = "auto" ]; then
                                    if [ "$SINGLE_THREAD" = true ]; then
                                        echo "   🚀 bafiq view"
                                    else
                                        echo "   🚀 bafiq view (auto threads)"
                                    fi
                                    ./target/release/bafiq view -f $flag "$bam_file" > "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" &
                                    BENCHMARK_PID=$!
                                else
                                    if [ "$SINGLE_THREAD" = true ]; then
                                        echo "   🚀 bafiq view"
                                    else
                                        echo "   🚀 bafiq view (${thread_count} threads)"
                                    fi
                                    ./target/release/bafiq --threads "$thread_count" view -f $flag "$bam_file" > "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" &
                                    BENCHMARK_PID=$!
                                fi
                                ;;
                            "sambamba-view")
                                if [ "$thread_count" = "auto" ]; then
                                    if [ "$SINGLE_THREAD" = true ]; then
                                        echo "   🔧 sambamba view"
                                    else
                                        echo "   🔧 sambamba view (auto threads)"
                                    fi
                                    sambamba view -h -f "flag & $flag != 0" "$bam_file" > "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" &
                                    BENCHMARK_PID=$!
                                else
                                    if [ "$SINGLE_THREAD" = true ]; then
                                        echo "   🔧 sambamba view"
                                    else
                                        echo "   🔧 sambamba view (${thread_count} threads)"
                                    fi
                                    sambamba view -t "$thread_count" -h -f "flag & $flag != 0" "$bam_file" > "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" &
                                    BENCHMARK_PID=$!
                                fi
                                ;;
                        esac
                        
                        # Start memory monitoring in background
                        MEMORY_FILE=$(monitor_memory $BENCHMARK_PID "$strategy" "$CSV_THREADS" "$START_TIME" "$(basename "$bam_file")" "$flag")
                
                # Wait for benchmark to complete
                wait $BENCHMARK_PID
                BENCHMARK_EXIT_CODE=$?
                
                END_TIME=$(python3 -c "import time; print(int(time.time() * 1000))")
                DURATION=$((END_TIME - START_TIME))
                DURATION_SEC=$(echo "scale=3; $DURATION / 1000" | bc -l 2>/dev/null || echo "0.000")
                
                if [ $BENCHMARK_EXIT_CODE -eq 0 ]; then
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
                    
                        # Count output lines and reads
                        case "$strategy" in
                            "samtools-view")
                                OUTPUT_LINES=$(wc -l < "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" 2>/dev/null || echo "0")
                                READS_FOUND=$(grep -v "^@" "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" 2>/dev/null | wc -l || echo "0")
                                ;;
                            "bafiq-view")
                                OUTPUT_LINES=$(wc -l < "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" 2>/dev/null || echo "0")
                                READS_FOUND=$(grep -v "^@" "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" 2>/dev/null | wc -l || echo "0")
                                ;;
                            "sambamba-view")
                                OUTPUT_LINES=$(wc -l < "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" 2>/dev/null || echo "0")
                                READS_FOUND=$(grep -v "^@" "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" 2>/dev/null | wc -l || echo "0")
                                ;;
                        esac
                        
                        # No index size for view operations
                        INDEX_SIZE_MB="0.0"
                        
                        # Add to CSV with additional metrics (conditional thread column)
                        if [ "$SINGLE_THREAD" = true ]; then
                            echo "$(basename "$bam_file"),$flag,$strategy,$DURATION,$PEAK_MEMORY,$AVG_MEMORY,$PEAK_CPU,$AVG_CPU,$INDEX_SIZE_MB,$SAMPLE_COUNT,$OUTPUT_LINES,$READS_FOUND" >> "$COMBINED_CSV"
                            echo "$(basename "$bam_file"),$flag,$strategy,$DURATION_SEC,$PEAK_MEMORY_GB,$AVG_MEMORY,$PEAK_CPU,$AVG_CPU,$INDEX_SIZE_MB,$OUTPUT_LINES,$READS_FOUND" >> "$TEMP_RESULTS"
                        else
                            echo "$(basename "$bam_file"),$flag,$CSV_THREADS,$strategy,$DURATION,$PEAK_MEMORY,$AVG_MEMORY,$PEAK_CPU,$AVG_CPU,$INDEX_SIZE_MB,$SAMPLE_COUNT,$OUTPUT_LINES,$READS_FOUND" >> "$COMBINED_CSV"
                            echo "$(basename "$bam_file"),$flag,$CSV_THREADS,$strategy,$DURATION_SEC,$PEAK_MEMORY_GB,$AVG_MEMORY,$PEAK_CPU,$AVG_CPU,$INDEX_SIZE_MB,$OUTPUT_LINES,$READS_FOUND" >> "$TEMP_RESULTS"
                        fi
                    
                    echo "   Time: ${DURATION_SEC}s, Peak Memory: ${PEAK_MEMORY_GB}GB, Avg CPU: ${AVG_CPU}%, Reads: $READS_FOUND"
                else
                    echo "   ❌ FAILED"
                fi
                
                                        # Clean up memory file
                        rm -f "$MEMORY_FILE"
                        echo ""
                    done
                    echo ""
                done
                echo ""
            done
            echo ""
        done
        
        # Comprehensive verification and comparison
        echo ""
        echo "COMPREHENSIVE RESULTS SUMMARY:"
        echo "=================================================================="
        
        # Verification across all tools (check consistency across BAM files, flags, and thread counts)
        echo "📋 Output Verification (by BAM file, flag, and thread count):"
        
        ALL_MATCH=true
        for bam_file in "${BAM_ARRAY[@]}"; do
            echo "   BAM: $(basename "$bam_file")"
            for flag in "${FLAG_ARRAY[@]}"; do
                echo "     Flag: $flag"
                for thread_count in "${THREAD_ARRAY[@]}"; do
                    SAMTOOLS_READS=0
                    BAFIQ_READS=0
                    SAMBAMBA_READS=0
                    
                    if [ -f "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" ]; then
                        SAMTOOLS_READS=$(grep -v "^@" "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" 2>/dev/null | wc -l || echo "0")
                    fi
                    if [ -f "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" ]; then
                        BAFIQ_READS=$(grep -v "^@" "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" 2>/dev/null | wc -l || echo "0")
                    fi
                    if [ -f "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" ]; then
                        SAMBAMBA_READS=$(grep -v "^@" "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" 2>/dev/null | wc -l || echo "0")
                    fi
                    
                    echo "       $thread_count threads: samtools=$SAMTOOLS_READS, bafiq=$BAFIQ_READS"
                    if [ "$SAMBAMBA_AVAILABLE" = true ]; then
                        echo "                         sambamba=$SAMBAMBA_READS"
                    fi
                    
                    # Check consistency for this combination
                    if [ "$SAMTOOLS_AVAILABLE" = true ] && [ "$BAFIQ_READS" -ne "$SAMTOOLS_READS" ]; then
                        ALL_MATCH=false
                    fi
                    if [ "$SAMBAMBA_AVAILABLE" = true ] && [ "$SAMBAMBA_READS" -ne "$BAFIQ_READS" ]; then
                        ALL_MATCH=false
                    fi
                done
            done
        done
        
        if [ "$ALL_MATCH" = true ]; then
            echo "✅ All available tools found identical number of reads across all combinations"
        else
            echo "❌ Read counts differ between tools or configurations"
        fi
        
        # Performance ranking and analysis
        echo ""
        echo "🏆 Performance Ranking:"
        echo "----------------------------------------------------------------"
        if [ "$SINGLE_THREAD" = true ]; then
            printf "%-20s %-15s %-10s %-10s %-10s %-10s %-10s\n" "BAM File" "Flag" "Tool" "Time" "Peak RAM" "Avg RAM" "Peak CPU"
            echo "----------------------------------------------------------------"
            
            while IFS=',' read -r bam_file flag strategy time_sec peak_mem_gb avg_mem_mb peak_cpu avg_cpu index_size_mb output_lines reads_found; do
                AVG_MEM_GB=$(echo "scale=1; $avg_mem_mb / 1024" | bc -l 2>/dev/null || echo "0.0")
                printf "%-20s %-15s %-10s %-10s %-10s %-10s %-10s\n" \
                    "$bam_file" "$flag" "$strategy" "${time_sec}s" "${peak_mem_gb}GB" "${AVG_MEM_GB}GB" "${peak_cpu}%"
            done < "$TEMP_RESULTS"
        else
            printf "%-20s %-15s %-10s %-10s %-10s %-10s %-10s %-10s\n" "BAM File" "Flag" "Threads" "Tool" "Time" "Peak RAM" "Avg RAM" "Peak CPU"
            echo "----------------------------------------------------------------"
            
            while IFS=',' read -r bam_file flag threads strategy time_sec peak_mem_gb avg_mem_mb peak_cpu avg_cpu index_size_mb output_lines reads_found; do
                AVG_MEM_GB=$(echo "scale=1; $avg_mem_mb / 1024" | bc -l 2>/dev/null || echo "0.0")
                printf "%-20s %-15s %-10s %-10s %-10s %-10s %-10s %-10s\n" \
                    "$bam_file" "$flag" "$threads" "$strategy" "${time_sec}s" "${peak_mem_gb}GB" "${AVG_MEM_GB}GB" "${peak_cpu}%"
            done < "$TEMP_RESULTS"
        fi
        
        echo "=================================================================="
        
        # Thread Scaling Analysis (skip if only one thread)
        if [ "$SINGLE_THREAD" = false ]; then
            echo ""
            echo "🧵 Thread Scaling Analysis:"
            for thread_count in "${THREAD_ARRAY[@]}"; do
                echo "   $thread_count thread(s):"
                # Use CSV thread value (0 for auto)
                CSV_THREAD_LOOKUP="$thread_count"
                if [ "$thread_count" = "auto" ]; then
                    CSV_THREAD_LOOKUP="0"
                fi
                BEST_FOR_THREADS=$(grep ",$CSV_THREAD_LOOKUP," "$COMBINED_CSV" | sort -t, -k5 -n | head -1)
                if [ -n "$BEST_FOR_THREADS" ]; then
                    BEST_BAM=$(echo "$BEST_FOR_THREADS" | cut -d, -f1)
                    BEST_FLAG=$(echo "$BEST_FOR_THREADS" | cut -d, -f2)
                    BEST_STRATEGY=$(echo "$BEST_FOR_THREADS" | cut -d, -f4)
                    BEST_TIME=$(echo "$BEST_FOR_THREADS" | cut -d, -f5)
                    BEST_TIME_SEC=$(echo "scale=3; $BEST_TIME / 1000" | bc -l 2>/dev/null || echo "0.000")
                    echo "     Best: $BEST_STRATEGY ($BEST_BAM, $BEST_FLAG) - ${BEST_TIME_SEC}s"
                fi
            done
        fi
        
        # Speed comparison analysis
        echo ""
        echo "🚀 Speed Analysis:"
        if [ "$SINGLE_THREAD" = true ]; then
            FASTEST_OVERALL=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k4 -n | head -1)
            if [ -n "$FASTEST_OVERALL" ]; then
                FASTEST_BAM=$(echo "$FASTEST_OVERALL" | cut -d, -f1)
                FASTEST_FLAG=$(echo "$FASTEST_OVERALL" | cut -d, -f2)
                FASTEST_STRATEGY=$(echo "$FASTEST_OVERALL" | cut -d, -f3)
                FASTEST_TIME=$(echo "$FASTEST_OVERALL" | cut -d, -f4)
                FASTEST_TIME_SEC=$(echo "scale=3; $FASTEST_TIME / 1000" | bc -l 2>/dev/null || echo "0.000")
                
                echo "   Fastest overall: $FASTEST_STRATEGY ($FASTEST_BAM, $FASTEST_FLAG) - ${FASTEST_TIME_SEC}s"
                
                # Speed comparison with samtools baseline
                if [ "$SAMTOOLS_AVAILABLE" = true ]; then
                    SAMTOOLS_TIME=$(tail -n +2 "$COMBINED_CSV" | grep ",samtools-view," | awk -F',' '{print $4}')
                    if [ -n "$SAMTOOLS_TIME" ] && [ "$SAMTOOLS_TIME" -ne "$FASTEST_TIME" ]; then
                        SPEEDUP=$(echo "scale=1; $SAMTOOLS_TIME / $FASTEST_TIME" | bc -l 2>/dev/null || echo "1.0")
                        echo "   Speedup vs samtools: ${SPEEDUP}x faster"
                    fi
                fi
            fi
        else
            FASTEST_OVERALL=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k5 -n | head -1)
            if [ -n "$FASTEST_OVERALL" ]; then
                FASTEST_BAM=$(echo "$FASTEST_OVERALL" | cut -d, -f1)
                FASTEST_FLAG=$(echo "$FASTEST_OVERALL" | cut -d, -f2)
                FASTEST_THREADS_CSV=$(echo "$FASTEST_OVERALL" | cut -d, -f3)
                FASTEST_STRATEGY=$(echo "$FASTEST_OVERALL" | cut -d, -f4)
                FASTEST_TIME=$(echo "$FASTEST_OVERALL" | cut -d, -f5)
                FASTEST_TIME_SEC=$(echo "scale=3; $FASTEST_TIME / 1000" | bc -l 2>/dev/null || echo "0.000")
                
                # Convert 0 back to "auto" for display
                FASTEST_THREADS_DISPLAY="$FASTEST_THREADS_CSV"
                if [ "$FASTEST_THREADS_CSV" = "0" ]; then
                    FASTEST_THREADS_DISPLAY="auto"
                fi
                
                echo "   Fastest overall: $FASTEST_STRATEGY ($FASTEST_BAM, $FASTEST_FLAG, ${FASTEST_THREADS_DISPLAY} threads) - ${FASTEST_TIME_SEC}s"
                
                # Speed comparison with samtools baseline
                if [ "$SAMTOOLS_AVAILABLE" = true ]; then
                    SAMTOOLS_TIME=$(tail -n +2 "$COMBINED_CSV" | grep ",samtools-view," | awk -F',' '{print $5}')
                    if [ -n "$SAMTOOLS_TIME" ] && [ "$SAMTOOLS_TIME" -ne "$FASTEST_TIME" ]; then
                        SPEEDUP=$(echo "scale=1; $SAMTOOLS_TIME / $FASTEST_TIME" | bc -l 2>/dev/null || echo "1.0")
                        echo "   Speedup vs samtools: ${SPEEDUP}x faster"
                    fi
                fi
            fi
        fi
        
        # Memory efficiency analysis
        echo ""
        echo "💾 Memory Analysis:"
        if [ "$SINGLE_THREAD" = true ]; then
            MOST_EFFICIENT=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k5 -n | head -1)
            if [ -n "$MOST_EFFICIENT" ]; then
                EFFICIENT_BAM=$(echo "$MOST_EFFICIENT" | cut -d, -f1)
                EFFICIENT_FLAG=$(echo "$MOST_EFFICIENT" | cut -d, -f2)
                EFFICIENT_STRATEGY=$(echo "$MOST_EFFICIENT" | cut -d, -f3)
                EFFICIENT_MEM=$(echo "$MOST_EFFICIENT" | cut -d, -f5)
                EFFICIENT_MEM_GB=$(echo "scale=1; $EFFICIENT_MEM / 1024" | bc -l 2>/dev/null || echo "0.0")
                echo "   Most memory efficient: $EFFICIENT_STRATEGY ($EFFICIENT_BAM, $EFFICIENT_FLAG) - ${EFFICIENT_MEM_GB}GB peak"
            fi
        else
            MOST_EFFICIENT=$(tail -n +2 "$COMBINED_CSV" | sort -t, -k6 -n | head -1)
            if [ -n "$MOST_EFFICIENT" ]; then
                EFFICIENT_BAM=$(echo "$MOST_EFFICIENT" | cut -d, -f1)
                EFFICIENT_FLAG=$(echo "$MOST_EFFICIENT" | cut -d, -f2)
                EFFICIENT_STRATEGY=$(echo "$MOST_EFFICIENT" | cut -d, -f4)
                EFFICIENT_MEM=$(echo "$MOST_EFFICIENT" | cut -d, -f6)
                EFFICIENT_MEM_GB=$(echo "scale=1; $EFFICIENT_MEM / 1024" | bc -l 2>/dev/null || echo "0.0")
                echo "   Most memory efficient: $EFFICIENT_STRATEGY ($EFFICIENT_BAM, $EFFICIENT_FLAG) - ${EFFICIENT_MEM_GB}GB peak"
            fi
        fi
        
        # Content comparison across tools (check first combinations as reference)
        echo ""
        echo "🔍 Content Verification:"
        CONTENT_MATCH=true
        FIRST_THREAD="${THREAD_ARRAY[0]}"
        FIRST_BAM="${BAM_ARRAY[0]}"
        FIRST_FLAG="${FLAG_ARRAY[0]}"
        
        if [ -f "$TEMP_DIR/out.samtools.${FIRST_THREAD}t.${FIRST_FLAG}.sam" ] && [ -f "$TEMP_DIR/out.bafiq.${FIRST_THREAD}t.${FIRST_FLAG}.sam" ]; then
            if ! head -20 "$TEMP_DIR/out.samtools.${FIRST_THREAD}t.${FIRST_FLAG}.sam" | diff - <(head -20 "$TEMP_DIR/out.bafiq.${FIRST_THREAD}t.${FIRST_FLAG}.sam") > /dev/null 2>&1; then
                CONTENT_MATCH=false
            fi
        fi
        if [ -f "$TEMP_DIR/out.sambamba.${FIRST_THREAD}t.${FIRST_FLAG}.sam" ] && [ -f "$TEMP_DIR/out.bafiq.${FIRST_THREAD}t.${FIRST_FLAG}.sam" ]; then
            if ! head -20 "$TEMP_DIR/out.sambamba.${FIRST_THREAD}t.${FIRST_FLAG}.sam" | diff - <(head -20 "$TEMP_DIR/out.bafiq.${FIRST_THREAD}t.${FIRST_FLAG}.sam") > /dev/null 2>&1; then
                CONTENT_MATCH=false
            fi
        fi
        
        if [ "$CONTENT_MATCH" = true ]; then
            echo "✅ Content samples match across all available tools (checked $(basename "$FIRST_BAM"), $FIRST_FLAG, $FIRST_THREAD threads)"
        else
            echo "❌ Content differs between available tools (checked $(basename "$FIRST_BAM"), $FIRST_FLAG, $FIRST_THREAD threads)"
            echo "   Manual inspection recommended"
        fi
        
        echo ""
        echo "VIEW BENCHMARK RESULTS SAVED:"
        echo "   Performance CSV: $COMBINED_CSV"
        echo "   Memory samples CSV: $MEMORY_CSV"
        echo "📁 Output files saved to: $TEMP_DIR"
        for bam_file in "${BAM_ARRAY[@]}"; do
            echo "   BAM: $(basename "$bam_file")"
            for flag in "${FLAG_ARRAY[@]}"; do
                echo "     Flag: $flag"
                for thread_count in "${THREAD_ARRAY[@]}"; do
                    if [ "$SINGLE_THREAD" = true ]; then
                        if [ -f "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" ]; then
                            echo "       samtools: out.samtools.${thread_count}t.${flag}.sam"
                        fi
                        if [ -f "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" ]; then
                            echo "       bafiq: out.bafiq.${thread_count}t.${flag}.sam"
                        fi
                        if [ -f "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" ]; then
                            echo "       sambamba: out.sambamba.${thread_count}t.${flag}.sam"
                        fi
                    else
                        echo "       $thread_count threads:"
                        if [ -f "$TEMP_DIR/out.samtools.${thread_count}t.${flag}.sam" ]; then
                            echo "         samtools: out.samtools.${thread_count}t.${flag}.sam"
                        fi
                        if [ -f "$TEMP_DIR/out.bafiq.${thread_count}t.${flag}.sam" ]; then
                            echo "         bafiq: out.bafiq.${thread_count}t.${flag}.sam"
                        fi
                        if [ -f "$TEMP_DIR/out.sambamba.${thread_count}t.${flag}.sam" ]; then
                            echo "         sambamba: out.sambamba.${thread_count}t.${flag}.sam"
                        fi
                    fi
                done
            done
        done
        
        # Clean up temporary files
        rm -f "$TEMP_RESULTS"
        
        # Keep temp directory for manual inspection
        trap - EXIT
        echo "   (Directory preserved for manual inspection)"
        echo ""
        echo "💡 Use CSV files with plotting tools (same format as 'just bench')"
    fi


# Show available commands
help:
    @just --list 

 