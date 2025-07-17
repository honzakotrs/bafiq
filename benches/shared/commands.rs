use anyhow::Result;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use super::analysis::ResourceSample;

// Helper function to create bafiq command with thread configuration
pub fn create_bafiq_command(thread_count: usize) -> Command {
    let mut cmd = Command::new("./target/release/bafiq");
    if thread_count > 0 {
        cmd.args(&["--threads", &thread_count.to_string()]);
    }
    cmd
}

// Helper function to format thread count for display
pub fn format_thread_display(thread_count: usize) -> String {
    match thread_count {
        0 => "auto".to_string(),
        n => n.to_string(),
    }
}

// Helper function to format thread count with suffix
pub fn format_thread_with_suffix(thread_count: usize) -> String {
    match thread_count {
        0 => "auto".to_string(),
        n => format!("{}t", n),
    }
}

/// Process monitoring helper functions
pub fn is_process_running(pid: u32) -> bool {
    // Cross-platform process check using ps command
    std::process::Command::new("ps")
        .args(&["-p", &pid.to_string()])
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Get memory and CPU stats for a process
pub fn get_process_stats(pid: u32) -> Result<(f64, f64)> {
    #[cfg(target_os = "macos")]
    {
        // macOS: Use ps to get RSS (memory) and %CPU
        let output = Command::new("ps")
            .args(&["-o", "rss=", "-o", "%cpu=", "-p", &pid.to_string()])
            .output()?;

        if output.status.success() {
            let output_str = String::from_utf8_lossy(&output.stdout);
            let mut parts = output_str.trim().split_whitespace();

            let memory_kb = parts
                .next()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let cpu_percent = parts
                .next()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            let memory_mb = memory_kb / 1024.0;
            Ok((memory_mb, cpu_percent))
        } else {
            Ok((0.0, 0.0))
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Linux: Use ps to get RSS (memory) and %CPU
        let output = Command::new("ps")
            .args(&["-o", "rss=", "-o", "%cpu=", "-p", &pid.to_string()])
            .output()?;

        if output.status.success() {
            let output_str = String::from_utf8_lossy(&output.stdout);
            let mut parts = output_str.trim().split_whitespace();

            let memory_kb = parts
                .next()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let cpu_percent = parts
                .next()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            let memory_mb = memory_kb / 1024.0;
            Ok((memory_mb, cpu_percent))
        } else {
            Ok((0.0, 0.0))
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        // Fallback for other platforms
        Ok((0.0, 0.0))
    }
}

/// Core monitoring function that runs command with process monitoring
/// Spawns process, monitors memory/CPU every 100ms, returns comprehensive metrics
pub fn run_with_monitoring(
    mut cmd: Command,
) -> Result<(
    Duration,
    u64,
    (f64, f64),          // (peak_memory, avg_memory)
    (f64, f64),          // (peak_cpu, avg_cpu)
    usize,               // sample_count
    Vec<ResourceSample>, // memory_samples
)> {
    // Set up command to capture output
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let start_time = Instant::now();
    let child = cmd.spawn()?;
    let pid = child.id();

    // Monitor process every 100ms at synchronized intervals (0ms, 100ms, 200ms, etc.)
    let monitor_handle = thread::spawn(move || {
        let mut samples = Vec::new();
        let mut sample_time_ms = 0u64;

        while is_process_running(pid) {
            if let Ok((memory_mb, cpu_percent)) = get_process_stats(pid) {
                samples.push(ResourceSample {
                    timestamp_ms: sample_time_ms,
                    memory_mb,
                    cpu_percent,
                });
            }

            thread::sleep(Duration::from_millis(100));
            sample_time_ms += 100; // Synchronized intervals: 0, 100, 200, 300ms...
        }
        samples
    });

    // Wait for process to complete
    let output = child.wait_with_output()?;
    let duration = start_time.elapsed();

    // Get monitoring results
    let samples = monitor_handle.join().unwrap_or_default();
    let sample_count = samples.len();

    // Calculate memory and CPU statistics
    let (peak_memory, avg_memory) = if samples.is_empty() {
        (0.0, 0.0)
    } else {
        let peak = samples.iter().map(|s| s.memory_mb).fold(0.0, f64::max);
        let avg = samples.iter().map(|s| s.memory_mb).sum::<f64>() / samples.len() as f64;
        (peak, avg)
    };

    let (peak_cpu, avg_cpu) = if samples.is_empty() {
        (0.0, 0.0)
    } else {
        let peak = samples.iter().map(|s| s.cpu_percent).fold(0.0, f64::max);
        let avg = samples.iter().map(|s| s.cpu_percent).sum::<f64>() / samples.len() as f64;
        (peak, avg)
    };

    // Extract record count from output
    let record_count = if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        extract_record_count_from_output(&stdout)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow::anyhow!("Process failed: {}", stderr));
    };

    Ok((
        duration,
        record_count,
        (peak_memory, avg_memory),
        (peak_cpu, avg_cpu),
        sample_count,
        samples,
    ))
}

/// Extract record count from tool output - generic implementation
pub fn extract_record_count_from_output(output: &str) -> u64 {
    // Try multiple patterns to extract record counts

    // Pattern 1: "Matching reads: 10732930" (bafiq query)
    if let Some(line) = output.lines().find(|line| line.contains("Matching reads:")) {
        if let Some(count_str) = line.split("Matching reads:").nth(1) {
            if let Ok(count) = count_str.trim().parse() {
                return count;
            }
        }
    }

    // Pattern 2: Plain numeric output (samtools view -c, bafiq fast-count)
    if let Ok(count) = output.trim().parse() {
        return count;
    }

    // Pattern 3: Last numeric line (bafiq fast-count fallback)
    if let Some(count) = output
        .trim()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .last()
    {
        return count;
    }

    0 // Default if no count found
}

/// Clear system cache to ensure fair benchmarking
pub fn clear_system_cache(clear_cache: bool) -> Result<()> {
    if !clear_cache {
        return Ok(());
    }

    println!("   Clearing system cache...");

    #[cfg(target_os = "macos")]
    {
        let output = Command::new("sudo").arg("purge").output();

        match output {
            Ok(result) if result.status.success() => {
                println!("   Cache cleared successfully (macOS)");
            }
            Ok(result) => {
                eprintln!(
                    "   Warning: Cache clear failed: {}",
                    String::from_utf8_lossy(&result.stderr)
                );
                eprintln!("      Continuing without cache clearing...");
            }
            Err(e) => {
                eprintln!("   Warning: Cache clear error: {}", e);
                eprintln!("      Continuing without cache clearing...");
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        // Try multiple cache clearing methods
        let methods = [
            ("echo 3 | sudo tee /proc/sys/vm/drop_caches", "drop_caches"),
            ("sudo sync", "sync"),
        ];

        for (cmd_str, name) in &methods {
            let parts: Vec<&str> = cmd_str.split_whitespace().collect();
            if parts.len() >= 2 {
                let output = Command::new(parts[0]).args(&parts[1..]).output();

                match output {
                    Ok(result) if result.status.success() => {
                        println!("   Cache cleared with {} (Linux)", name);
                        break;
                    }
                    Ok(result) => {
                        eprintln!(
                            "   Warning: Cache clear ({}) failed: {}",
                            name,
                            String::from_utf8_lossy(&result.stderr)
                        );
                    }
                    Err(e) => {
                        eprintln!("   Warning: Cache clear ({}) error: {}", name, e);
                    }
                }
            }
        }
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        eprintln!("   Warning: Cache clearing not supported on this platform");
        eprintln!("      Results may be affected by file system caching");
    }

    // Brief pause to let cache clearing take effect
    thread::sleep(Duration::from_millis(100));

    Ok(())
}
