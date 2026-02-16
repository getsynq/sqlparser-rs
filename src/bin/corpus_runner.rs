use rayon::prelude::*;
use sqlparser::dialect::dialect_from_str;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use walkdir::WalkDir;

const DEFAULT_CORPUS_ROOT: &str = "tests/corpus";
const REPORT_PATH: &str = "target/corpus-report.json";

/// Extract the dialect name from a directory name.
/// Uses the part after the last `_` as the dialect (e.g., `sqlglot_bigquery` -> `bigquery`,
/// `customer_bigquery` -> `bigquery`). If no `_` exists, uses the whole name.
fn normalize_dialect_name(name: &str) -> &str {
    name.rsplit_once('_').map(|(_, suffix)| suffix).unwrap_or(name)
}

fn dialect_for_name(name: &str) -> Option<Box<dyn sqlparser::dialect::Dialect>> {
    let base_name = normalize_dialect_name(name);
    dialect_from_str(base_name)
}

fn dialect_from_path(path: &Path, corpus_root: &Path) -> Option<String> {
    let relative = path.strip_prefix(corpus_root).ok()?;
    relative
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .map(|s| s.to_string())
}

/// Extract expected statement count from `-- Statements: N` header comment.
fn expected_statement_count(sql: &str) -> Option<usize> {
    for line in sql.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("-- Statements:") {
            return rest.trim().parse().ok();
        }
        // Stop looking after non-comment, non-empty lines
        if !trimmed.is_empty() && !trimmed.starts_with("--") {
            break;
        }
    }
    None
}

fn run_test(path: &Path, corpus_root: &Path) -> Result<(), String> {
    let dialect_name = dialect_from_path(path, corpus_root)
        .ok_or_else(|| format!("Could not determine dialect from path: {}", path.display()))?;

    let dialect = dialect_for_name(&dialect_name)
        .ok_or_else(|| format!("Unknown dialect: {}", dialect_name))?;

    let sql = std::fs::read_to_string(path).map_err(|e| format!("Failed to read file: {e}"))?;

    match Parser::parse_sql(&*dialect, &sql) {
        Ok(statements) => {
            if statements.is_empty() {
                return Err(format!("Parsed 0 statements from {}", path.display()));
            }

            if let Some(expected) = expected_statement_count(&sql) {
                if expected != statements.len() {
                    return Err(format!(
                        "Statement count mismatch: expected {expected}, got {}",
                        statements.len()
                    ));
                }
            }

            Ok(())
        }
        Err(e) => Err(format!("Failed to parse: {e}")),
    }
}

fn collect_sql_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if !dir.is_dir() {
        return files;
    }
    for entry in WalkDir::new(dir).sort_by_file_name() {
        let Ok(entry) = entry else { continue };
        if entry.file_type().is_file() && entry.path().extension().is_some_and(|ext| ext == "sql") {
            files.push(entry.into_path());
        }
    }
    files
}

/// Per-dialect pass/fail counts: [passed, failed]
type Stats = BTreeMap<String, [usize; 2]>;

/// Individual test results by test path
/// Value is "pass" or an error message (when --errors flag is used) or "fail"
type TestResults = BTreeMap<String, String>;

/// Escape a string for JSON output
fn json_escape(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c < '\x20' => escaped.push_str(&format!("\\u{:04x}", c as u32)),
            c => escaped.push(c),
        }
    }
    escaped
}

fn write_report(stats: &Stats, test_results: &TestResults) {
    // Ensure target/ directory exists
    let _ = std::fs::create_dir_all("target");

    let mut total_passed = 0usize;
    let mut total_failed = 0usize;
    for [passed, failed] in stats.values() {
        total_passed += passed;
        total_failed += failed;
    }

    // Build JSON manually to avoid needing serde
    let mut json = String::from("{\n");

    // Summary section
    json.push_str("  \"summary\": {\n");
    json.push_str(&format!("    \"total_passed\": {total_passed},\n"));
    json.push_str(&format!("    \"total_failed\": {total_failed},\n"));
    json.push_str(&format!(
        "    \"total_tests\": {}\n",
        total_passed + total_failed
    ));
    json.push_str("  },\n");

    // Per-dialect stats
    json.push_str("  \"by_dialect\": {\n");
    let dialect_count = stats.len();
    for (i, (dialect, [passed, failed])) in stats.iter().enumerate() {
        json.push_str(&format!(
            "    \"{dialect}\": {{\"passed\": {passed}, \"failed\": {failed}}}{}",
            if i + 1 < dialect_count { ",\n" } else { "\n" }
        ));
    }
    json.push_str("  },\n");

    // Individual test results (relative paths as keys)
    json.push_str("  \"test_results\": {\n");
    let test_count = test_results.len();
    for (i, (path, status)) in test_results.iter().enumerate() {
        let escaped_path = json_escape(path);
        let escaped_status = json_escape(status);
        json.push_str(&format!(
            "    \"{escaped_path}\": \"{escaped_status}\"{}",
            if i + 1 < test_count { ",\n" } else { "\n" }
        ));
    }
    json.push_str("  }\n}\n");

    if let Err(e) = std::fs::write(REPORT_PATH, &json) {
        eprintln!("Error: failed to write report to {REPORT_PATH}: {e}");
        std::process::exit(1);
    } else {
        eprintln!("✓ Corpus report written to {REPORT_PATH}");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // Parse flags
    let include_errors = args.iter().any(|a| a == "--errors");

    // Get corpus root (first non-flag argument, or default)
    let corpus_root = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with("--"))
        .map(|s| s.as_str())
        .unwrap_or(DEFAULT_CORPUS_ROOT);

    let corpus_path = Path::new(corpus_root);
    if !corpus_path.exists() {
        eprintln!(
            "Error: Corpus directory not found at {}",
            corpus_path.display()
        );
        std::process::exit(1);
    }

    eprintln!("Scanning corpus directory: {}", corpus_path.display());
    if include_errors {
        eprintln!("Including error messages in report (--errors)");
    }

    let sql_files = collect_sql_files(corpus_path);
    if sql_files.is_empty() {
        eprintln!("Error: No .sql files found in {}", corpus_path.display());
        std::process::exit(1);
    }

    eprintln!("Found {} SQL files", sql_files.len());

    let stats: Arc<Mutex<Stats>> = Arc::new(Mutex::new(BTreeMap::new()));
    let test_results: Arc<Mutex<TestResults>> = Arc::new(Mutex::new(BTreeMap::new()));

    let start_time = std::time::Instant::now();
    let processed = Arc::new(AtomicUsize::new(0));
    let total = sql_files.len();
    let report_every = 1000;

    // Process files in parallel
    sql_files.par_iter().for_each(|path| {
        // Get relative path for reporting
        let relative_path = path
            .strip_prefix(corpus_path)
            .unwrap_or(path)
            .display()
            .to_string();

        // Get dialect name from path
        let dialect_dir_name = match dialect_from_path(path, corpus_path) {
            Some(d) => d,
            None => {
                eprintln!(
                    "Warning: Could not determine dialect for {}, skipping",
                    path.display()
                );
                return;
            }
        };

        // Normalize dialect name (extract base dialect from directory name)
        let normalized_dialect = normalize_dialect_name(&dialect_dir_name);

        // Skip if dialect is not supported
        if dialect_for_name(normalized_dialect).is_none() {
            // Skip silently - unknown dialects are expected (e.g., trino)
            return;
        }

        // Run test with panic catching
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_test(path, corpus_path)));

        // Update stats (with lock)
        {
            let mut stats_guard = stats.lock().unwrap();
            let mut test_results_guard = test_results.lock().unwrap();

            // Use normalized dialect name for stats (groups customer_* with base dialect)
            let entry = stats_guard
                .entry(normalized_dialect.to_string())
                .or_insert([0, 0]);
            let status = match result {
                Ok(Ok(())) => {
                    entry[0] += 1;
                    "pass".to_string()
                }
                Ok(Err(e)) => {
                    entry[1] += 1;
                    if include_errors {
                        // Take first line of error, truncate to 200 chars
                        let first_line = e.lines().next().unwrap_or(&e);
                        if first_line.len() > 200 {
                            format!("fail: {}...", &first_line[..200])
                        } else {
                            format!("fail: {}", first_line)
                        }
                    } else {
                        "fail".to_string()
                    }
                }
                Err(panic) => {
                    entry[1] += 1;
                    if include_errors {
                        let msg = if let Some(s) = panic.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = panic.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "unknown panic".to_string()
                        };
                        format!("panic: {}", msg)
                    } else {
                        "fail".to_string()
                    }
                }
            };

            test_results_guard.insert(relative_path, status);
        }

        // Update progress counter
        let current = processed.fetch_add(1, Ordering::Relaxed) + 1;

        // Report progress periodically (avoid excessive locking)
        if current % report_every == 0 || current == total {
            let elapsed = start_time.elapsed();
            let rate = current as f64 / elapsed.as_secs_f64();
            let eta_secs = ((total - current) as f64 / rate) as u64;
            eprintln!(
                "Progress: {}/{} ({:.1}%) - {:.0} files/sec - ETA: {}s",
                current,
                total,
                (current as f64 / total as f64) * 100.0,
                rate,
                eta_secs
            );

            // Write incremental report (with lock)
            let stats_guard = stats.lock().unwrap();
            let test_results_guard = test_results.lock().unwrap();
            write_report(&stats_guard, &test_results_guard);
        }
    });

    // Write final report
    let stats_guard = stats.lock().unwrap();
    let test_results_guard = test_results.lock().unwrap();
    write_report(&stats_guard, &test_results_guard);

    let elapsed = start_time.elapsed();
    let total_passed = stats_guard.values().map(|[p, _]| p).sum::<usize>();
    let total_failed = stats_guard.values().map(|[_, f]| f).sum::<usize>();

    eprintln!("\n=== Summary ===");
    eprintln!("Total files:  {}", total);
    eprintln!(
        "Passed:       {} ({:.1}%)",
        total_passed,
        (total_passed as f64 / total as f64) * 100.0
    );
    eprintln!(
        "Failed:       {} ({:.1}%)",
        total_failed,
        (total_failed as f64 / total as f64) * 100.0
    );
    eprintln!("Time:         {:.1}s", elapsed.as_secs_f64());
    eprintln!(
        "Rate:         {:.0} files/sec",
        total as f64 / elapsed.as_secs_f64()
    );

    eprintln!("\n=== By Dialect ===");
    for (dialect, [passed, failed]) in stats_guard.iter() {
        let total_d = passed + failed;
        eprintln!(
            "{:12} {:6} passed, {:6} failed ({:.1}% pass rate)",
            dialect,
            passed,
            failed,
            (*passed as f64 / total_d as f64) * 100.0
        );
    }

    // Exit with error code if any tests failed
    if total_failed > 0 {
        std::process::exit(1);
    }
}
