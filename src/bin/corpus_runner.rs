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
    name.rsplit_once('_')
        .map(|(_, suffix)| suffix)
        .unwrap_or(name)
}

/// Map a corpus dialect-dir name to the closest supported parser dialect.
///
/// Dialects without a dedicated parser fall back to a related dialect or to
/// `GenericDialect` rather than being silently skipped, so corpus stats reflect
/// every file under `tests/corpus/`. Aliases are best-effort:
///   - `presto` / `athena` use Trino-style SQL → Generic (same as our `trino`)
///   - `tsql` / `fabric` use T-SQL → MsSql
///   - `spark` uses Spark SQL → Databricks
///   - `materialize` is Postgres-compatible → Postgres
///   - `singlestore` / `doris` / `starrocks` are MySQL-compatible → MySql
///   - `oracle` / `teradata` / `dremio` / `exasol` have no dedicated parser → Generic
fn dialect_for_name(name: &str) -> Box<dyn sqlparser::dialect::Dialect> {
    let base_name = normalize_dialect_name(name).to_lowercase();
    if let Some(d) = dialect_from_str(&base_name) {
        return d;
    }
    let alias: &str = match base_name.as_str() {
        "presto" | "athena" => "trino",
        "tsql" | "fabric" => "mssql",
        "spark" => "databricks",
        "materialize" => "postgres",
        "singlestore" | "doris" | "starrocks" => "mysql",
        // Everything else (oracle, teradata, dremio, exasol, …) → generic
        _ => "generic",
    };
    dialect_from_str(alias).unwrap_or_else(|| {
        Box::new(sqlparser::dialect::GenericDialect)
            as Box<dyn sqlparser::dialect::Dialect>
    })
}

fn dialect_from_path(path: &Path, corpus_root: &Path) -> Option<String> {
    let relative = path.strip_prefix(corpus_root).ok()?;
    relative
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .map(|s| s.to_string())
}

fn run_test(path: &Path, corpus_root: &Path) -> Result<(), String> {
    let dialect_name = dialect_from_path(path, corpus_root)
        .ok_or_else(|| format!("Could not determine dialect from path: {}", path.display()))?;

    let dialect = dialect_for_name(&dialect_name);

    let sql = std::fs::read_to_string(path).map_err(|e| format!("Failed to read file: {e}"))?;

    // Normalize log-escaped SQL: some corpus files come from query logs where actual
    // newlines/CRs were escaped as literal \n / \r sequences. Unescape them before parsing
    // so the parser sees proper whitespace.
    let sql = if sql.contains("\\n") || sql.contains("\\r") {
        sql.replace("\\r\\n", "\n")
            .replace("\\r", "\r")
            .replace("\\n", "\n")
    } else {
        sql
    };

    // Customer SQL routinely contains deep COALESCE/TRIM/REPLACE chains
    // (100+ levels) that blow past the default 61-deep recursion guard.
    // Bump the limit for the corpus runner; the 8 MB RUST_MIN_STACK keeps
    // the actual stack from blowing.
    const CORPUS_RECURSION_LIMIT: usize = 256;
    let parse_result = Parser::new(&*dialect)
        .with_recursion_limit(CORPUS_RECURSION_LIMIT)
        .try_with_sql(&sql)
        .and_then(|mut p| p.parse_statements());
    match parse_result {
        Ok(statements) => {
            if statements.is_empty() {
                return Err(format!("Parsed 0 statements from {}", path.display()));
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
/// Value is "pass", "fail: <error message>", or "panic: <message>"
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
        // The bare `customer/` dir has no dialect suffix — bucket it under
        // `generic` so the report doesn't list a phantom "customer" dialect.
        let normalized_dialect = if dialect_dir_name == "customer" {
            "generic"
        } else {
            normalize_dialect_name(&dialect_dir_name)
        };
        // No silent skip: dialects without a dedicated parser fall back to a
        // related dialect or to GenericDialect inside `dialect_for_name`.

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
                    // Take first line of error, truncate to 200 chars
                    let first_line = e.lines().next().unwrap_or(&e);
                    if first_line.len() > 200 {
                        format!("fail: {}...", &first_line[..200])
                    } else {
                        format!("fail: {}", first_line)
                    }
                }
                Err(panic) => {
                    entry[1] += 1;
                    let msg = if let Some(s) = panic.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic.downcast_ref::<&str>() {
                        s.to_string()
                    } else {
                        "unknown panic".to_string()
                    };
                    format!("panic: {}", msg)
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
}
