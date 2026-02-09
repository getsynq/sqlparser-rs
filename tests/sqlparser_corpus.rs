use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use libtest_mimic::Failed;
use sqlparser::dialect::*;
use sqlparser::parser::Parser;

const CORPUS_ROOT: &str = "tests/corpus";
const REPORT_PATH: &str = "target/corpus-report.json";

fn dialect_for_name(name: &str) -> Box<dyn Dialect> {
    match name {
        "bigquery" => Box::new(BigQueryDialect),
        "clickhouse" => Box::new(ClickHouseDialect {}),
        "snowflake" => Box::new(SnowflakeDialect),
        "redshift" => Box::new(RedshiftSqlDialect {}),
        "databricks" => Box::new(DatabricksDialect {}),
        "duckdb" => Box::new(DuckDbDialect {}),
        "mysql" => Box::new(MySqlDialect {}),
        "mssql" => Box::new(MsSqlDialect {}),
        "postgres" | "postgresql" => Box::new(PostgreSqlDialect {}),
        "hive" => Box::new(HiveDialect {}),
        "sqlite" => Box::new(SQLiteDialect {}),
        "generic" => Box::new(GenericDialect {}),
        other => panic!("Unknown dialect: {other}"),
    }
}

fn dialect_from_path(path: &Path) -> &str {
    let relative = path.strip_prefix(CORPUS_ROOT).unwrap();
    relative
        .components()
        .next()
        .unwrap()
        .as_os_str()
        .to_str()
        .unwrap()
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

fn fail(msg: String) -> Failed {
    Failed::from(msg)
}

fn run_test(path: &Path) -> Result<(), Failed> {
    let dialect_name = dialect_from_path(path);
    let dialect = dialect_for_name(dialect_name);

    let sql =
        std::fs::read_to_string(path).map_err(|e| fail(format!("Failed to read file: {e}")))?;

    match Parser::parse_sql(&*dialect, &sql) {
        Ok(statements) => {
            if statements.is_empty() {
                return Err(fail(format!("Parsed 0 statements from {}", path.display())));
            }

            if let Some(expected) = expected_statement_count(&sql) {
                if expected != statements.len() {
                    return Err(fail(format!(
                        "Statement count mismatch: expected {expected}, got {}",
                        statements.len()
                    )));
                }
            }

            Ok(())
        }
        Err(e) => Err(fail(format!("Failed to parse: {e}"))),
    }
}

fn collect_sql_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if !dir.is_dir() {
        return files;
    }
    let walker = walkdir::WalkDir::new(dir).sort_by_file_name();
    for entry in walker {
        let Ok(entry) = entry else { continue };
        if entry.file_type().is_file() && entry.path().extension().is_some_and(|ext| ext == "sql") {
            files.push(entry.into_path());
        }
    }
    files
}

/// Per-dialect pass/fail counts.
type Stats = BTreeMap<String, [usize; 2]>;

fn write_report(stats: &Stats) {
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
    json.push_str(&format!("  \"total_passed\": {total_passed},\n"));
    json.push_str(&format!("  \"total_failed\": {total_failed},\n"));
    json.push_str("  \"dialects\": {\n");
    let dialect_count = stats.len();
    for (i, (dialect, [passed, failed])) in stats.iter().enumerate() {
        json.push_str(&format!(
            "    \"{dialect}\": {{\"passed\": {passed}, \"failed\": {failed}}}{}",
            if i + 1 < dialect_count { ",\n" } else { "\n" }
        ));
    }
    json.push_str("  }\n}\n");

    if let Err(e) = std::fs::write(REPORT_PATH, &json) {
        eprintln!("Warning: failed to write report to {REPORT_PATH}: {e}");
    } else {
        eprintln!("Corpus report written to {REPORT_PATH}");
    }
}

fn main() {
    let corpus_path = Path::new(CORPUS_ROOT);
    if !corpus_path.exists() {
        eprintln!("Corpus directory not found at {CORPUS_ROOT}, skipping corpus tests");
        return;
    }

    let sql_files = collect_sql_files(corpus_path);
    if sql_files.is_empty() {
        eprintln!("No .sql files found in {CORPUS_ROOT}, skipping corpus tests");
        return;
    }

    let stats: Arc<Mutex<Stats>> = Arc::new(Mutex::new(BTreeMap::new()));

    let tests: Vec<libtest_mimic::Trial> = sql_files
        .into_iter()
        .map(|path| {
            let name = path
                .strip_prefix(CORPUS_ROOT)
                .unwrap_or(&path)
                .display()
                .to_string();
            let test_path = path.clone();
            let stats = stats.clone();
            let dialect = dialect_from_path(&path).to_string();

            libtest_mimic::Trial::test(name, move || {
                let result = run_test(&test_path);
                let mut s = stats.lock().unwrap();
                let entry = s.entry(dialect).or_insert([0, 0]);
                if result.is_ok() {
                    entry[0] += 1;
                } else {
                    entry[1] += 1;
                }
                result
            })
        })
        .collect();

    let args = libtest_mimic::Arguments::from_args();
    let conclusion = libtest_mimic::run(&args, tests);

    // Write stats report before exiting
    let stats = stats.lock().unwrap();
    if !stats.is_empty() {
        write_report(&stats);
    }

    conclusion.exit();
}
