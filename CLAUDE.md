# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# sqlparser-rs - SYNQ Fork

## Overview
This is a **fork of [apache/datafusion-sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs)**, an extensible SQL lexer and parser for Rust supporting ANSI SQL:2011 and multiple SQL dialects. This fork contains SYNQ-specific extensions for parsing SQL dialects used in the kernel-cll column-level lineage parser.

## Building and Testing

### Core Commands
```bash
# Build the library
cargo build

# Check code compiles without building
cargo check

# Run all tests (preferred method)
cargo nextest run --all-features

# Run tests for a specific dialect
cargo nextest run --test sqlparser_snowflake
cargo nextest run --test sqlparser_postgres
cargo nextest run --test sqlparser_bigquery

# Run specific test by name
cargo nextest run -E 'test(test_name_pattern)'

# Format code (uses default rustfmt settings)
cargo fmt

# Run linter
cargo clippy
```

### Reproducing a parse failure on one SQL file
- Use `cargo run --release --quiet --features json_example --example cli FILE --DIALECT 2>&1 1>/dev/null | grep "Error during parsing"` — DEBUG logs flood stderr by default and obscure the actual parse error.
- The release CLI (`target/release/examples/cli`) is rebuilt independently from `corpus-runner`. After parser edits, run `cargo build --release --example cli` before re-running single-file repros — otherwise you're testing the previous build and may report false positives.

### Backgrounding gotcha

- **`cmd > log 2>&1 &` in a Bash tool call with `run_in_background: true` returns "completed" immediately** because the shell exits while the `&`-detached child keeps running. Don't trust the completion notification for detached processes — verify with `pgrep -f <name>` or arm a Monitor that polls `pgrep`.

### Performance and Profiling

**Critical:** Always profile BEFORE optimizing. Assumptions about bottlenecks are often wrong.

```bash
# Profile with corpus-runner on macOS (reliable)
target/release/corpus-runner tests/corpus &
PID=$!
sample $PID 120 -file /tmp/profile.txt
kill $PID
grep "Sort by top of stack" -A 30 /tmp/profile.txt

# Build corpus-runner for testing/profiling
cargo build --release --bin corpus-runner
```

**Profiling lessons:**
- Test framework overhead often dominates (0.65s/test in old corpus tests)
- Profile with real workload (corpus-runner), not synthetic benchmarks
- macOS `sample` is more reliable than samply for long-running processes
- Look for hot spots in system calls (malloc/free/memmove) not just parser code
- Example from this codebase: 22% time in memory ops, 8% tokenization, only 4% token cloning

**Performance patterns:**
- Use `rayon::prelude::*` and `.par_iter()` for parallel processing (3-4x speedup on multi-core)
- `Arc<Mutex<Stats>>` for shared state, `Arc<AtomicUsize>` for lock-free counters
- Standalone binaries avoid test framework overhead (2,643x faster than libtest-mimic)
- `std::panic::catch_unwind` for graceful failure handling in parallel code

### Test Structure
Tests are organized by SQL dialect in the `tests/` directory:
- `sqlparser_common.rs` - Generic/cross-dialect tests
- `sqlparser_snowflake.rs` - Snowflake-specific tests
- `sqlparser_postgres.rs` - PostgreSQL-specific tests
- `sqlparser_bigquery.rs` - BigQuery-specific tests
- `sqlparser_clickhouse.rs` - ClickHouse-specific tests
- `sqlparser_mysql.rs`, `sqlparser_mssql.rs`, etc. - Other dialects

Each test file contains comprehensive parsing tests for dialect-specific syntax.

### Corpus Testing

The `corpus-runner` binary is a standalone tool for parsing corpus tests without test framework overhead:

```bash
# Build the corpus-runner
cargo build --release --bin corpus-runner

# Run all corpus tests (completes in ~7 seconds for 100k files)
# Use RUST_BACKTRACE=1 to get parser call chain in error messages for easier debugging
RUST_MIN_STACK=8388608 RUST_BACKTRACE=1 target/release/corpus-runner tests/corpus
# Run specific dialect directory
RUST_MIN_STACK=8388608 RUST_BACKTRACE=1 target/release/corpus-runner tests/corpus/bigquery
# Compare reports
node scripts/compare-corpus-reports.js target/corpus-report.json target/corpus-results/corpus-report-*.json
```

**Performance:**
- Processes ~15,000 files/sec on M-series Mac (using 3-4 CPU cores via rayon)
- 99,856 files in 6.7 seconds
- **2,643x faster than old libtest-mimic test harness**
- Generates `target/corpus-report.json` for CI/CD

**Features:**
- Parallel processing with rayon
- Progress reporting every 1000 files
- Per-dialect pass/fail stats
- Handles panics and stack overflows gracefully
- Same binary used in GitHub Actions (`.github/workflows/corpus.yml`)
- Can be used for profiling (same workload as tests)
- `customer_*` and `synq_*` dialect prefixes automatically stripped (customer_bigquery → bigquery, synq_clickhouse → clickhouse)
- **Cannot run corpus-runner on subdirectories** — dialect is extracted from first path component relative to corpus root
- **Corpus files are symlinked** from `kernel-cll-corpus` repo — commit corpus changes there, not in sqlparser-rs
- Analyze failures: parse `target/corpus-report.json` with Python to filter/group `test_results` by dialect or error pattern
- **Always rebuild before corpus run**: `cargo build --release --bin corpus-runner` — stale binary produces stale reports
- **Refresh baseline after each accepted commit**: `cp target/corpus-report.json target/corpus-report-baseline.json`. Otherwise `compare-corpus-reports.js` credits old deltas and can hide fresh regressions.
- `compare-corpus-reports.js` only lists *added* tests under "New Tests" — deleted/pruned files don't appear there. After a kernel-cll-corpus pipeline run, also check `git status -s` in that repo to see what was removed.
- **Pipeline reprocess (`make process` in kernel-cll-corpus) takes ~10 minutes** for the full corpus. Don't poll — arm a Monitor on `while pgrep -f pipeline.process; do sleep 15; done` and let it wake you.
- **Anonymizer-corruption signature**: `'s'<word>` (the `'s'` placeholder string directly abutting an identifier/keyword, e.g. `'s'HOUR`, `'s'id_5`) is unique to anonymizer misalignment. Filter on exactly `'s'<word>` — a broader `'<anything>'<word>` regex misaligns on multi-string SQL (`'foo','bar'`) and silently deletes hand-written sqlglot fixtures.
- **Query-log truncation heuristics** (`pipeline/process.py::_looks_truncated`) that worked without false positives: trailing punctuation (`,`/`(`/`=`/operator), trailing clause keyword (SELECT/FROM/BY/AS/…), and `CASE` count > `END` count. Removed ~4k Redshift query-log fragments.
- Adding a real dispatch in `parse_create` for a previously-unsupported `CREATE <X>` shape can flag *new* corpus failures: files that slipped through the generic skip-until-semicolon fallback are now actually parsed. Either extend support, accept on a case-by-case basis, or fall back gracefully — but expect the delta.
- **This repo is PUBLIC** (`getsynq/sqlparser-rs`, a fork of `apache/datafusion-sqlparser-rs`). Never put customer names, workspace IDs, or internal codenames into commit messages, branch names, PR titles, file names, or function names — even anonymized SQL content must be attributed generically. Every push is mirrored into GH Archive's permanent public dataset, which force-push cannot undo.
- **Pulling real SQL from production Clickhouse for regression coverage**: `SELECT sql FROM schema.latest_sql_definitions FINAL WHERE workspace='<name>' AND asset_type IN (...)`. `asset_type` codes from `proto/core/types/v1/asset_type.proto` that carry SQL bodies parseable by this library:
  - Snowflake: 508 view, 510 dynamic table, 511 task, 513 materialized view, 514 procedure, 515 function
  - Redshift: 805 view, 806 procedure, 807 function
  - BigQuery: 105 view
  - Databricks: 1805 view · ClickHouse: 1305 view · Postgres: 1605 view · MySQL: 1705 view · MSSQL: 3405 view · Oracle: 3505 view · DuckDB: 2005 view · Trino: 2105 view

  Anonymize schemas, tables, columns, function names, and string literals before landing anything as a test file; test file and function names must also be generic (no customer identifier).

**Development workflow:**
- Use corpus-runner for fast feedback (6.7s vs 2+ hours)
- No need to run via cargo test - it's a standalone binary
- CI uses the same binary (`target/release/corpus-runner tests/corpus`)
- Report format matches old test harness for compatibility

**For profiling:**
```bash
# Profile for 2 minutes with macOS sample
target/release/corpus-runner tests/corpus &
PID=$!
sample $PID 120 -file /tmp/profile.txt
kill $PID

# View hot spots
grep "Sort by top of stack" -A 30 /tmp/profile.txt
```

## Architecture

### Core Components

#### 1. Tokenizer (`src/tokenizer.rs`)
- Lexical analysis - converts SQL strings into tokens
- Handles different quote styles, identifiers, keywords, operators
- Dialect-aware tokenization

#### 2. Parser (`src/parser/mod.rs`)
- **Design**: Hand-written recursive descent parser
- **Expression parsing**: Uses Pratt Parser (TDOP - Top-Down Operator-Precedence) for expressions
- **Recursion protection**: `RecursionCounter` prevents stack overflow on deeply nested queries
- Main entry point: `Parser::parse_sql(&dialect, sql)`
- Most parsing logic is in the massive `src/parser/mod.rs` file (~412KB)
- ALTER statement parsing is separated into `src/parser/alter.rs`

#### 3. AST (`src/ast/mod.rs`)
- Complete Abstract Syntax Tree representation of SQL
- All AST nodes implement `Debug`, `Clone`, `PartialEq`, `Eq`
- Optional features:
  - `serde`: Serialize/deserialize AST nodes
  - `visitor`: Recursive AST walking via Visitor pattern
- Sub-modules:
  - `data_type.rs` - SQL data type definitions
  - `ddl.rs` - DDL statement structures (CREATE, ALTER, DROP)
  - `dcl.rs` - DCL statement structures (GRANT, REVOKE, ALTER ROLE)
  - `query.rs` - Query structures (SELECT, joins, CTEs, window functions)
  - `value.rs` - Literal values and constants
  - `operator.rs` - Binary and unary operators
  - `visitor.rs` - AST visitor pattern (when `visitor` feature enabled)

#### 4. Dialects (`src/dialect/`)
Each dialect module defines parsing behavior variations:
- `GenericDialect` - Default baseline dialect
- `AnsiDialect` - Strict ANSI SQL:2011
- `SnowflakeDialect`, `PostgreSqlDialect`, `BigQueryDialect`, `MySqlDialect`, etc.
- **Note**: `customer_*` prefixed dialects (e.g., `customer_bigquery`) map to their base dialect

Dialects control:
- Quote character handling for identifiers
- Reserved vs. non-reserved keywords
- Custom syntax extensions
- Operator support

### Key Design Patterns

#### Dialect-Specific Parsing
Use the `dialect_of!` macro to check parser's dialect:
```rust
if dialect_of!(parser is SnowflakeDialect | BigQueryDialect) {
    // Parse Snowflake/BigQuery-specific syntax
}
```

#### Pratt Parser for Expressions
Expression parsing uses operator precedence climbing:
- `parse_expr()` - Entry point
- `parse_prefix()` - Handles prefix operators and primary expressions
- `parse_infix()` - Handles binary operators based on precedence
- Precedence levels defined in `get_precedence()`

#### Common Parser Patterns

**Lookahead and backtracking:**
```rust
if self.peek_token().token == Token::Keyword(Keyword::FOO) {
    self.next_token(); // consume
    // ... parse FOO syntax
} else {
    self.prev_token(); // backtrack if needed
}
```

**Negative lookahead** (distinguish between similar patterns):
```rust
// Check for absence of keywords to detect non-keyword identifier
if !matches!(self.peek_token().token, Token::Word(w) if w.keyword == Keyword::PARTITION) {
    // Parse as identifier, not as PARTITION keyword
}
```

**Reserved keyword lists** (`src/keywords.rs`):
- `RESERVED_FOR_COLUMN_ALIAS` - Keywords that can't be column aliases in SELECT
- `RESERVED_FOR_TABLE_ALIAS` - Keywords that can't be table aliases in FROM/JOIN
- Add clause-level keywords (FORMAT, SETTINGS, SAMPLE) to BOTH lists to prevent incorrect alias parsing

**Dialect conflicts** (same keyword, different syntax):
- Problem: Keyword parsed in multiple locations (e.g., SAMPLE as table factor vs SELECT clause)
- Solution: Use `dialect_of!` to exclude conflicting dialects from one parsing location
- Example: ClickHouse `SAMPLE n` (clause) vs Snowflake `SAMPLE (n)` (table factor) - exclude ClickHouse from table factor parsing

**ParserError construction:**
`ParserError::ParserError` wraps `ParserErrorMessage` (not a raw `String`). Use `.into()` to convert:
```rust
ParserError::ParserError(format!("msg {x}").into())
ParserError::ParserError("literal message".into())
```
This applies to test assertions too. The `parser_err!` macro handles conversion automatically.

**Balanced paren consumption** (opaque clause parsing):
```rust
// Consume MATCH_RECOGNIZE(...), CODEC(...), BEFORE(...), etc. without deep AST support
self.expect_token(&Token::LParen)?;
let mut depth = 1i32;
while depth > 0 {
    match self.next_token().token {
        Token::LParen => depth += 1,
        Token::RParen => depth -= 1,
        Token::EOF => break,
        _ => {}
    }
}
```
Use this pattern for dialect-specific clauses that don't need AST representation.

**Reserved-keyword-as-alias carve-out** (recurring fix shape):
- When a keyword (e.g. CLUSTER, SORT, FINAL, AT, BEFORE) is reserved only because of a specific clause (`CLUSTER BY`, `t AT(...)` time-travel), accept it as an identifier alias in `parse_optional_alias` when the lookahead doesn't match the clause shape (next-not-`BY`, next-not-`(`, etc.).
- Existing instances in `parse_optional_alias` for CLUSTER / SORT / FINAL / VIEW / OPTION / USE/IGNORE/FORCE in `parse_table_factor` for AT / BEFORE serve as templates — copy the matching block rather than reinventing.
- **Match-arm ordering trap:** the catch-all `Token::Word(w) if after_as || !reserved_kwds.contains(&w.keyword)` arm fires first for any non-reserved keyword. New carve-outs for dialect-specific clauses (e.g. `OPTION (`, `USE INDEX`) must be placed *before* it or they silently never run.

**Trailing-comma support** lives in `is_parse_comma_separated_end` (`src/parser/mod.rs:~4280`). Extending the set of clauses where a trailing `,` is tolerated (FROM list, IN list, …) goes there, or in the per-clause loop's bespoke check (e.g. `parse_from_clause_body`'s Snowflake terminator set).

**Prefer `WithSpan<Ident>` / `WithSpan<Expr>` in new AST fields:**
`parse_identifier(in_table_clause)?` returns `WithSpan<Ident>`. Keep that wrapping in new AST nodes so kernel-cll lineage can surface source positions. Reach for `parse_identifier_no_span()` only when there's a specific reason not to carry the span (e.g. matching an existing surrounding type that's still plain `Ident`). For new `Expr` fields, wrap with the standard idiom: `let start_idx = self.index; let expr = self.parse_expr()?; expr.spanning(self.span_from_index(start_idx))` — see `selection` in `parse_select` for the template. `span_from_index` anchors its end at `self.index - 1` (the last consumed token), so the trailing `)` of an enclosing `(...)` is correctly excluded.

**No `peek_keyword`:**
For one-token keyword lookahead, use `matches!(self.peek_token_kind(), Token::Word(w) if w.keyword == Keyword::FOO)`. For fixed-length sequences use `peek_keywords::<N>() -> [Keyword; N]`. There is no `peek_keyword(Keyword::FOO) -> bool`.

**Match non-keyword words case-insensitively, don't add to `keywords.rs`:**
Words that appear only in narrow constructs (e.g. `SEMANTIC`, `EXCLUDING`, `ADDITIVE`, `SYNONYMS`, `CORTEX`) shouldn't go in `ALL_KEYWORDS` — adding them risks breaking identifier usage and the reserved-alias lists. Match with `Token::Word(w) if w.value.eq_ignore_ascii_case("FOO")` (or the `peek_word_ci` / `parse_word_ci` helpers in the SEMANTIC VIEW parser).

**Tokenizer `Number("N.")` quirk:**
The tokenizer greedily folds a trailing `.` into the number token, so `proj-NNN.dataset` produces `Number("NNN.")` followed by `Word("dataset")`. To consume the digit prefix as part of a hyphenated identifier and keep the dot as a separator, mutate the just-consumed token in place: `self.tokens[idx].token = Token::Period;` then `prev_token()` so the surrounding `parse_object_name` loop continues. See the BigQuery hyphenated project-ID block in `parse_identifier` for the template.

**Dialect struct names** (for `dialect_of!` macro):
- `RedshiftSqlDialect` (NOT `RedshiftDialect`), `AnsiDialect`, `DuckDbDialect`
- `ClickHouseDialect`, `SnowflakeDialect`, `BigQueryDialect`, `PostgreSqlDialect`
- `MySqlDialect`, `MsSqlDialect`, `SQLiteDialect`

## Development Guidelines

### Syntax vs Semantics
This parser is **syntax-only** - it does NOT perform semantic validation. For example:
- `CREATE TABLE(x int, x int)` parses successfully (duplicate column names)
- Type checking is not performed
- Schema validation is not done

Semantic analysis varies drastically between SQL dialects and is left to consumers of this library.

### Extending the Parser

#### Adding New Syntax
1. **Update AST** (`src/ast/*.rs`): Add new AST node structures if needed
2. **Update Parser** (`src/parser/mod.rs`): Add parsing logic
3. **Add Tests**: Write dialect-specific tests in appropriate test file
4. **Consider Dialect**: Use `dialect_of!` if syntax is dialect-specific

#### Adding New Keywords
Keywords in `src/keywords.rs` MUST be in strict alphabetical order — `ALL_KEYWORDS` uses binary search.
If a keyword is out of order, the tokenizer silently fails to recognize it (maps to `Keyword::NoKeyword`).
Verify ordering carefully: e.g., `EXCHANGE` < `EXCLUDE` < `EXEC` (compare character by character).

#### AST Change Workflow
When adding fields to AST structs, you must update ALL pattern matches:
1. Add field to struct definition (e.g., `src/ast/mod.rs`, `src/ast/query.rs`)
2. Update Display implementation to output new field
3. Update parser to initialize new field
4. Fix all test files - add `new_field: _` to pattern matches (Rust errors E0027, E0063 guide you)
5. Use `cargo check` to find all locations requiring updates

**High-churn structs**: `Function` struct is constructed in ~30+ places across parser and test files. Adding a field requires updating all of them — use `replace_all` or agent assistance.

**Multi-line sed on macOS**: `sed -i ''` operates line-by-line and silently skips multi-line patterns. For bulk edits that span newlines (e.g. appending a field after a `params,\n        })` block) use `perl -i -0pe '…'` instead.

**Recursive AST types**: New fields containing `Expr` inside `Function`/`Expr` cycle need `Box<Expr>` to break infinite size (e.g., `HavingBound(Box<Expr>)`).

**`ObjectName` uses `Vec<Ident>`** not `Vec<WithSpan<Ident>>` — don't wrap idents in `WithSpan` when constructing manually.

**`DECLARE` has 5 dialect-specific branches** in `parse_declare` (Snowflake block, Databricks variable+cursor, BigQuery variable-list, T-SQL `@var [AS] type [= expr]`, Postgres-style cursor). Order matters: each branch returns early; a later branch never runs if an earlier one matched. When adding a new dialect, gate by `dialect_of!` *and* by a discriminator that won't false-match the others (e.g. T-SQL gates on `name.value.starts_with('@')`, Databricks falls through to the cursor path when `peek == CURSOR`).

**Variable-length type lengths** are split: `parse_optional_character_length` (used by VARCHAR / CHAR — accepts `MAX` and unit suffixes) vs `parse_optional_precision` (used by NVARCHAR / VARBINARY / TIMESTAMP / etc. — `Option<u64>`, accepts `MAX` as `None`). When a new type accepts `MAX`, pick the right helper or extend it; don't write a third one.

#### Upstream Compatibility
Since this is a fork of apache/datafusion-sqlparser-rs:
- Avoid creating new AST node types when possible
- Prefer parameterizing existing AST nodes for compatibility
- Document SYNQ-specific extensions clearly
- Consider if changes should be contributed upstream

### Testing Requirements
- **All PRs must include tests** - PRs without tests will not be reviewed
- Test both success and error paths
- Use `pretty_assertions` for readable diffs
- Run `cargo fmt`, `cargo clippy`, `cargo nextest run` before submitting
- CI runs: `cargo check`, `cargo nextest run --all-features`
- `cargo nextest run` without `RUST_MIN_STACK=8388608` always SIGABRTs three tests (`parse_deeply_nested_expr_hits_recursion_limits`, `..._parens_...`, `..._subquery_...`). These are stack-size probes, not regressions — ignore if the rest of the run is green.
- Avoid adding serde dependency to test code - use manual JSON building if needed
- `verified_stmt(s)` requires `s` to be a full statement that round-trips (parse → display → equal). For fragment / coverage tests on a non-statement (a bare CAST, a SELECT-list snippet), wrap in `SELECT` or call `parse_sql_statements(input).unwrap()` directly — `verified_stmt` will otherwise fail with `Expected an SQL statement, found: ...`.

**Running corpus tests (now fast!):**
```bash
# Run full corpus test suite (~7 seconds for 100k files)
target/release/corpus-runner tests/corpus

# Or use the benchmark suite for micro-benchmarking
cargo bench
```

## Performance Optimization

**⚠️ CRITICAL: Profile before optimizing!**

Without profiling data, optimizations are guesswork. Benchmarks measure total time but don't show WHERE time is spent.

**Profiling workflow (macOS):**
```bash
# 1. Create a simple profiling binary (benches/simple_profile.rs):
#    - Loop parse operations 10,000+ times
#    - Add [[bin]] section to Cargo.toml pointing to it

# 2. Build in release mode
cargo build --release --bin profile_parse

# 3. Profile with samply (install: cargo install samply)
samply record target/release/profile_parse
# Opens Firefox Profiler UI at http://127.0.0.1:3000
# Shows call stacks, hot functions, time distribution

# Alternative: cargo-instruments (install: brew install cargo-instruments)
cargo instruments --bin profile_parse --release --template time
# Note: May fail with Xcode symbol errors on some macOS versions
# If it works, opens .trace file in Instruments.app

# Fallback: Use Instruments directly
xcrun xctrace record --template 'Time Profiler' \
  --output profile.trace \
  --launch -- target/release/profile_parse
# Open with: open profile.trace
```

**Profiling on Linux:**
```bash
# Use perf (better than macOS tools)
cargo build --release --bin profile_parse
perf record -g target/release/profile_parse
perf report
# Or generate flamegraph:
cargo flamegraph --bin profile_parse
```

**Benchmarking with criterion:**
```bash
# Add to Cargo.toml dev-dependencies: criterion = { version = "0.5", features = ["html_reports"] }
# Create benches/benchmark_name.rs with criterion_group! and criterion_main!
# Add [[bench]] section to Cargo.toml with harness = false
cargo bench --bench benchmark_name                    # Run benchmarks
cargo bench --bench benchmark_name -- --save-baseline name  # Save baseline
```

**Important caveats:**
- **Profile FIRST, optimize SECOND** - Don't guess where bottlenecks are
- Rust compiler can inline and optimize stack allocations, but **cannot eliminate heap allocations**
- `Token` enum contains `String` fields - clones require heap allocation
- Benchmarks show TOTAL time, profiling shows WHERE time is spent
- Small improvements (3-5%) may not be worth code complexity without profiling data

**Bulk code transformations:**
```bash
# Always backup before bulk sed operations
cp src/parser/mod.rs src/parser/mod.rs.backup
# Use sed carefully - test incrementally
sed -i '' 's/pattern/replacement/g' src/parser/mod.rs
cargo check  # Verify after each transformation
```

## Features (Cargo.toml)

```toml
[features]
default = ["std"]
std = []                    # Standard library support
serde = [...]              # Serialize/deserialize AST
visitor = [...]            # AST visitor pattern
json_example = [...]       # JSON output in CLI example
```

## CLI Tool

The crate includes a CLI for parsing SQL and dumping JSON:
```bash
cargo run --features json_example --example cli queries/example.sql
cargo run --features json_example --example cli queries/example.sql --snowflake
# Dialect flag is the dialect name as a `--<name>` token: --bigquery, --snowflake,
# --redshift, --postgres, --clickhouse, --duckdb, --mssql, --mysql, --hive, --sqlite,
# --ansi, or --generic (default). Not `--dialect <name>`.
```

## Troubleshooting

### "Failed to look up symbolic reference" error
This is a known Xcode Instruments issue on some macOS versions. Use samply instead.

### Cargo.toml Errors
- "duplicate key" error: Check for multiple `[dependencies]` sections (merge them)
- Ensure dev-dependencies use correct section name (not `[dev-dependencies]` twice)

### Profiling Issues
- samply may not generate output on timeout - use macOS `sample` command instead
- Test framework overhead dominates profiles - use corpus-runner for real workload
- Profile shows mutex locks: test harness coordination overhead, not parsing work

### Performance Debugging
- Always check actual numbers: "25k files/sec" during warmup ≠ sustained 15k files/sec
- Measure complete runs, not just initial progress reports
- Use `time` command to verify total execution time matches reported rate

## Automated fix loop (`scripts/fix-corpus-loop.sh`)

- Exits cleanly after 5 minutes without a new commit (interpreted as "no viable tier-1 fix left", not a bug). Restart manually to resume.
- Commits land **locally only** — CI (`corpus.yml`) and the PR corpus-report comment refresh only when someone pushes the branch.
- Prompt lives in `.claude/fix-corpus-loop.md`; tier-1 = `unparsed_*` + `customer_*` only, tier-2/3 patterns are explicitly out of scope.

## Related Documentation

- [Custom SQL Parser Guide](docs/custom_sql_parser.md) - How to write dialect extensions
- [Fuzzing](docs/fuzzing.md) - Fuzz testing setup
- [Benchmarking](docs/benchmarking.md) - Performance benchmarks
- [Upstream Repository](https://github.com/apache/datafusion-sqlparser-rs) - Apache DataFusion SQL Parser
