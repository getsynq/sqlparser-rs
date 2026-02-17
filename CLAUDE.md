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
RUST_MIN_STACK=8388608 RUST_BACKTRACE=1 target/release/corpus-runner tests/corpus --errors

# Run specific dialect directory
RUST_MIN_STACK=8388608 RUST_BACKTRACE=1 target/release/corpus-runner tests/corpus/bigquery --errors

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
- `customer_*` dialect prefixes automatically stripped (customer_bigquery → bigquery)

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

#### AST Change Workflow
When adding fields to AST structs, you must update ALL pattern matches:
1. Add field to struct definition (e.g., `src/ast/mod.rs`, `src/ast/query.rs`)
2. Update Display implementation to output new field
3. Update parser to initialize new field
4. Fix all test files - add `new_field: _` to pattern matches (Rust errors E0027, E0063 guide you)
5. Use `cargo check` to find all locations requiring updates

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
- Avoid adding serde dependency to test code - use manual JSON building if needed

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
cargo run --features json_example --example cli queries/example.sql --dialect snowflake
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

## Related Documentation

- [Custom SQL Parser Guide](docs/custom_sql_parser.md) - How to write dialect extensions
- [Fuzzing](docs/fuzzing.md) - Fuzz testing setup
- [Benchmarking](docs/benchmarking.md) - Performance benchmarks
- [Upstream Repository](https://github.com/apache/datafusion-sqlparser-rs) - Apache DataFusion SQL Parser
