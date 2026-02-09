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
Corpus tests in `tests/sqlparser_corpus.rs` parse real SQL from `tests/corpus/{dialect}/` directories:
```bash
# Run corpus tests with timestamped results and comparison
./scripts/run-corpus-tests.sh --compare

# Run manually with increased stack size (if needed)
RUST_MIN_STACK=4194304 cargo test --test sqlparser_corpus

# Compare two specific reports
node scripts/compare-corpus-reports.js target/corpus-report.json target/corpus-results/corpus-report-*.json

# List saved reports
ls -lht target/corpus-results/corpus-report-*.json
```

**Corpus test infrastructure:**
- Uses custom test harness (`harness = false` in Cargo.toml with libtest_mimic)
- `customer_*` dialect prefixes are automatically stripped (customer_bigquery → bigquery)
- Reports track individual test results for regression detection
- Stack size increased to 4MB (`RUST_MIN_STACK=4194304`) to handle deeply nested queries without excessive memory
- See `scripts/README.md` for detailed usage

**Debugging stack overflows:**
- If tests crash despite increased stack, use binary search to find problematic files
- Run subsets: `cargo test --test sqlparser_corpus -- dialect/category/`
- Check test logs for patterns before crash

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

## Related Documentation

- [Custom SQL Parser Guide](docs/custom_sql_parser.md) - How to write dialect extensions
- [Fuzzing](docs/fuzzing.md) - Fuzz testing setup
- [Benchmarking](docs/benchmarking.md) - Performance benchmarks
- [Upstream Repository](https://github.com/apache/datafusion-sqlparser-rs) - Apache DataFusion SQL Parser
