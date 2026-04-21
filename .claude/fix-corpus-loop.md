# SQL Parser Corpus Fix Loop

You are working on the SYNQ fork of sqlparser-rs at `/Users/lustefaniak/getsynq/cloud/kernel-cll/sqlparser-rs`. Your goal is to fix one failing corpus test per iteration.

## Priority

**Focus on customer-facing issues first.** The corpus directories have these priority tiers:

1. **Highest priority — customer query logs & real customer SQL:**
   - `unparsed_snowflake`, `unparsed_bigquery`, `unparsed_redshift`, `unparsed_trino`
   - `customer_snowflake`, `customer_bigquery`, `customer_redshift`, `customer_clickhouse`, `customer_databricks`, `customer_postgres`, etc.
2. **High priority — first-party dialect tests:**
   - `snowflake`, `bigquery`, `redshift`, `databricks`, `clickhouse`, `postgres`
3. **Lower priority — sqlglot fixtures and others:**
   - `sqlglot_*`, `synq_*`, etc.

**Dialect priority order:** Snowflake > BigQuery > Redshift > Databricks > ClickHouse > Postgres > everything else.

When picking which failure to fix, prefer higher-priority directories and dialects. Within the same priority, prefer error patterns that affect more files.

## Dialect strategy

- **Prefer adding support to GenericDialect** (the broadest dialect) unless the syntax genuinely conflicts with another dialect's grammar. GenericDialect improvements benefit all dialects at once.
- Only gate behind `dialect_of!` when a syntax is truly dialect-specific and would break parsing in other dialects.
- When a syntax works in multiple databases (e.g., `QUALIFY`, `LATERAL FLATTEN`, `COPY INTO`), implement it in GenericDialect if possible.

## Lineage focus

This parser's primary consumer is CLL (column-level lineage). **Everything useful for lineage analysis must be preserved in the parsed AST:**
- Table references (FROM, JOIN, INSERT INTO, MERGE, COPY INTO targets/sources)
- Column references (SELECT list, WHERE, GROUP BY, HAVING, ORDER BY, window functions)
- CTEs, subqueries, set operations (UNION/INTERSECT/EXCEPT)
- Column aliases and table aliases
- Star expressions with EXCLUDE/EXCEPT modifiers

When implementing a fix, ask: "Does the AST preserve enough information for a lineage tracer to connect output columns to input tables/columns?" If not, the fix needs more structure.

### When balanced-paren consumption is NOT acceptable

CLAUDE.md documents a pattern for opaque clause consumption (count parens, skip tokens until matched close). This is a **last-resort shortcut** that is only acceptable when the consumed body contains **no table or column references**. Examples where it is OK: `CODEC(...)` compression specs, `MATCH_RECOGNIZE(PATTERN ...)` pattern definitions, storage `SETTINGS (...)`.

It is **NOT acceptable** when the clause contains:
- Table names (e.g. subqueries, `FROM` inside a pivot source, `REFERENCES tbl(col)`)
- Column references (e.g. window frames, `QUALIFY` expressions, `HAVING`, `USING (cols)`, filter/predicate clauses)
- CTE or query bodies

If the clause has lineage content, you must parse it into real AST nodes (even if you reuse existing `Expr`/`Query` variants). A parse that "succeeds" by silently eating tables/columns is a *silent regression* in the real goal — the corpus report will look greener while CLL output gets worse. Prefer leaving a failure in the corpus over landing a lineage-blind parse.

### Verify AST exposes refs after a fix

When your fix touches a construct that holds table/column refs, check the test asserts those identifiers appear in the AST — not just that `verified_stmt` roundtrips. A roundtripping `to_string()` can still hide a structure where a visitor can't reach the refs.

## Workflow

### Step 1: Identify a fixable failure
Run the corpus runner and analyze failures:
```bash
cargo build --release --bin corpus-runner 2>&1 | tail -5
RUST_MIN_STACK=8388608 RUST_BACKTRACE=1 target/release/corpus-runner tests/corpus 2>&1 | tail -20
```

Save the baseline report for regression checking:
```bash
cp target/corpus-report.json target/corpus-report-baseline.json
```

Then analyze failures to pick one to fix, **filtered by priority**:
```bash
python3 -c "
import json, collections
data = json.load(open('target/corpus-report.json'))
results = data.get('test_results', {})
fails = [(k,v) for k,v in results.items() if v.startswith('fail:')]

# Priority tiers
tier1 = ['unparsed_', 'customer_']
tier2_dialects = ['snowflake', 'bigquery', 'redshift', 'databricks', 'clickhouse', 'postgres']

by_error = collections.defaultdict(lambda: {'files': [], 'tier1': 0, 'tier2': 0, 'tier3': 0})
for k,v in fails:
    err = v[5:].strip()
    by_error[err]['files'].append(k)
    if any(k.startswith(t) for t in tier1):
        by_error[err]['tier1'] += 1
    elif k.split('/')[0] in tier2_dialects:
        by_error[err]['tier2'] += 1
    else:
        by_error[err]['tier3'] += 1

# Sort by tier1 count desc, then tier2, then total
ranked = sorted(by_error.items(), key=lambda x: (x[1]['tier1'], x[1]['tier2'], len(x[1]['files'])), reverse=True)
for err, info in ranked[:15]:
    total = len(info['files'])
    print(f'[{total}] (t1:{info[\"tier1\"]} t2:{info[\"tier2\"]} t3:{info[\"tier3\"]}) {err[:120]}')
    for f in info['files'][:3]:
        print(f'    {f}')
    print()
"
```

**Pick a failure pattern that:**
- Has the most tier-1 (customer/unparsed) failures
- Affects important dialects (Snowflake > BigQuery > Redshift > Databricks > ClickHouse > Postgres)
- Looks like a parser limitation (not garbage SQL)
- You can actually fix without massive AST changes

Read 2-3 example SQL files for the chosen error pattern to understand what syntax is failing.

### Step 2: Understand the root cause
- Read the failing SQL files to understand what SQL construct is not supported
- **When in doubt about correct syntax, look up the official database documentation online** (e.g., Snowflake docs, BigQuery docs, ClickHouse docs, Redshift docs, etc.) to confirm the expected grammar before implementing a fix
- Search the parser code to find where parsing fails
- Determine the minimal change needed

#### Exit hatch: invalid SQL in the corpus

Corpus files come from upstream scraping (query logs, customer dumps, sqlglot fixtures) and can contain genuinely broken SQL — truncated queries, template placeholders, shell artifacts, non-SQL text. **We do not want to contort the parser to accept invalid input.** If, after consulting the official dialect docs, the SQL looks invalid per the stated dialect (not just "unusual"), do not attempt a fix.

Signs the SQL is probably invalid:
- Unbalanced parens/quotes that no dialect would accept
- Template markers (`{{ ... }}`, `%(name)s`, `$VAR` outside dialects that support it)
- Truncation (query ends mid-clause, e.g. `SELECT a FROM` with nothing after)
- Syntax that contradicts the official grammar for every dialect the file could belong to
- Tokens that are clearly from another language (Python tracebacks, JSON blobs, shell output)

When you conclude SQL is invalid:
1. Do **not** modify the parser to accept it.
2. Do **not** modify or delete the corpus file (corpus is a symlink to `kernel-cll-corpus` and not owned by this repo).
3. Report the path and a one-line reason in your final message (so it can later be pruned from `kernel-cll-corpus`), and pick a different failure pattern for this iteration.
4. If *every* candidate failure in the top tier looks invalid, report that and stop the iteration without a commit — the loop's no-commit timer will end the run.

### Step 3: Implement the fix
**Rules:**
- Focus on making the SQL **parseable**. Preserve table/column references and query structure in the AST — this is critical for lineage.
- Preserve as much of the original query structure as possible in the AST and Display output.
- Prefer roundtrip fidelity: `parse(sql).to_string()` should ideally reproduce the input.
- **Add support to GenericDialect unless the syntax conflicts with another dialect.** Only use `dialect_of!` when truly needed.
- Avoid creating new AST node types when possible — parameterize existing ones.
- Keywords in `src/keywords.rs` MUST be in strict alphabetical order (binary search).
- If adding keywords to reserved lists (`RESERVED_FOR_COLUMN_ALIAS`, `RESERVED_FOR_TABLE_ALIAS`), add to BOTH.
- `ParserError::ParserError` wraps `ParserErrorMessage`, use `.into()`.

### Step 4: Write tests
- Add tests in the appropriate dialect test file (`tests/sqlparser_<dialect>.rs`)
- Use `verified_stmt(sql)` for roundtrip tests, `one_statement_parses_to(input, expected)` for non-roundtrip
- Test with the simplest possible SQL that reproduces the pattern
- Keep tests focused and minimal

### Step 5: Validate — no regressions
Run in this exact order:

1. **Format:**
   ```bash
   cargo fmt
   ```

2. **Compile check:**
   ```bash
   cargo check --all-features
   ```

3. **Run all unit tests:**
   ```bash
   cargo nextest run --all-features
   ```
   If any test fails, fix it before proceeding.

4. **Rebuild corpus runner and run corpus tests, then compare:**
   ```bash
   cargo build --release --bin corpus-runner
   RUST_MIN_STACK=8388608 target/release/corpus-runner tests/corpus 2>&1 | tail -20
   node scripts/compare-corpus-reports.js target/corpus-report.json target/corpus-report-baseline.json
   ```

   **CRITICAL: There must be ZERO regressions.** If the comparison shows any regressions (tests that were passing and now fail), you must fix them before committing. Improvements (tests now passing) are expected. If you cannot eliminate the regressions, revert your changes (`git checkout -- .`) and pick a different failure for the next iteration rather than commit a net-negative change.

### Step 6: Commit
If and only if all validations pass with zero regressions, commit the single fix on its own so it is easy to inspect and revert if needed. **One fix per commit.**

Commit message format (matches this repo's history — run `git log --oneline -20` to cross-check):

```
fix(<dialect>): <imperative short description, lowercase, no trailing period>

<1-3 sentence explanation of the syntax and how it is parsed>.

Fixes <N> corpus test failures (<Dialect>[, <Dialect2>]).
```

Rules:
- Scope format: `fix(snowflake):`, `fix(snowflake,redshift):` for multi-dialect, or plain `fix:` when the change applies broadly across all/most dialects.
- Subject is imperative mood (e.g. "support", "handle", "preserve"), lowercase, no trailing period.
- **A body paragraph is required** — briefly state what syntax is now accepted and the parsing approach. This is non-negotiable; every commit in history has one.
- Footer line: `Fixes N corpus test failures` with an optional `(Dialect[, Dialect])` qualifier when the fix is dialect-scoped. Use the count reported by `compare-corpus-reports.js`.
- **Do NOT add a `Co-Authored-By: Claude` / `Generated with Claude Code` trailer** — the repo's history does not carry them and they add noise.
- Stage explicitly the files you changed (`git add <paths>`). Prefer that over `git add -A` so stray artifacts don't slip in.

Example (real commit from history):
```
fix(clickhouse): handle EXTRACT as regex function when first arg is string

ClickHouse overloads EXTRACT as a regex function: extract(str, pattern).
When the first argument after ( is a string literal, parse as a regular
function call instead of date extraction.

Fixes 2 corpus test failures (ClickHouse).
```

### Step 7: Report and stop
Report what you fixed, how many corpus tests improved, and confirm zero regressions. Then stop — the next loop iteration will pick the next failure.

## Important reminders
- Read CLAUDE.md in the repo for detailed coding guidelines
- Always rebuild corpus-runner after parser changes: `cargo build --release --bin corpus-runner`
- The corpus is symlinked from kernel-cll-corpus — never modify corpus files
- `customer_*` and `synq_*` dialect prefixes are stripped to their base dialect
- If a fix requires large AST changes, prefer a smaller fix first and note the larger change needed
- One fix per commit — don't bundle multiple independent fixes, even if you spot them in the same iteration
- Don't spend more than one fix per iteration — commit and let the next loop handle the next issue
