# Corpus Test Scripts

Helper scripts for running and comparing corpus test results.

## Quick Start

### Run corpus tests and save results
```bash
./scripts/run-corpus-tests.sh
```

### Run and compare with previous run
```bash
./scripts/run-corpus-tests.sh --compare
```

### Compare two specific reports
```bash
node scripts/compare-corpus-reports.js target/corpus-report.json target/corpus-results/corpus-report-20260209-123456.json
```

## Scripts

### `run-corpus-tests.sh`

Runs the corpus test suite and saves timestamped results.

**Usage:**
```bash
./scripts/run-corpus-tests.sh [--compare]
```

**What it does:**
- Runs `cargo test --test sqlparser_corpus`
- Saves results to `target/corpus-results/corpus-report-YYYYMMDD-HHMMSS.json`
- Saves full test output to `target/corpus-results/corpus-run-YYYYMMDD-HHMMSS.log`
- Shows summary (passed/failed counts, pass rate)
- With `--compare`: automatically compares with most recent previous run

**Example output:**
```
==> Running corpus tests...
✅ Results saved to: target/corpus-results/corpus-report-20260209-143022.json

==> Summary
  Passed: 3284 / 3617 (90.8%)
  Failed: 333
```

### `compare-corpus-reports.js`

Generates a markdown comparison report between two corpus test runs.

**Usage:**
```bash
node scripts/compare-corpus-reports.js <current-report.json> [baseline-report.json]
```

**What it does:**
- Loads and compares two corpus test reports
- Identifies:
  - ❌ **Regressions**: tests that were passing, now failing
  - ✅ **Improvements**: tests that were failing, now passing
  - ➕ **New tests**: added to corpus
  - ➖ **Removed tests**: removed from corpus
- Generates markdown report with per-dialect statistics
- If no baseline provided, shows only current stats

**Example output:**
```markdown
## Corpus Parsing Report

**Total: 3284 passed, 333 failed** (90.8% pass rate)

### Changes

❌ **2 regression(s)** (now failing)
✅ **15 improvement(s)** (now passing)

### By Dialect

| Dialect | Passed | Failed | Total | Pass Rate | Delta |
|---------|-------:|-------:|------:|----------:|------:|
| bigquery | 1000 | 50 | 1050 | 95.2% | **+5** |
...
```

## Tracking Progress

### View all saved reports
```bash
ls -lht target/corpus-results/corpus-report-*.json
```

### Compare current work against main branch baseline
```bash
# Save current state
./scripts/run-corpus-tests.sh

# Checkout main and run tests
git stash
git checkout main
./scripts/run-corpus-tests.sh

# Return to your branch
git checkout -
git stash pop

# Compare
MAIN_REPORT=$(ls -t target/corpus-results/corpus-report-*.json | head -2 | tail -1)
CURRENT_REPORT=$(ls -t target/corpus-results/corpus-report-*.json | head -1)
node scripts/compare-corpus-reports.js $CURRENT_REPORT $MAIN_REPORT
```

## CI/CD Integration

The GitHub Actions workflow (`.github/workflows/corpus.yml`) uses these scripts:

1. Runs corpus tests
2. Saves report to `target/corpus-report.json`
3. Downloads baseline from main branch
4. Runs `compare-corpus-reports.js` to generate comparison
5. Posts markdown report as PR comment

This ensures consistent reporting between local development and CI.
