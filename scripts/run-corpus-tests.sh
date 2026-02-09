#!/bin/bash
set -e

# Run corpus tests and store results with timestamp
# Usage: ./scripts/run-corpus-tests.sh [--compare]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/target/corpus-results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
OUTPUT_FILE="$RESULTS_DIR/corpus-report-$TIMESTAMP.json"

# Parse arguments
COMPARE=false
if [ "$1" = "--compare" ]; then
    COMPARE=true
fi

echo "==> Running corpus tests..."
cd "$REPO_ROOT"

# Run tests (continue on error to get partial results)
# Set RUST_MIN_STACK to 4MB to handle deeply nested queries without excessive memory
# Use nextest for better timeout handling (see .config/nextest.toml)
RUST_MIN_STACK=4194304 cargo nextest run --test sqlparser_corpus --no-fail-fast 2>&1 | tee "$RESULTS_DIR/corpus-run-$TIMESTAMP.log" || true

# Check if report was generated
if [ ! -f "$REPO_ROOT/target/corpus-report.json" ]; then
    echo "❌ Error: corpus-report.json not generated"
    exit 1
fi

# Create results directory if it doesn't exist
mkdir -p "$RESULTS_DIR"

# Copy report to timestamped file
cp "$REPO_ROOT/target/corpus-report.json" "$OUTPUT_FILE"

echo ""
echo "✅ Results saved to: $OUTPUT_FILE"

# Show summary
PASSED=$(jq -r '.summary.total_passed' "$OUTPUT_FILE")
FAILED=$(jq -r '.summary.total_failed' "$OUTPUT_FILE")
TOTAL=$(jq -r '.summary.total_tests' "$OUTPUT_FILE")
PASS_RATE=$(echo "scale=1; $PASSED * 100 / $TOTAL" | bc)

echo ""
echo "==> Summary"
echo "  Passed: $PASSED / $TOTAL ($PASS_RATE%)"
echo "  Failed: $FAILED"

# Check if we hit stack overflow
if grep -q "stack overflow" "$RESULTS_DIR/corpus-run-$TIMESTAMP.log" 2>/dev/null; then
    echo ""
    echo "⚠️  Stack overflow detected - some tests may have crashed"
    echo "  Consider increasing RUST_MIN_STACK or investigating deeply nested queries"
fi

# Compare with previous run if requested
if [ "$COMPARE" = true ]; then
    # Find the most recent previous report (excluding current one)
    PREV_REPORT=$(find "$RESULTS_DIR" -name "corpus-report-*.json" -type f ! -name "corpus-report-$TIMESTAMP.json" | sort -r | head -1)

    if [ -n "$PREV_REPORT" ]; then
        PREV_TIMESTAMP=$(basename "$PREV_REPORT" | sed 's/corpus-report-\(.*\)\.json/\1/')
        echo ""
        echo "==> Comparing with previous run ($PREV_TIMESTAMP)"
        echo ""
        node "$SCRIPT_DIR/compare-corpus-reports.js" "$OUTPUT_FILE" "$PREV_REPORT"
    else
        echo ""
        echo "ℹ️  No previous report found for comparison"
    fi
fi

echo ""
echo "To compare with a specific run:"
echo "  node scripts/compare-corpus-reports.js $OUTPUT_FILE <baseline-report.json>"
echo ""
echo "To list all saved reports:"
echo "  ls -lht $RESULTS_DIR/corpus-report-*.json"
