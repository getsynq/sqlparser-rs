#!/usr/bin/env bash
# Runs the corpus fix loop using Claude Code (Opus) until no progress is made.
# Usage: ./scripts/fix-corpus-loop.sh [max_iterations] [-- extra claude flags...]
#
# Examples:
#   ./scripts/fix-corpus-loop.sh                          # unlimited, default flags
#   ./scripts/fix-corpus-loop.sh 20                       # max 20 iterations
#   ./scripts/fix-corpus-loop.sh 0 -- --model opus
#
# Stops when:
#   - No commit produced in the last 5 minutes (agent got stuck)
#   - Max iterations reached (default: unlimited)

set -euo pipefail
cd "$(dirname "$0")/.."

# Parse args: first positional is max_iterations, everything after -- is extra claude flags
MAX_ITERATIONS=0
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --)
      shift
      EXTRA_ARGS=("$@")
      break
      ;;
    *)
      MAX_ITERATIONS="$1"
      shift
      ;;
  esac
done

ITERATION=0
PROMPT_FILE=".claude/fix-corpus-loop.md"

if [ ! -f "$PROMPT_FILE" ]; then
  echo "Error: $PROMPT_FILE not found"
  exit 1
fi


if [ -f target/corpus-report.json ]; then
  echo "Starting corpus stats:"
  python3 -c "
import json
data = json.load(open('target/corpus-report.json'))
s = data['summary']
print(f\"  Passed: {s['total_passed']}/{s['total_tests']} ({s['total_passed']/s['total_tests']*100:.1f}%)\")
print(f\"  Failed: {s['total_failed']}\")
"
fi

echo "Starting corpus fix loop (model: opus, max iterations: ${MAX_ITERATIONS:-unlimited})"
if [ ${#EXTRA_ARGS[@]} -gt 0 ]; then
  echo "Extra claude flags: ${EXTRA_ARGS[*]}"
fi
echo "Press Ctrl+C to stop"
echo ""

while true; do
  ITERATION=$((ITERATION + 1))

  if [ "$MAX_ITERATIONS" -gt 0 ] && [ "$ITERATION" -gt "$MAX_ITERATIONS" ]; then
    echo "=== Reached max iterations ($MAX_ITERATIONS), stopping ==="
    break
  fi

  echo "=== Iteration $ITERATION ($(date '+%H:%M:%S')) ==="

  claude \
    --model opus \
    --dangerously-skip-permissions \
    --verbose \
    --output-format stream-json \
    ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} \
    -p "$(cat "$PROMPT_FILE")" | tee -a corpus-fix-loop-$(date +%Y-%m-%dT%T%z).log

  # Check if a commit was produced recently (within last 5 minutes)
  LAST_COMMIT=$(git log -1 --format=%ct 2>/dev/null || echo 0)
  NOW=$(date +%s)
  ELAPSED=$((NOW - LAST_COMMIT))

  if [ "$ELAPSED" -gt 300 ]; then
    echo ""
    echo "=== No commit in last 5 minutes ($ELAPSED seconds ago), stopping ==="
    echo "=== Completed $ITERATION iterations ==="
    break
  fi

  echo ""
  echo "=== Iteration $ITERATION complete, commit was ${ELAPSED}s ago ==="
  echo ""
done

echo ""
echo "Final corpus stats:"
if [ -f target/corpus-report.json ]; then
  python3 -c "
import json
data = json.load(open('target/corpus-report.json'))
s = data['summary']
print(f\"  Passed: {s['total_passed']}/{s['total_tests']} ({s['total_passed']/s['total_tests']*100:.1f}%)\")
print(f\"  Failed: {s['total_failed']}\")
"
fi
