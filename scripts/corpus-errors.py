#!/usr/bin/env python3
"""Analyze corpus runner error patterns.

Usage:
    # Show top 20 error patterns with 2 example files each (default)
    python3 scripts/corpus-errors.py

    # Show top 10 patterns with 3 examples each
    python3 scripts/corpus-errors.py -n 10 -e 3

    # Show all patterns with 5 examples
    python3 scripts/corpus-errors.py -n 0 -e 5

    # Filter by dialect
    python3 scripts/corpus-errors.py -d clickhouse

    # Filter by error message substring
    python3 scripts/corpus-errors.py -f "found: AS"

    # Show file content for examples
    python3 scripts/corpus-errors.py -f "found: AS" -n 3 --show-content

    # Use a specific report file
    python3 scripts/corpus-errors.py -r path/to/corpus-report.json
"""

import argparse
import collections
import json
import os
import re
import sys


def main():
    parser = argparse.ArgumentParser(description="Analyze corpus runner error patterns")
    parser.add_argument("-r", "--report", default="target/corpus-report.json",
                        help="Path to corpus-report.json (default: target/corpus-report.json)")
    parser.add_argument("-n", "--num-patterns", type=int, default=20,
                        help="Number of error patterns to show (0 = all, default: 20)")
    parser.add_argument("-e", "--examples", type=int, default=2,
                        help="Number of example files per pattern (default: 2)")
    parser.add_argument("-d", "--dialect", default=None,
                        help="Filter by dialect (e.g., clickhouse, snowflake)")
    parser.add_argument("-f", "--filter", default=None,
                        help="Filter by error message substring")
    parser.add_argument("--show-content", action="store_true",
                        help="Show first few lines of example SQL files")
    parser.add_argument("--content-lines", type=int, default=5,
                        help="Number of SQL lines to show per example (default: 5)")
    args = parser.parse_args()

    if not os.path.exists(args.report):
        print(f"Error: Report file not found: {args.report}")
        print("Run: target/release/corpus-runner tests/corpus --errors")
        sys.exit(1)

    with open(args.report) as f:
        data = json.load(f)

    # Collect errors
    errors = collections.defaultdict(lambda: {"total": 0, "dialects": collections.Counter(), "files": []})

    for path, result in data.get("test_results", {}).items():
        if not result.startswith("fail: "):
            continue

        msg = result[6:]
        # Normalize: strip line/col info
        msg = re.sub(r" at Line: \d+, Column: \d+", "", msg)
        # Strip "Failed to parse: sql parser error: " prefix
        msg = re.sub(r"^Failed to parse: sql parser error: ", "", msg)

        # Extract dialect from path like 'bigquery/_blocks/xxx.sql'
        parts = path.split("/")
        dialect = re.sub(r"^customer_", "", parts[0]) if parts else "unknown"

        # Apply filters
        if args.dialect and dialect != args.dialect:
            continue
        if args.filter and args.filter.lower() not in msg.lower():
            continue

        errors[msg]["total"] += 1
        errors[msg]["dialects"][dialect] += 1
        errors[msg]["files"].append((dialect, path))

    if not errors:
        print("No matching errors found.")
        sys.exit(0)

    # Sort by count
    sorted_errors = sorted(errors.items(), key=lambda x: -x[1]["total"])
    if args.num_patterns > 0:
        sorted_errors = sorted_errors[:args.num_patterns]

    total_failures = sum(info["total"] for _, info in sorted_errors)
    all_failures = sum(info["total"] for _, info in errors.items())

    print(f"Total matching failures: {all_failures}")
    if args.num_patterns > 0:
        print(f"Showing top {len(sorted_errors)} patterns ({total_failures} failures)")
    print()

    for msg, info in sorted_errors:
        dialects_str = ", ".join(f"{d}:{c}" for d, c in info["dialects"].most_common(6))
        print(f"  {info['total']:4d}  [{dialects_str}]")
        print(f"        {msg[:120]}")

        # Show examples
        shown = 0
        for dialect, path in info["files"]:
            if shown >= args.examples:
                break
            print(f"        -> tests/corpus/{path}")
            if args.show_content:
                full_path = f"tests/corpus/{path}"
                if os.path.exists(full_path):
                    with open(full_path) as sf:
                        lines = sf.readlines()
                    # Skip comment lines
                    sql_lines = [l for l in lines if not l.strip().startswith("--")]
                    for line in sql_lines[:args.content_lines]:
                        print(f"           | {line.rstrip()[:150]}")
            shown += 1
        print()


if __name__ == "__main__":
    main()
