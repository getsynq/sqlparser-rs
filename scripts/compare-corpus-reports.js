#!/usr/bin/env node
/**
 * Compare two corpus test reports and generate a markdown summary.
 *
 * Usage:
 *   node scripts/compare-corpus-reports.js <current-report.json> [baseline-report.json]
 *
 * If baseline is omitted, only shows current stats without comparison.
 */

const fs = require('fs');
const path = require('path');

function loadReport(filePath) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(content);
  } catch (e) {
    console.error(`Error loading ${filePath}: ${e.message}`);
    return null;
  }
}

function generateReport(current, baseline = null) {
  let body = '## Corpus Parsing Report\n\n';

  const totalTests = current.summary.total_tests;
  const passRate = (current.summary.total_passed / totalTests * 100).toFixed(1);
  body += `**Total: ${current.summary.total_passed} passed, ${current.summary.total_failed} failed** (${passRate}% pass rate)\n\n`;

  let hasBaseline = baseline !== null;
  let regressions = [];
  let improvements = [];
  let removed = [];
  let added = [];

  if (hasBaseline) {
    // Detect changes by comparing individual test results
    const baselineTests = baseline.test_results || {};
    const currentTests = current.test_results || {};

    for (const [test, status] of Object.entries(currentTests)) {
      if (baselineTests[test] === 'pass' && status === 'fail') {
        regressions.push(test);
      } else if (baselineTests[test] === 'fail' && status === 'pass') {
        improvements.push(test);
      } else if (!baselineTests[test]) {
        added.push(test);
      }
    }

    for (const test of Object.keys(baselineTests)) {
      if (!currentTests[test]) {
        removed.push(test);
      }
    }

    // Show regressions and improvements summary
    if (regressions.length > 0 || improvements.length > 0 || removed.length > 0 || added.length > 0) {
      body += '### Changes\n\n';
      if (regressions.length > 0) {
        body += `❌ **${regressions.length} regression(s)** (now failing)\n`;
      }
      if (improvements.length > 0) {
        body += `✅ **${improvements.length} improvement(s)** (now passing)\n`;
      }
      if (added.length > 0) {
        body += `➕ **${added.length} new test(s)**\n`;
      }
      if (removed.length > 0) {
        body += `➖ **${removed.length} removed test(s)**\n`;
      }
      body += '\n';
    } else {
      body += '**✨ No changes in test results**\n\n';
    }
  }

  // Per-dialect table
  body += '### By Dialect\n\n';
  body += '| Dialect | Passed | Failed | Total | Pass Rate |';
  if (hasBaseline) body += ' Delta |';
  body += '\n';

  body += '|---------|-------:|-------:|------:|----------:|';
  if (hasBaseline) body += '------:|';
  body += '\n';

  for (const [dialect, stats] of Object.entries(current.by_dialect)) {
    const total = stats.passed + stats.failed;
    const rate = (stats.passed / total * 100).toFixed(1);
    body += `| ${dialect} | ${stats.passed} | ${stats.failed} | ${total} | ${rate}% |`;

    if (hasBaseline && baseline.by_dialect && baseline.by_dialect[dialect]) {
      const base = baseline.by_dialect[dialect];
      const diff = stats.passed - base.passed;
      if (diff > 0) body += ` **+${diff}** |`;
      else if (diff < 0) body += ` **${diff}** |`;
      else body += ` - |`;
    } else if (hasBaseline) {
      body += ` *new* |`;
    }

    body += '\n';
  }

  // Show details for regressions and improvements
  if (hasBaseline) {
    const maxShow = 10;

    if (regressions.length > 0) {
      body += '\n<details>\n<summary>❌ Regressions (' + regressions.length + ')</summary>\n\n';
      body += '```\n';
      for (const test of regressions.slice(0, maxShow)) {
        body += test + '\n';
      }
      if (regressions.length > maxShow) {
        body += `... and ${regressions.length - maxShow} more\n`;
      }
      body += '```\n</details>\n';
    }

    if (improvements.length > 0) {
      body += '\n<details>\n<summary>✅ Improvements (' + improvements.length + ')</summary>\n\n';
      body += '```\n';
      for (const test of improvements.slice(0, maxShow)) {
        body += test + '\n';
      }
      if (improvements.length > maxShow) {
        body += `... and ${improvements.length - maxShow} more\n`;
      }
      body += '```\n</details>\n';
    }

    if (added.length > 0) {
      body += '\n<details>\n<summary>➕ New Tests (' + added.length + ')</summary>\n\n';
      body += '```\n';
      for (const test of added.slice(0, maxShow)) {
        body += test + '\n';
      }
      if (added.length > maxShow) {
        body += `... and ${added.length - maxShow} more\n`;
      }
      body += '```\n</details>\n';
    }

    if (removed.length > 0) {
      body += '\n<details>\n<summary>➖ Removed Tests (' + removed.length + ')</summary>\n\n';
      body += '```\n';
      for (const test of removed.slice(0, maxShow)) {
        body += test + '\n';
      }
      if (removed.length > maxShow) {
        body += `... and ${removed.length - maxShow} more\n`;
      }
      body += '```\n</details>\n';
    }
  } else {
    body += '\n*No baseline available for comparison*\n';
  }

  return body;
}

function main() {
  const args = process.argv.slice(2);

  if (args.length < 1) {
    console.error('Usage: node compare-corpus-reports.js <current-report.json> [baseline-report.json]');
    process.exit(1);
  }

  const currentPath = args[0];
  const baselinePath = args[1];

  const current = loadReport(currentPath);
  if (!current) {
    console.error('Failed to load current report');
    process.exit(1);
  }

  const baseline = baselinePath ? loadReport(baselinePath) : null;

  const report = generateReport(current, baseline);
  console.log(report);
}

if (require.main === module) {
  main();
}

// Export for use in GitHub Actions
module.exports = { generateReport, loadReport };
