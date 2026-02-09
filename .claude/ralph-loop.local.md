---
active: true
iteration: 3
max_iterations: 10
completion_promise: "All important SQL syntax is handled"
started_at: "2026-02-09T00:39:35Z"
---

Run tests/sqlparser_corpus.rs and see which files faile to parse. Check if the SQL is a valid one or if it fails correctly. When SQL looks valid, and the parser should support it, add support for such new syntax and restart the test. Make sure we are not regressing. Any new supported features should not only parse (as in tokens) but as well build AST to allow writing it back to SQL. We should rely on the parser -> ast -> sql -> parser to yield more or less same result. In some places we expect very specific order of parameters in the SQL, that is not always the case and we should rather be more flexible allowing any order of operators if they do not cause new parse errors. Make sure to use proper oneof to encode the data. Create new enums where needed and remember in them what was parsed.
