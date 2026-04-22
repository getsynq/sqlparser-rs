//! Regression test for parser-error hot path performance.
//!
//! Background: `ParserErrorMessage` can capture a Rust backtrace at the point
//! an error is created. Originally this was keyed off `RUST_BACKTRACE`, which
//! callers commonly set in production for panic diagnostics — causing every
//! speculative parser error to eagerly capture + symbolicate + stringify a
//! full backtrace. In one real production profile that code path accounted
//! for ~90% of CPU on kernel-cll.
//!
//! The fix has three parts:
//!   1. Capture is gated on a dedicated `SQLPARSER_BACKTRACE` env var so
//!      production `RUST_BACKTRACE=full` does not trigger it.
//!   2. `Parser::maybe_parse` / `try_parse_*` mark a speculative scope; errors
//!      constructed inside are discarded, so capture is suppressed there even
//!      when the dedicated env var is set.
//!   3. The captured `Backtrace` is stored as-is (lazy symbol resolution)
//!      instead of eagerly `.to_string()`-ified.
//!
//! This test pins the observable invariants.

use std::time::Instant;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError, ParserErrorMessage};

#[test]
fn parser_error_message_does_not_capture_by_default() {
    let msg = ParserErrorMessage::from("boom");
    assert!(
        msg.backtrace().is_none(),
        "no backtrace should be captured unless SQLPARSER_BACKTRACE is set"
    );
}

#[test]
fn parse_error_from_parser_has_no_backtrace_by_default() {
    let dialect = GenericDialect {};
    let err = Parser::parse_sql(&dialect, "SELECT * FROM t WHERE x =").unwrap_err();
    match err {
        ParserError::ParserError(msg) => {
            assert!(
                msg.backtrace().is_none(),
                "parser error should not carry a backtrace unless explicitly enabled"
            );
        }
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[test]
fn many_parser_errors_are_cheap() {
    // Typical speculative parser path: SQL that triggers many internal
    // try-parse failures before settling on a result.
    let sqls = [
        "SELECT * FROM t WHERE x = (SELECT y FROM z WHERE",
        "CREATE TABLE foo (",
        "WITH cte AS (SELECT",
        "SELECT 1 +",
        "INSERT INTO t VALUES (1, 2,",
    ];
    let dialect = GenericDialect {};

    let start = Instant::now();
    for _ in 0..2_000 {
        for sql in &sqls {
            let _ = Parser::parse_sql(&dialect, sql);
        }
    }
    let elapsed = start.elapsed();

    // With the old eager-backtrace code path this loop took multiple seconds
    // (sometimes tens of seconds) when RUST_BACKTRACE was set. With the fix
    // it's a few hundred ms even on loaded CI. Leave generous headroom.
    assert!(
        elapsed.as_secs() < 5,
        "parsing 10k erroring SQLs took {elapsed:?}; regression suspected"
    );
}
