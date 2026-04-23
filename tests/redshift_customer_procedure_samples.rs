//! Regression tests for Redshift stored procedures and UDFs collected
//! from real customer warehouses. All identifiers (schemas, tables,
//! columns, function names, literals) have been anonymized, but the
//! SQL shapes — dollar-quoted bodies, plpgsql control flow, plpython,
//! refcursors, RAISE EXCEPTION — are preserved so that regressions in
//! how we tokenize or parse these constructs will fail loudly.

use sqlparser::ast::{FunctionDefinition, Statement};
use sqlparser::dialect::RedshiftSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Tokenizer};

/// Parse preserving token spans — [`Parser::parse_sql`] strips locations
/// via [`Tokenizer::tokenize`] for backwards compatibility, which zeros out
/// every `Span`. Tests that assert on source positions must feed the parser
/// via `tokenize_with_location` + `with_tokens_with_locations`.
fn parse_sql_with_locations(sql: &str) -> Vec<Statement> {
    let dialect = RedshiftSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .unwrap();
    Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .unwrap()
}

fn assert_parses(sql: &str) {
    Parser::parse_sql(&RedshiftSqlDialect {}, sql)
        .unwrap_or_else(|e| panic!("parse failed: {e}\nSQL:\n{sql}"));
}

#[test]
fn redshift_plpgsql_procedure_preserves_body_definition() {
    // The parser cannot walk plpgsql, but it must preserve the raw dollar-
    // quoted body string in `CreateProcedure.body_definition` so downstream
    // consumers (lineage extractors) can re-parse it.
    let sql = r#"CREATE OR REPLACE PROCEDURE analytics.load_daily()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO analytics.daily SELECT * FROM staging.daily;
END;
$$;"#;
    let mut stmts = Parser::parse_sql(&RedshiftSqlDialect {}, sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match stmts.pop().unwrap() {
        Statement::CreateProcedure {
            body,
            body_definition,
            ..
        } => {
            assert!(
                body.is_empty(),
                "plpgsql body is opaque, body should be empty"
            );
            let def = body_definition.expect("body_definition should carry the $$..$$ string");
            let raw = match def {
                FunctionDefinition::DoubleDollarDef { value, .. } => value,
                other => panic!("expected DoubleDollarDef, got {other:?}"),
            };
            assert!(raw.contains("INSERT INTO analytics.daily"));
            assert!(raw.contains("FROM staging.daily"));
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn function_definition_records_body_start_for_dollar_quoted_body() {
    // Body starts at the first character after the opening `$$`. For
    //   line 3:  `AS $$\n`
    //   line 4:  `BEGIN\n`
    // the opening `$$` begins at column 4 on line 3, so `body_start` must
    // point at line 3, column 6 (the newline immediately after `$$`).
    let sql = "CREATE OR REPLACE PROCEDURE analytics.load()
LANGUAGE plpgsql
AS $$
BEGIN
    NULL;
END;
$$;";
    let mut stmts = parse_sql_with_locations(sql);
    let def = match stmts.pop().unwrap() {
        Statement::CreateProcedure {
            body_definition: Some(def),
            ..
        } => def,
        other => panic!("expected CreateProcedure with body_definition, got {other:?}"),
    };
    match def {
        FunctionDefinition::DoubleDollarDef { value, body_start } => {
            assert!(value.contains("BEGIN"));
            assert_eq!(
                body_start,
                Location { line: 3, column: 6 },
                "body_start should point at the first character after `AS $$`"
            );
        }
        other => panic!("expected DoubleDollarDef, got {other:?}"),
    }
}

#[test]
fn function_definition_records_body_start_for_tagged_dollar_body() {
    // `$tag$` opening is `tag.len() + 2` chars wide. For `AS $body$...$body$;`
    // on line 1, the `$body$` opening starts at column 16 (the `$`), so the
    // body's first character is at column 16 + len("body") + 2 = 22.
    let sql =
        "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $body$ BEGIN RETURN 1; END; $body$;";
    let mut stmts = parse_sql_with_locations(sql);
    let def = match stmts.pop().unwrap() {
        Statement::CreateFunction { params, .. } => params.as_.expect("as_ must be set"),
        other => panic!("expected CreateFunction, got {other:?}"),
    };
    match def {
        FunctionDefinition::DoubleDollarDef { body_start, .. } => {
            // Column of the opening `$` of `$body$` = 54 (1-based). Opening
            // delimiter length = 6. body_start column = 54 + 6 = 60.
            let open_col = sql.find("$body$").unwrap() as u64 + 1;
            let expected_col = open_col + ("body".len() as u64) + 2;
            assert_eq!(
                body_start,
                Location {
                    line: 1,
                    column: expected_col
                }
            );
        }
        other => panic!("expected DoubleDollarDef, got {other:?}"),
    }
}

#[test]
fn function_definition_records_body_start_for_single_quoted_body() {
    // Opening `'` at column 70; body starts at column 71.
    let sql =
        "CREATE OR REPLACE PROCEDURE analytics.noop() LANGUAGE plpgsql AS 'BEGIN NULL; END;';";
    let mut stmts = parse_sql_with_locations(sql);
    let def = match stmts.pop().unwrap() {
        Statement::CreateProcedure {
            body_definition: Some(def),
            ..
        } => def,
        other => panic!("expected CreateProcedure with body_definition, got {other:?}"),
    };
    match def {
        FunctionDefinition::SingleQuotedDef { body_start, .. } => {
            let open_col = sql.find('\'').unwrap() as u64 + 1;
            assert_eq!(
                body_start,
                Location {
                    line: 1,
                    column: open_col + 1
                }
            );
        }
        other => panic!("expected SingleQuotedDef, got {other:?}"),
    }
}

#[test]
fn redshift_plpgsql_procedure_single_quoted_body_preserved() {
    // Single-quoted body variant — rarer but legal in PostgreSQL/Redshift.
    let sql =
        "CREATE OR REPLACE PROCEDURE analytics.noop() LANGUAGE plpgsql AS 'BEGIN NULL; END;';";
    let mut stmts = Parser::parse_sql(&RedshiftSqlDialect {}, sql).unwrap();
    match stmts.pop().unwrap() {
        Statement::CreateProcedure {
            body_definition: Some(FunctionDefinition::SingleQuotedDef { value, .. }),
            ..
        } => {
            assert!(value.contains("BEGIN"));
        }
        other => panic!("expected CreateProcedure with SingleQuotedDef, got {other:?}"),
    }
}

#[test]
fn redshift_sql_function_with_dollar_body() {
    // SQL function: positional args, STABLE, interval arithmetic inside a dollar-quoted body.
    let sql = r#"CREATE OR REPLACE FUNCTION util.fn_epoch_to_ts(in_unix int8, denom int4)
RETURNS timestamp
LANGUAGE sql
STABLE
AS $$
SELECT timestamp 'epoch' + $1/$2::bigint * interval '1 second'
$$;"#;
    assert_parses(sql);
}

#[test]
fn redshift_plpython_function_with_return_statement() {
    // plpythonu UDF: Python source (with `return` and `.format()`) lives inside
    // a dollar-quoted body. The parser must treat it as an opaque string.
    let sql = r#"CREATE OR REPLACE FUNCTION contact_centre.f_format(q varchar, run_id varchar, src_filter varchar, tgt_filter varchar)
RETURNS varchar
LANGUAGE plpythonu
STABLE
AS $$
return q.format(run_id, src_filter, tgt_filter)
$$;"#;
    assert_parses(sql);
}

#[test]
fn redshift_plpgsql_procedure_with_raise_exception() {
    // plpgsql procedure: IF / THEN / RAISE EXCEPTION / END IF inside a
    // dollar-quoted body.
    let sql = r#"CREATE OR REPLACE PROCEDURE analytics.check_duplicates()
LANGUAGE plpgsql
AS $$
BEGIN
IF EXISTS (
    SELECT pk_col, count(*)
    FROM analytics.source_table
    GROUP BY pk_col
    HAVING count(*) > 0
)
THEN
    RAISE EXCEPTION 'Error : There are duplicate PK_COL in source data.';
END IF;
END;
$$;"#;
    assert_parses(sql);
}

#[test]
fn redshift_plpgsql_procedure_with_for_in_execute_loop() {
    // plpgsql procedure with DECLARE, dynamic SQL via EXECUTE, and a
    // `FOR record IN EXECUTE query LOOP ... END LOOP` block.
    let sql = r#"CREATE OR REPLACE PROCEDURE admin.sp_grant_select(schema_name varchar, group_name varchar)
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    query text;
BEGIN
    query := 'SELECT usename FROM admin.v_user_schema_privileges WHERE schemaname = '
             || quote_literal(schema_name)
             || ' AND user_has_create_permission = TRUE';

    FOR rec IN EXECUTE query
    LOOP
        EXECUTE 'ALTER DEFAULT PRIVILEGES FOR USER '
                || quote_ident(rec.usename)
                || ' IN SCHEMA '
                || schema_name
                || ' GRANT SELECT ON TABLES TO GROUP '
                || group_name;
    END LOOP;
END;
$$;"#;
    assert_parses(sql);
}

#[test]
fn redshift_plpgsql_procedure_with_inout_refcursor() {
    // plpgsql procedure returning a refcursor via OPEN ... FOR SELECT.
    let sql = r#"CREATE OR REPLACE PROCEDURE analytics.check_duplicates_cursor(INOUT rs_out refcursor)
LANGUAGE plpgsql
AS $$
BEGIN
IF EXISTS (
    SELECT pk_col, count(*)
    FROM analytics.source_table
    GROUP BY pk_col
    HAVING count(*) > 1
)
THEN
    RAISE EXCEPTION 'Error : There are duplicate PK_COL in source data.';
END IF;
OPEN rs_out FOR SELECT 'OK' AS res;
END;
$$;"#;
    assert_parses(sql);
}
