//! Regression tests for Redshift stored procedures and UDFs collected
//! from real customer warehouses. All identifiers (schemas, tables,
//! columns, function names, literals) have been anonymized, but the
//! SQL shapes — dollar-quoted bodies, plpgsql control flow, plpython,
//! refcursors, RAISE EXCEPTION — are preserved so that regressions in
//! how we tokenize or parse these constructs will fail loudly.

use sqlparser::ast::{FunctionDefinition, Statement};
use sqlparser::dialect::RedshiftSqlDialect;
use sqlparser::parser::Parser;

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
            assert!(body.is_empty(), "plpgsql body is opaque, body should be empty");
            let def = body_definition.expect("body_definition should carry the $$..$$ string");
            let raw = match def {
                FunctionDefinition::DoubleDollarDef(s) => s,
                other => panic!("expected DoubleDollarDef, got {other:?}"),
            };
            assert!(raw.contains("INSERT INTO analytics.daily"));
            assert!(raw.contains("FROM staging.daily"));
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn redshift_plpgsql_procedure_single_quoted_body_preserved() {
    // Single-quoted body variant — rarer but legal in PostgreSQL/Redshift.
    let sql = "CREATE OR REPLACE PROCEDURE analytics.noop() LANGUAGE plpgsql AS 'BEGIN NULL; END;';";
    let mut stmts = Parser::parse_sql(&RedshiftSqlDialect {}, sql).unwrap();
    match stmts.pop().unwrap() {
        Statement::CreateProcedure {
            body_definition: Some(FunctionDefinition::SingleQuotedDef(s)),
            ..
        } => {
            assert!(s.contains("BEGIN"));
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
