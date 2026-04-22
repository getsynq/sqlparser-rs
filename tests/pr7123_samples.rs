//! Regression tests for PR-7123: `CREATE PROCEDURE` / `CREATE FUNCTION`
//! samples collected from real workspace query logs (Snowflake, BigQuery,
//! Redshift). Each test asserts the statement parses into the expected
//! AST shape — not just that parsing succeeds — so future refactors that
//! silently drop modes, names, or arg types will fail loudly.

use sqlparser::ast::{ArgMode, DataType, ObjectName, Statement};
use sqlparser::dialect::*;
use sqlparser::parser::Parser;

fn parse_one(dialect: &dyn Dialect, sql: &str) -> Statement {
    let mut stmts = Parser::parse_sql(dialect, sql).expect("parse");
    assert_eq!(stmts.len(), 1, "expected exactly one statement");
    stmts.pop().unwrap()
}

fn object_name(parts: &[&str]) -> ObjectName {
    ObjectName(parts.iter().map(|p| (*p).into()).collect())
}

#[test]
fn snowflake_sql_udf_with_empty_varchar_and_comment() {
    // Sample 1: `returns varchar()` (empty parens — Snowflake-ism), `immutable`,
    // `comment = '...'`, single-quoted body with doubled `''` escapes.
    let sql = r#"create or replace function utils.make_guid(luid integer, region_id integer)
    returns varchar()
    immutable
    comment = 'Construct a GUID.'
    as 'select concat(region_id, ''_'', luid)'"#;

    let stmt = parse_one(&SnowflakeDialect {}, sql);
    match stmt {
        Statement::CreateFunction {
            or_replace,
            name,
            args,
            return_type,
            ..
        } => {
            assert!(or_replace);
            assert_eq!(name, object_name(&["utils", "make_guid"]));
            let args = args.expect("function args");
            assert_eq!(args.len(), 2);
            assert_eq!(args[0].name.as_ref().unwrap().value, "luid");
            assert_eq!(args[1].name.as_ref().unwrap().value, "region_id");
            // Empty `varchar()` should parse as Varchar with no length.
            assert!(matches!(return_type, Some(DataType::Varchar(None))));
        }
        other => panic!("expected CreateFunction, got {other:?}"),
    }
}

#[test]
fn snowflake_procedure_with_execute_as_caller() {
    // Sample 2: CREATE PROCEDURE with LANGUAGE SQL, STRICT, EXECUTE AS CALLER
    // and single-quoted body containing nested DDL. Parser must not misclassify
    // the whole statement as CREATE_TABLE because of the inner DDL.
    let sql = r#"CREATE OR REPLACE PROCEDURE analytics.sp_refresh_current_inventory()
RETURNS VARCHAR
LANGUAGE SQL
STRICT
EXECUTE AS CALLER
AS '
begin
CREATE OR REPLACE TEMPORARY TABLE t_stage AS
SELECT 1 AS x;

INSERT INTO analytics.tbl_current_inventory
SELECT * FROM t_stage;

RETURN ''ok'';
end
'"#;

    let stmt = parse_one(&SnowflakeDialect {}, sql);
    match stmt {
        Statement::CreateProcedure { name, params, .. } => {
            assert_eq!(
                name,
                object_name(&["analytics", "sp_refresh_current_inventory"])
            );
            assert_eq!(params.as_deref(), Some([].as_slice()));
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn bigquery_scalar_udf_with_parenthesised_body() {
    // Sample 3: BigQuery `AS (expr)` expression body, regex string prefix.
    let sql = r#"CREATE OR REPLACE FUNCTION utils.prettify_string(var_name STRING)
RETURNS STRING
AS (
  TRIM(REGEXP_REPLACE(var_name, r'\s+', ' '))
)"#;

    let stmt = parse_one(&BigQueryDialect {}, sql);
    match stmt {
        Statement::CreateFunction {
            name,
            args,
            return_type,
            ..
        } => {
            assert_eq!(name, object_name(&["utils", "prettify_string"]));
            let args = args.expect("function args");
            assert_eq!(args.len(), 1);
            assert_eq!(args[0].name.as_ref().unwrap().value, "var_name");
            assert!(matches!(
                return_type,
                Some(DataType::String(_) | DataType::Varchar(_))
            ));
        }
        other => panic!("expected CreateFunction, got {other:?}"),
    }
}

#[test]
fn redshift_python_udf_trailing_language() {
    // Sample 4: Redshift plpythonu — language clause is at the **tail** after
    // the dollar-quoted body, not the head. Volatility (STABLE) precedes body.
    let sql = r#"CREATE OR REPLACE FUNCTION models.f_distinct_numbers(input VARCHAR)
    RETURNS VARCHAR
    STABLE
AS
$$
    result = ''
    seen = []
    return result
$$ LANGUAGE plpythonu"#;

    let stmt = parse_one(&RedshiftSqlDialect {}, sql);
    match stmt {
        Statement::CreateFunction { name, args, .. } => {
            assert_eq!(name, object_name(&["models", "f_distinct_numbers"]));
            assert_eq!(args.as_ref().unwrap().len(), 1);
        }
        other => panic!("expected CreateFunction, got {other:?}"),
    }
}

#[test]
fn redshift_procedure_with_out_parameter_modes() {
    // Sample 5: Redshift plpgsql with IN/OUT mixed parameters. Without
    // parse_procedure_param honouring IN/OUT/INOUT, this used to fail with
    // "Expected ',' or ')' after parameter definition, found: int4".
    let sql = r#"CREATE OR REPLACE PROCEDURE credit_analytics.sp_insert_borrowing_main_new_loans(
    in_job_run_id varchar,
    OUT out_inserted_records int4,
    OUT out_updated_records int4,
    OUT out_deleted_records int4
)
LANGUAGE plpgsql
AS $$
DECLARE
    row_count integer;
BEGIN
    out_inserted_records := 0;
    DELETE FROM credit_analytics.tbl_borrowing_main
    WHERE brw_pk IN (SELECT brw_pk FROM staging.tbl_bm_scope);

    INSERT INTO credit_analytics.tbl_borrowing_main
    SELECT * FROM staging.tbl_bm_scope;
END;
$$"#;

    let stmt = parse_one(&RedshiftSqlDialect {}, sql);
    match stmt {
        Statement::CreateProcedure { name, params, .. } => {
            assert_eq!(
                name,
                object_name(&["credit_analytics", "sp_insert_borrowing_main_new_loans"])
            );
            let params = params.expect("params");
            assert_eq!(params.len(), 4);
            assert_eq!(params[0].mode, None);
            assert_eq!(params[0].name.value, "in_job_run_id");
            assert_eq!(params[1].mode, Some(ArgMode::Out));
            assert_eq!(params[1].name.value, "out_inserted_records");
            assert_eq!(params[2].mode, Some(ArgMode::Out));
            assert_eq!(params[3].mode, Some(ArgMode::Out));
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn procedure_with_inout_argument_mode() {
    // Common Redshift refcursor return pattern.
    let sql = r#"CREATE OR REPLACE PROCEDURE foo.bar(INOUT rs refcursor, IN job_id varchar)
LANGUAGE plpgsql AS $$ BEGIN SELECT 1; END; $$"#;

    let stmt = parse_one(&RedshiftSqlDialect {}, sql);
    match stmt {
        Statement::CreateProcedure { params, .. } => {
            let params = params.unwrap();
            assert_eq!(params[0].mode, Some(ArgMode::InOut));
            assert_eq!(params[0].name.value, "rs");
            assert_eq!(params[1].mode, Some(ArgMode::In));
            assert_eq!(params[1].name.value, "job_id");
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn procedure_param_with_default_value() {
    // Snowflake stored procedures allow `DEFAULT <expr>` on parameters.
    let sql = "CREATE OR REPLACE PROCEDURE foo.bar(\"A\" VARCHAR, \"B\" VARCHAR DEFAULT 'x', \"C\" NUMBER DEFAULT 1) RETURNS VARCHAR LANGUAGE SQL AS 'return 1'";
    let stmt = parse_one(&SnowflakeDialect {}, sql);
    match stmt {
        Statement::CreateProcedure { params, .. } => {
            let params = params.unwrap();
            assert_eq!(params.len(), 3);
            assert!(params[0].default_expr.is_none());
            assert!(params[1].default_expr.is_some());
            assert!(params[2].default_expr.is_some());
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn snowflake_varchar_empty_parens_as_column_type() {
    // Guardrails: the `VARCHAR()` fix should also hold outside of CREATE FUNCTION.
    let stmt = parse_one(&SnowflakeDialect {}, "CREATE TABLE t (c VARCHAR())");
    match stmt {
        Statement::CreateTable { columns, .. } => {
            assert_eq!(columns.len(), 1);
            assert!(matches!(columns[0].data_type, DataType::Varchar(None)));
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }
}
