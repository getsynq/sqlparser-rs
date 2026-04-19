// Smoke tests for PR-7123 samples: CREATE PROCEDURE / FUNCTION across dialects.
use sqlparser::dialect::*;
use sqlparser::parser::Parser;

fn try_parse(name: &str, dialect: &dyn sqlparser::dialect::Dialect, sql: &str) -> bool {
    match Parser::parse_sql(dialect, sql) {
        Ok(stmts) => {
            println!("=== {name}: OK ({} stmts)", stmts.len());
            true
        }
        Err(e) => {
            println!("=== {name}: ERR: {e}");
            false
        }
    }
}

#[test]
fn sample_1_snowflake_udf() {
    let sql = r#"create or replace function utils.make_guid(luid integer, region_id integer)
    returns varchar()
    immutable
    comment = 'Construct a GUID.'
    as 'select concat(region_id, ''_'', luid)'"#;
    assert!(try_parse("snowflake udf", &SnowflakeDialect {}, sql));
}

#[test]
fn sample_2_snowflake_procedure() {
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
    assert!(try_parse("snowflake proc", &SnowflakeDialect {}, sql));
}

#[test]
fn sample_3_bigquery_udf() {
    let sql = r#"CREATE OR REPLACE FUNCTION utils.prettify_string(var_name STRING)
RETURNS STRING
AS (
  TRIM(REGEXP_REPLACE(var_name, r'\s+', ' '))
)"#;
    assert!(try_parse("bigquery udf", &BigQueryDialect {}, sql));
}

#[test]
fn sample_4_redshift_python_udf() {
    let sql = r#"CREATE OR REPLACE FUNCTION models.f_distinct_numbers(input VARCHAR)
    RETURNS VARCHAR
    STABLE
AS
$$
    result = ''
    seen = []
    return result
$$ LANGUAGE plpythonu"#;
    assert!(try_parse("redshift python udf", &RedshiftSqlDialect {}, sql));
}

#[test]
fn sample_5_redshift_procedure() {
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
    assert!(try_parse("redshift proc", &RedshiftSqlDialect {}, sql));
}
