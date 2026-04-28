// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![warn(clippy::all)]
//! Test SQL syntax specific to Snowflake. The parser based on the
//! generic dialect is also tested (on the inputs it can handle).

#[cfg(test)]
use pretty_assertions::assert_eq;
use sqlparser::ast::*;
use sqlparser::dialect::{DatabricksDialect, GenericDialect};
use sqlparser::parser::ParserOptions;
use test_utils::*;

#[macro_use]
mod test_utils;

fn databricks() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DatabricksDialect {})],
        options: None,
    }
}

fn databricks_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DatabricksDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

fn databricks_unescaped() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DatabricksDialect {})],
        options: Some(ParserOptions::new().with_unescape(false)),
    }
}

#[test]
fn test_databricks_create_table() {
    let sql = "CREATE TABLE main.dbt_lukasz.customers (customer_id BIGINT, customer_lifetime_value DOUBLE) USING delta TBLPROPERTIES ('delta.minReaderVersion' = '3', 'delta.minWriterVersion' = '7')";
    match databricks_and_generic().verified_stmt(sql) {
        Statement::CreateTable { name, .. } => {
            assert_eq!("main.dbt_lukasz.customers", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_identifiers() {
    let sql = "SELECT * FROM `main`.`dbt_lukasz`.`raw_orders`";
    databricks().verified_stmt(sql);
}

#[test]
fn test_div_operator() {
    databricks().verified_stmt("SELECT 5 DIV 2");
    databricks().verified_stmt("SELECT -5.9 DIV 1");
    databricks().verified_stmt("SELECT INTERVAL '100' HOUR DIV INTERVAL '1' DAY");
}

#[test]
fn test_numeric_literal_type_suffixes() {
    // Spark/Databricks numeric literal suffixes: L, S, Y, F, D, BD.
    // The suffix signals the literal's SQL type; for lineage purposes we
    // only need the numeric value to parse successfully.
    databricks().one_statement_parses_to("SELECT 5.0D", "SELECT 5.0");
    databricks().one_statement_parses_to("SELECT 4.0D, 5.0D", "SELECT 4.0, 5.0");
    databricks().one_statement_parses_to(
        "SELECT ARRAY(0.45D, -0.35D, 0.78D)",
        "SELECT ARRAY(0.45, -0.35, 0.78)",
    );
    databricks().one_statement_parses_to("SELECT 1F", "SELECT 1");
    databricks().one_statement_parses_to("SELECT 1S", "SELECT 1");
    databricks().one_statement_parses_to("SELECT 1Y", "SELECT 1");
    databricks().one_statement_parses_to("SELECT 1BD", "SELECT 1");
}

#[test]
fn test_string_escape() {
    databricks().one_statement_parses_to(r#"SELECT 'O\'Connell'"#, r#"SELECT 'O''Connell'"#);
}

#[test]
fn test_string_raw_literal() {
    let sql = r#"SELECT R'Some\nText'"#;
    databricks_unescaped().verified_stmt(sql);
}

#[test]
fn test_rlike() {
    let sql = r#"SELECT R'%SystemDrive%\Users\John' RLIKE R'%SystemDrive%\\Users.*'"#;
    databricks_unescaped().verified_stmt(sql);
}

#[test]
fn test_create_table_comment_tblproperties() {
    let sql = "CREATE TABLE main.dbt_cloud_lukasz.customers (customer_id BIGINT COMMENT 'Customer Unique identifier', first_name STRING, last_name STRING) USING delta TBLPROPERTIES ('delta.checkpoint.writeStatsAsJson' = 'false') COMMENT 'The ''customers'' table.'";

    databricks_unescaped().verified_stmt(sql);
}

#[test]
fn test_select_star_except() {
    let sql = "SELECT * EXCEPT (c2) FROM tbl";

    databricks().verified_stmt(sql);
}

#[test]
fn test_create_table_partitioned_by_as() {
    let sql = "CREATE TABLE logs PARTITIONED BY (datepart) AS SELECT 1";

    databricks().verified_stmt(sql);
}

#[test]
fn test_create_table_map_type() {
    let sql = "CREATE TABLE logs (info MAP<STRING, ARRAY<STRING>>)";

    databricks().verified_stmt(sql);
}

#[test]
fn test_select_placeholder() {
    let sql = "SELECT {{param}}";

    databricks().verified_stmt(sql);
}

#[test]
fn test_select_dollar_brace_placeholder() {
    let sql = "SELECT ${x} FROM ${y} WHERE ${z} > 1";
    databricks().verified_stmt(sql);
}

#[test]
fn test_underscore_column_name() {
    databricks().verified_stmt("SELECT _column FROM `myproject`.`mydataset`.`mytable`");

    databricks().verified_stmt("SELECT other AS _column FROM `myproject`.`mydataset`.`mytable`");
}

#[test]
fn test_create_table_column_mask() {
    databricks().verified_stmt("CREATE TABLE persons (name STRING, ssn STRING MASK mask_ssn)");
}

#[test]
fn test_cte_columns() {
    databricks()
        .verified_stmt("WITH t (x, y) AS (SELECT 1, 2) SELECT * FROM t WHERE x = 1 AND y = 2");
}

#[test]
fn test_cte_no_as() {
    databricks().one_statement_parses_to(
        "WITH foo (SELECT 'bar' as baz) SELECT * FROM foo",
        "WITH foo AS (SELECT 'bar' AS baz) SELECT * FROM foo",
    );
    databricks().one_statement_parses_to(
        "WITH foo (WITH b (SELECT * FROM bb) SELECT 'bar' as baz FROM b) SELECT * FROM foo",
        "WITH foo AS (WITH b AS (SELECT * FROM bb) SELECT 'bar' AS baz FROM b) SELECT * FROM foo",
    );
}

#[test]
fn test_create_or_replace_temporary_function_returns_expression() {
    databricks().verified_stmt(
        "CREATE OR REPLACE TEMPORARY FUNCTION GG_Account_ID RETURN '0F98682E-005D-43A9-A5EC-464E8AC478C9'",
    );
    databricks()
        .verified_stmt("CREATE FUNCTION area(x DOUBLE, y DOUBLE) RETURNS DOUBLE RETURN x * y");
    databricks().verified_stmt("CREATE FUNCTION square(x DOUBLE) RETURNS DOUBLE RETURN area(x, x)");
}

#[test]
fn test_create_or_replace_temporary_function_returns_select() {
    databricks().verified_stmt(
        "CREATE FUNCTION avg_score(p INT) RETURNS FLOAT COMMENT 'get an average score of the player' RETURN SELECT AVG(score) FROM scores WHERE player = p",
    );
}

#[test]
fn test_struct_literal() {
    databricks().verified_stmt(
        "SELECT STRUCT(loan_app.bank_statement_regular_income_alimony AS alimony, loan_app.bank_statement_regular_income_pension AS pension, loan_app.bank_statement_regular_income_salary AS salary) AS bank_statement_regular_income FROM loan_app",
    );
}

#[test]
fn test_create_table_struct_column() {
    databricks().verified_stmt(
        "CREATE TABLE foo (bank_statement_regular_income STRUCT<alimony: DECIMAL(19,5), pension: DECIMAL(19,5), salary: DECIMAL(19,5)>)",
    );
}

#[test]
fn test_functions_without_parens() {
    databricks().verified_expr("current_timestamp");
    databricks().verified_expr("current_timestamp()");
    databricks().verified_expr("current_date");
    databricks().verified_expr("current_date()");
    databricks().verified_expr("now()");
    databricks().verified_expr("current_timezone()");
}

#[test]
fn test_parse_literal_array() {
    databricks().verified_stmt("SELECT array(current_date, current_date)");
}

#[test]
fn test_parse_substring() {
    databricks().one_statement_parses_to(
        "SELECT SUBSTR('Spark SQL', 5)",
        "SELECT SUBSTRING('Spark SQL' FROM 5)",
    );
    databricks().one_statement_parses_to(
        "SELECT SUBSTR('Spark SQL' FROM 5 FOR 1)",
        "SELECT SUBSTRING('Spark SQL' FROM 5 FOR 1)",
    );
    databricks().one_statement_parses_to(
        "SELECT SUBSTRING('Spark SQL', 5)",
        "SELECT SUBSTRING('Spark SQL' FROM 5)",
    );
    databricks().verified_expr("SUBSTRING('Spark SQL' FROM 5)");
    databricks().verified_expr("SUBSTRING('Spark SQL' FROM 5 FOR 1)");
}

#[test]
fn test_array_access() {
    databricks()
        .verified_stmt("SELECT id, extra_questions[0] AS question, FROM AS detailed_survey");
}

#[test]
fn test_array_access_paren() {
    databricks().one_statement_parses_to(
        "SELECT id, extra_questions[(0)] AS question, FROM AS detailed_survey",
        "SELECT id, extra_questions[0] AS question, FROM AS detailed_survey",
    );
}

#[test]
fn test_array_struct_access() {
    databricks()
        .verified_stmt("SELECT id, extra_questions[0].label AS question, FROM AS detailed_survey");
}

#[test]
fn test_create_table_with_location() {
    // Databricks: CREATE TABLE with LOCATION and TBLPROPERTIES in various orders
    databricks().verified_stmt("CREATE TABLE t (c1 INT) LOCATION 's3://bucket/path'");
    databricks()
        .verified_stmt("CREATE TABLE t (c1 INT) COMMENT 'test' LOCATION 's3://bucket/path'");
    databricks().verified_stmt(
        "CREATE TABLE t (c1 INT) LOCATION 's3://bucket/path' TBLPROPERTIES ('k1' = 'v1')",
    );
}

#[test]
fn test_alter_table_set_tblproperties() {
    databricks_and_generic().verified_stmt(
        "ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name', 'delta.enableDeletionVectors' = 'true')",
    );
}

#[test]
fn test_cross_join() {
    databricks_and_generic().verified_stmt("SELECT * FROM tbl CROSS JOIN tbl2 ON tbl.id = tbl2.id");
}

#[test]
fn test_describe_history() {
    // Simple table name
    match databricks().verified_stmt("DESCRIBE HISTORY table_name") {
        Statement::DescribeHistory { table_name } => {
            assert_eq!(table_name.to_string(), "table_name");
        }
        _ => unreachable!(),
    }

    // Qualified table name
    match databricks().verified_stmt("DESCRIBE HISTORY a.b") {
        Statement::DescribeHistory { table_name } => {
            assert_eq!(table_name.to_string(), "a.b");
        }
        _ => unreachable!(),
    }

    // DESC alias
    databricks().one_statement_parses_to("DESC HISTORY my_table", "DESCRIBE HISTORY my_table");
}

#[test]
fn test_json_path_with_colon() {
    // Backtick-quoted field names with spaces
    databricks().one_statement_parses_to(
        "SELECT raw:`zip code`, raw:`fb:testid` FROM t",
        "SELECT raw:`zip code`, raw:`fb:testid` FROM t",
    );

    // Bracket notation with single-quoted strings
    databricks().one_statement_parses_to(
        "SELECT raw:store['bicycle'], raw:store[\"zip code\"] FROM t",
        "SELECT raw:store['bicycle'], raw:store[\"zip code\"] FROM t",
    );

    // Combined: all syntax variants from the corpus test
    databricks().one_statement_parses_to(
        "SELECT raw:`zip code`, raw:`fb:testid`, raw:store['bicycle'], raw:store[\"zip code\"] FROM t",
        "SELECT raw:`zip code`, raw:`fb:testid`, raw:store['bicycle'], raw:store[\"zip code\"] FROM t",
    );

    // Bracket notation directly after colon
    databricks().one_statement_parses_to(
        "SELECT c1:['price'] FROM VALUES ('{ \"price\": 5 }') AS T(c1)",
        "SELECT c1:['price'] FROM (VALUES ('{ \"price\": 5 }')) AS T (c1)",
    );

    // Wildcard array subscript [*] in JSON path
    databricks().one_statement_parses_to(
        "SELECT c1:item[*].price FROM VALUES ('{ \"item\": [ { \"model\" : \"basic\", \"price\" : 6.12 } ] }') AS T(c1)",
        "SELECT c1:item[*].price FROM (VALUES ('{ \"item\": [ { \"model\" : \"basic\", \"price\" : 6.12 } ] }')) AS T (c1)",
    );

    // Wildcard array subscript used inside function arguments
    databricks().one_statement_parses_to(
        "SELECT INLINE(FROM_JSON(c1:item[*], 'ARRAY<STRUCT<model STRING, price DOUBLE>>')) FROM VALUES ('{}') AS T(c1)",
        "SELECT INLINE(FROM_JSON(c1:item[*], 'ARRAY<STRUCT<model STRING, price DOUBLE>>')) FROM (VALUES ('{}')) AS T (c1)",
    );

    // Wildcard array subscript followed by further subscript access
    databricks().one_statement_parses_to(
        "SELECT FROM_JSON(c1:item[*].price, 'ARRAY<DOUBLE>')[0] FROM VALUES ('{}') AS T(c1)",
        "SELECT FROM_JSON(c1:item[*].price, 'ARRAY<DOUBLE>')[0] FROM (VALUES ('{}')) AS T (c1)",
    );
}

#[test]
fn test_try_cast_operator() {
    // ?:: is a try cast operator in Databricks, equivalent to TRY_CAST
    databricks()
        .one_statement_parses_to("SELECT '20'?::INTEGER", "SELECT TRY_CAST('20' AS INTEGER)");

    // Chaining with regular cast
    databricks().one_statement_parses_to("SELECT col?::VARCHAR", "SELECT TRY_CAST(col AS VARCHAR)");
}

#[test]
fn test_named_parameter_placeholder() {
    // Databricks {:name} parameter syntax
    let sql = "SELECT * FROM tbl WHERE col = {:store} LIMIT 10";
    databricks_and_generic().one_statement_parses_to(sql, sql);
}

#[test]
fn test_create_function_dollar_quoted() {
    // Databricks CREATE FUNCTION with $$...$$ body
    let sql = "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $$def add_one(x):\n  return x+1$$";
    databricks().one_statement_parses_to(
        sql,
        "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $$def add_one(x):\n  return x+1$$",
    );
}

#[test]
fn test_create_function_tagged_dollar_quoted() {
    // Databricks CREATE FUNCTION with $TAG$...$TAG$ body
    let sql = "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $FOO$def add_one(x):\n  return x+1$FOO$";
    databricks().one_statement_parses_to(
        sql,
        "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $$def add_one(x):\n  return x+1$$",
    );
}

#[test]
fn test_filter_during_aggregation_in_expressions() {
    // FILTER (WHERE ...) used in arithmetic expressions (e.g. division)
    // Parser accepts both FILTER( and FILTER (, Display normalizes to space before (
    databricks().one_statement_parses_to(
        "SELECT SUM(col1) FILTER(WHERE col2) / SUM(col1) AS pct FROM tbl",
        "SELECT SUM(col1) FILTER (WHERE col2) / SUM(col1) AS pct FROM tbl",
    );

    // FILTER (WHERE ...) in a simple aggregate - roundtrips correctly
    let sql = "SELECT COUNT(DISTINCT col1) FILTER (WHERE col2 AND col3) AS cnt FROM tbl";
    databricks().verified_stmt(sql);
}

#[test]
fn test_alter_table_add_column_after() {
    // Databricks supports ADD COLUMN ... AFTER col_name
    let sql = "ALTER TABLE db.schema.tbl ADD COLUMN new_col INT AFTER existing_col";
    databricks().verified_stmt(sql);
}

#[test]
fn test_from_stream_modifier() {
    // Databricks structured streaming: `FROM STREAM table_name` reads the
    // source as a streaming table. The STREAM keyword is consumed; the
    // underlying table reference is preserved in the AST for lineage.
    databricks_and_generic().one_statement_parses_to(
        "SELECT a FROM STREAM db.sch.tbl",
        "SELECT a FROM db.sch.tbl",
    );
    databricks_and_generic().one_statement_parses_to(
        "SELECT a FROM STREAM db.sch.tbl AS t",
        "SELECT a FROM db.sch.tbl AS t",
    );
}

#[test]
fn test_parenthesized_bare_table() {
    // Databricks permits extra parentheses around a lone table reference,
    // e.g. `FROM (db.sch.tbl)`. The extra parens are not preserved in the AST.
    databricks().one_statement_parses_to(
        "SELECT c1, c2 FROM (`db_1`.`sch_1`.tbl_1)",
        "SELECT c1, c2 FROM `db_1`.`sch_1`.tbl_1",
    );
}

#[test]
fn test_explode_multi_alias() {
    // Databricks/Spark generator function with multiple column aliases
    // EXPLODE(col) AS (key, value) - returns multiple columns
    let sql = "SELECT EXPLODE_OUTER(col_6) AS (season, balance) FROM tbl";
    databricks().verified_stmt(sql);

    // Also works with EXPLODE
    let sql2 = "SELECT col_1, EXPLODE(map_col) AS (k, v), col_2 FROM tbl";
    databricks().verified_stmt(sql2);
}

#[test]
fn test_alter_table_add_columns_plural() {
    // Databricks/Hive: ADD COLUMNS (col type, ...) [CASCADE]
    databricks().verified_stmt("ALTER TABLE `db_1`.sch_1.tbl_1 ADD COLUMNS (test_column STRING)");
    databricks()
        .verified_stmt("ALTER TABLE tbl_1 ADD COLUMNS (a INT, b STRING, c TIMESTAMP) CASCADE");
}

#[test]
fn test_alter_table_drop_columns_plural() {
    // Databricks: DROP COLUMNS (c1, c2, ...)
    databricks().verified_stmt("ALTER TABLE `db_1`.sch_1.tbl_1 DROP COLUMNS (c_name)");
    databricks().verified_stmt("ALTER TABLE tbl_1 DROP COLUMNS IF EXISTS (a, b) CASCADE");
}

#[test]
fn test_extract_custom_date_part() {
    // Databricks supports EXTRACT with date parts like YEAROFWEEK and WEEKOFYEAR
    // that are not reserved keywords. Parse them as custom date/time fields.
    databricks().verified_stmt("SELECT EXTRACT(YEAROFWEEK FROM col) FROM tbl");
}

#[test]
fn test_exists_higher_order_function() {
    // Spark/Databricks overload EXISTS as a higher-order function that takes
    // an array and a lambda predicate. When the parenthesised body does not
    // start a subquery, parse it as a regular function call.
    databricks().verified_stmt("SELECT EXISTS(arr, x -> x > 0) FROM tbl");
    databricks().verified_stmt("SELECT NOT EXISTS(arr, x -> x > 0) FROM tbl");
    // First argument may be an ARRAY literal built via keyword constructors.
    databricks().verified_stmt("SELECT EXISTS(ARRAY(1, 2, 3), x -> x IS NULL)");
    databricks().verified_stmt("SELECT EXISTS(ARRAY(1, 2, 3), x -> x % 2 = 10)");
    // Subquery form still works.
    databricks().verified_stmt("SELECT 1 FROM tbl WHERE EXISTS (SELECT 1 FROM u)");
    databricks().verified_stmt("SELECT 1 FROM tbl WHERE NOT EXISTS (SELECT 1 FROM u)");
}

#[test]
fn test_cast_interval_with_qualifier() {
    // Databricks/Spark supports qualified INTERVAL types in CAST:
    //   INTERVAL DAY TO SECOND, INTERVAL YEAR TO MONTH, INTERVAL DAY, etc.
    databricks().verified_stmt("SELECT CAST('11 23:4:0' AS INTERVAL DAY TO SECOND)");
    databricks().verified_stmt("SELECT CAST('1' AS INTERVAL DAY TO MINUTE)");
    databricks().verified_stmt("SELECT CAST('1-6' AS INTERVAL YEAR TO MONTH)");
    databricks().verified_stmt("SELECT CAST('1' AS INTERVAL DAY)");
}

#[test]
fn test_insert_by_name() {
    // Databricks supports `INSERT INTO target BY NAME <query>` where columns
    // are matched by name rather than by position.
    databricks().verified_stmt("INSERT INTO target BY NAME SELECT 1 AS a, 2 AS b");
    // Combined with REPLACE WHERE predicate.
    databricks().verified_stmt(
        "INSERT INTO sales BY NAME REPLACE WHERE tx_date = '2022-10-01' SELECT 1 AS amount",
    );
}

#[test]
fn test_pivot_multi_column_for() {
    // Databricks supports multi-column PIVOT: the FOR clause may reference a
    // parenthesised list of columns whose values are matched against tuples in
    // the IN clause.
    databricks().verified_stmt(
        "SELECT * FROM sales PIVOT(SUM(amount) AS sum_score, AVG(amount) AS avg_score \
         FOR (subject, quarter) IN (('Math', 'Q1') AS q1_math, ('English', 'Q1') AS q1_english))",
    );
    // Single-column form still works unchanged.
    databricks()
        .verified_stmt("SELECT * FROM sales PIVOT(SUM(amount) FOR quarter IN ('Q1', 'Q2'))");
}

#[test]
fn test_execute_immediate_using_with_alias() {
    // Databricks supports named parameter markers (`:name`) bound via
    // `USING <expr> AS <name>`. The alias must be preserved in the AST.
    databricks_and_generic().verified_stmt("EXECUTE IMMEDIATE 'SELECT :val AS c1' USING 10 AS val");
    databricks_and_generic()
        .verified_stmt("EXECUTE IMMEDIATE 'SELECT 1::DECIMAL(:p, :s)' USING 6 AS p, 4 AS s");
    // Mixed aliased and unaliased arguments still parse.
    databricks_and_generic().verified_stmt("EXECUTE IMMEDIATE 'SELECT ?, :n' USING 1, 2 AS n");
}

#[test]
fn test_sort_by_with_direction_and_nulls() {
    // Databricks supports ASC/DESC and NULLS FIRST/LAST on SORT BY expressions.
    databricks().verified_stmt("SELECT age, name, zip_code FROM person SORT BY age DESC");
    databricks()
        .verified_stmt("SELECT age, name, zip_code FROM person SORT BY age DESC NULLS FIRST");
    databricks().verified_stmt("SELECT a FROM t SORT BY a ASC, b DESC NULLS LAST");
}

#[test]
fn test_declare_variable() {
    // Databricks SQL Scripting variable declaration:
    // DECLARE [OR REPLACE] [VARIABLE] name [type] [DEFAULT expr]
    // https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-var-declare.html
    let cases = [
        "DECLARE state STRING",
        "DECLARE argstr STRING",
        "DECLARE line BIGINT",
        "DECLARE args MAP<STRING, STRING>",
        "DECLARE OR REPLACE VARIABLE x INT DEFAULT 5",
        "DECLARE VARIABLE y DEFAULT 1",
        "DECLARE z DEFAULT 'hello'",
    ];
    for sql in cases {
        databricks()
            .parse_sql_statements(sql)
            .unwrap_or_else(|e| panic!("failed to parse {sql:?}: {e}"));
    }
}
