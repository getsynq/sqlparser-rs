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

#[macro_use]
mod test_utils;

use test_utils::*;

use sqlparser::ast::*;
use sqlparser::dialect::RedshiftSqlDialect;
use sqlparser::parser::ParserOptions;

#[test]
fn test_square_brackets_over_db_schema_table_name() {
    let select = redshift().verified_only_select("SELECT [col1] FROM [test_schema].[test_table]");
    assert_eq!(
        select.projection[0].clone().unwrap(),
        SelectItem::UnnamedExpr(
            Expr::Identifier(
                Ident {
                    value: "col1".to_string(),
                    quote_style: Some('[')
                }
                .empty_span()
            )
            .empty_span()
        ),
    );
    assert_eq!(
        select.from[0],
        TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![
                    Ident {
                        value: "test_schema".to_string(),
                        quote_style: Some('[')
                    },
                    Ident {
                        value: "test_table".to_string(),
                        quote_style: Some('[')
                    }
                ]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        }
    );
}

#[test]
fn brackets_over_db_schema_table_name_with_whites_paces() {
    match redshift().parse_sql_statements("SELECT [   col1  ] FROM [  test_schema].[ test_table]") {
        Ok(statements) => {
            assert_eq!(statements.len(), 1);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_double_quotes_over_db_schema_table_name() {
    let select =
        redshift().verified_only_select("SELECT \"col1\" FROM \"test_schema\".\"test_table\"");
    assert_eq!(
        select.projection[0].clone().unwrap(),
        SelectItem::UnnamedExpr(
            Expr::Identifier(
                Ident {
                    value: "col1".to_string(),
                    quote_style: Some('"')
                }
                .empty_span()
            )
            .empty_span()
        ),
    );
    assert_eq!(
        select.from[0],
        TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![
                    Ident {
                        value: "test_schema".to_string(),
                        quote_style: Some('"')
                    },
                    Ident {
                        value: "test_table".to_string(),
                        quote_style: Some('"')
                    }
                ]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: None,
                partitions: vec![],
            },
            joins: vec![],
        }
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = redshift().verified_only_select(
        r#"SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias""#,
    );
    // check FROM
    match only(select.from).relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            partitions: _,
        } => {
            assert_eq!(vec![Ident::with_quote('"', "a table")], name.0);
            assert_eq!(
                Ident::with_quote('"', "alias").empty_span(),
                alias.unwrap().name
            );
            assert!(args.is_none());
            assert!(with_hints.is_empty());
            assert!(version.is_none());
        }
        _ => panic!("Expecting TableFactor::Table"),
    }
    // check SELECT
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::CompoundIdentifier(
            vec![
                Ident::with_quote('"', "alias"),
                Ident::with_quote('"', "bar baz"),
            ]
            .empty_span()
        ),
        expr_from_projection(&select.projection[0]),
    );
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::with_quote('"', "myfun")]),
            args: vec![],
            parameters: None,
            over: None,
            distinct: false,
            special: false,
            order_by: vec![],
            limit: None,
            on_overflow: None,
            null_treatment: None,
            within_group: None,
        }),
        expr_from_projection(&select.projection[1]),
    );
    match &select.projection[2].clone().unwrap() {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert_eq!(
                &Expr::Identifier(Ident::with_quote('"', "simple id").empty_span()).empty_span(),
                expr
            );
            assert_eq!(&Ident::with_quote('"', "column alias").empty_span(), alias);
        }
        _ => panic!("Expected ExprWithAlias"),
    }

    redshift().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    redshift().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = redshift().verified_only_select(sql);
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            }
            .empty_span(),
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            r#"SELECT * FROM customers WHERE name {}LIKE '%a' ESCAPE '\\'"#,
            if negated { "NOT " } else { "" }
        );
        let select = redshift().verified_only_select_with_canonical(sql, "");
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }
            .empty_span(),
            select.selection.unwrap()
        );

        // This statement tests that LIKE and NOT LIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            r#"SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL"#,
            if negated { "NOT " } else { "" }
        );
        let select = redshift().verified_only_select_with_canonical(sql, "");
        assert_eq!(
            Expr::IsNull(Box::new(Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            }))
            .empty_span(),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn parse_similar_to() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = redshift().verified_only_select(sql);
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: None,
            }
            .empty_span(),
            select.selection.unwrap()
        );

        // Test with escape char
        let sql = &format!(
            r#"SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\'"#,
            if negated { "NOT " } else { "" }
        );
        let select = redshift().verified_only_select_with_canonical(sql, "");
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }
            .empty_span(),
            select.selection.unwrap()
        );

        // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
        let sql = &format!(
            r#"SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\' IS NULL"#,
            if negated { "NOT " } else { "" }
        );
        let select = redshift().verified_only_select_with_canonical(sql, "");
        assert_eq!(
            Expr::IsNull(Box::new(Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }))
            .empty_span(),
            select.selection.unwrap()
        );
    }
    chk(false);
    chk(true);
}

fn redshift() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(RedshiftSqlDialect {})],
        options: None,
    }
}

fn redshift_unescaped() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(RedshiftSqlDialect {})],
        options: Some(ParserOptions::new().with_unescape(false)),
    }
}

#[test]
fn test_sharp() {
    let sql = "SELECT #_of_values";
    let select = redshift().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(
            Expr::Identifier(Ident::new("#_of_values").empty_span()).empty_span()
        ),
        select.projection[0].clone().unwrap(),
    );
}

#[test]
fn test_select_ignore_nulls() {
    redshift().verified_stmt("SELECT last_value(b) IGNORE NULLS FROM test_data");
}

#[test]
fn test_materialized_view_auto_refresh() {
    redshift().verified_stmt(r#"CREATE MATERIALIZED VIEW view AUTO REFRESH YES AS (SELECT MAX("events"."derived_tstamp") AS "aggvar_2" FROM "atomic"."events" AS "events")"#);
    redshift().verified_stmt(r#"CREATE MATERIALIZED VIEW view AUTO REFRESH NO AS (SELECT MAX("events"."derived_tstamp") AS "aggvar_2" FROM "atomic"."events" AS "events")"#);
}

#[test]
fn parse_create_materialized_view_with_redshift_clauses() {
    // BACKUP and DISTSTYLE are skipped during parsing
    redshift().one_statement_parses_to(
        "CREATE MATERIALIZED VIEW mv BACKUP YES DISTSTYLE EVEN AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv AS SELECT 1",
    );

    // All Redshift-specific clauses combined; BACKUP/DISTSTYLE/DISTKEY/SORTKEY are skipped,
    // AUTO REFRESH is preserved in the AST
    redshift().one_statement_parses_to(
        "CREATE MATERIALIZED VIEW mv BACKUP NO DISTSTYLE KEY DISTKEY (col1) SORTKEY (col1, col2) AUTO REFRESH YES AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv AUTO REFRESH YES AS SELECT 1",
    );

    // INTERLEAVED SORTKEY
    redshift().one_statement_parses_to(
        "CREATE MATERIALIZED VIEW mv BACKUP YES DISTSTYLE EVEN INTERLEAVED SORTKEY (col1, col2) AUTO REFRESH NO AS SELECT 1",
        "CREATE MATERIALIZED VIEW mv AUTO REFRESH NO AS SELECT 1",
    );
}

#[test]
fn test_create_view_late_binding() {
    redshift()
        .verified_stmt("CREATE VIEW myevent AS SELECT eventname FROM event WITH NO SCHEMA BINDING");
}

#[test]
fn parse_within_group() {
    redshift().verified_only_select(r#"SELECT SOMEAGGFUNC(sellerid, ', ') WITHIN GROUP (ORDER BY sellerid) FROM sales WHERE eventid = 4337"#);
}

#[test]
fn parse_trim() {
    redshift().verified_only_select("SELECT TRIM(col, ' ') FROM tbl");
    redshift().verified_only_select("SELECT TRIM('    dog    ')");
    redshift().one_statement_parses_to(
        "SELECT TRIM(BOTH FROM '    dog    ')",
        "SELECT TRIM(BOTH '    dog    ')",
    );
    redshift().verified_only_select(r#"SELECT TRIM(LEADING '"' FROM '"dog"')"#);
    redshift().verified_only_select(r#"SELECT TRIM(TRAILING '"' FROM '"dog"')"#);
    redshift().verified_only_select(r#"SELECT TRIM('CDG' FROM venuename)"#);
}

#[test]
fn parse_listagg() {
    redshift().verified_only_select(r#"SELECT LISTAGG(a.attname, '|') WITHIN GROUP (ORDER BY a.attsortkeyord) OVER (PARTITION BY n.nspname, c.relname) AS sort_keys FROM bar"#);
}

#[test]
fn parse_quoted_identifier() {
    redshift().verified_only_select(r#"SELECT 'foo' AS "123_col""#);
}

#[test]
fn test_escape_string() {
    redshift_unescaped().verified_stmt(r"SELECT 'I\'m fine'");
    redshift_unescaped().verified_stmt(r"SELECT 'I\\\'m fine'");
    redshift_unescaped().verified_stmt(r#"SELECT 'I''m fine'"#);
    redshift_unescaped().verified_stmt(r#"SELECT 'I\\\'m fine'"#);
    redshift_unescaped().verified_stmt(r#"SELECT '[\'\\[\\]]'"#);
}

#[test]
fn test_distribution_styles() {
    let sql = "CREATE TABLE foo (id VARCHAR(32)) DISTSTYLE KEY DISTKEY(id) COMPOUND SORTKEY(id)";
    redshift().verified_stmt(sql);
}

#[test]
fn test_extract_with_string_date_part() {
    // Redshift allows date parts as string literals in EXTRACT
    let sql = "SELECT EXTRACT('hour' FROM col) FROM tbl";
    redshift().parse_sql_statements(sql).unwrap();

    let sql = "SELECT EXTRACT('year' FROM col) FROM tbl";
    redshift().parse_sql_statements(sql).unwrap();

    let sql = "SELECT EXTRACT('day' FROM col) FROM tbl";
    redshift().parse_sql_statements(sql).unwrap();

    let sql = "SELECT EXTRACT('minute' FROM col) FROM tbl";
    redshift().parse_sql_statements(sql).unwrap();

    let sql = "SELECT EXTRACT('second' FROM col) FROM tbl";
    redshift().parse_sql_statements(sql).unwrap();

    let sql = "SELECT EXTRACT('quarter' FROM col) FROM tbl";
    redshift().parse_sql_statements(sql).unwrap();
}

#[test]
fn test_utf8_column_names() {
    redshift().verified_stmt("SELECT financing_cost_€k FROM tbl");
}

#[test]
fn test_redshift_unpivot_super() {
    // Basic UNPIVOT with AS and AT
    redshift().verified_stmt(
        "SELECT attr, val FROM customer_orders_lineitem AS c, UNPIVOT c.c_orders AS val AT attr WHERE c_custkey = 9451",
    );

    // UNPIVOT with array subscript
    redshift().verified_stmt(
        "SELECT attr, val FROM customer_orders_lineitem AS c, UNPIVOT c.c_orders[0] WHERE c_custkey = 9451",
    );

    // UNPIVOT without AT clause
    redshift().one_statement_parses_to(
        "SELECT * FROM tbl AS t, UNPIVOT t.col AS val",
        "SELECT * FROM tbl AS t, UNPIVOT t.col AS val",
    );

    // UNPIVOT with AS and AT in complex query
    redshift().verified_stmt(
        "SELECT col_1, col_2, SPLIT_PART(col_12, '_', 2) AS currency_code FROM tbl_1 AS tbl_1, UNPIVOT tbl_2.col_14 AS val AT attr",
    );

    // UNPIVOT with CTE
    redshift().verified_stmt(
        "WITH cte AS (SELECT col_1, col_2 FROM tbl_5) SELECT col_1, col_2 FROM tbl_2 AS tbl_2, UNPIVOT tbl_1.col_6 AS carrier_logic AT carrier_id",
    );
}

#[test]
fn test_create_database_collate() {
    let sql = "CREATE DATABASE sampledb COLLATE case_insensitive";
    match redshift().verified_stmt(sql) {
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
            collation,
            ..
        } => {
            assert_eq!("sampledb", db_name.to_string());
            assert!(!if_not_exists);
            assert_eq!(Some("case_insensitive".to_string()), collation);
        }
        _ => unreachable!(),
    }

    // Also works with case_sensitive
    redshift().verified_stmt("CREATE DATABASE mydb COLLATE case_sensitive");
}

#[test]
fn test_grant_with_group_grantee() {
    // GRANT ... TO GROUP name
    redshift().verified_stmt("GRANT ALL ON SCHEMA qa_tickit TO GROUP qa_users");

    // GRANT ... TO multiple GROUP grantees (ON TABLE serializes as ON)
    redshift().one_statement_parses_to(
        "GRANT ALL ON TABLE qa_tickit.sales TO GROUP qa_users, GROUP ro_users",
        "GRANT ALL ON qa_tickit.sales TO GROUP qa_users, GROUP ro_users",
    );

    // GRANT with column-level privileges
    redshift().verified_stmt(
        "GRANT SELECT (cust_name, cust_phone), UPDATE (cust_contact_preference) ON cust_profile TO GROUP sales_group",
    );

    // GRANT with column-level privileges (no space before parentheses)
    redshift().one_statement_parses_to(
        "GRANT SELECT(cust_name, cust_phone), UPDATE(cust_contact_preference) ON cust_profile TO GROUP sales_group",
        "GRANT SELECT (cust_name, cust_phone), UPDATE (cust_contact_preference) ON cust_profile TO GROUP sales_group",
    );

    // Verify AST structure
    match redshift().verified_stmt("GRANT ALL ON SCHEMA qa_tickit TO GROUP qa_users") {
        Statement::Grant {
            grantees,
            ..
        } => {
            assert_eq!(1, grantees.len());
            assert_eq!(Some(GranteesType::Group), grantees[0].grantee_type);
            assert_eq!("qa_users", grantees[0].name.value);
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_revoke_with_group_grantee() {
    // With explicit RESTRICT
    redshift()
        .verified_stmt("REVOKE ALL ON SCHEMA qa_tickit FROM GROUP qa_users RESTRICT");

    // Without CASCADE/RESTRICT (defaults to RESTRICT in output)
    redshift().one_statement_parses_to(
        "REVOKE ALL ON SCHEMA qa_tickit FROM GROUP qa_users",
        "REVOKE ALL ON SCHEMA qa_tickit FROM GROUP qa_users RESTRICT",
    );
}

#[test]
fn test_minus_as_except() {
    // MINUS is a Redshift/Oracle synonym for EXCEPT
    redshift().one_statement_parses_to(
        "SELECT foo, bar FROM table_1 MINUS SELECT foo, bar FROM table_2",
        "SELECT foo, bar FROM table_1 EXCEPT SELECT foo, bar FROM table_2",
    );
}
