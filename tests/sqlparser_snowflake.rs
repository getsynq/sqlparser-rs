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
use sqlparser::ast::helpers::stmt_data_loading::{
    DataLoadingOption, DataLoadingOptionType, StageLoadSelectItem,
};
use sqlparser::ast::Expr::MapAccess;
use sqlparser::ast::*;
use sqlparser::dialect::{GenericDialect, SnowflakeDialect};
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::*;
use test_utils::*;

#[macro_use]
mod test_utils;

#[test]
fn test_snowflake_create_table() {
    let sql = "CREATE TABLE _my_$table (am00unt number)";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable { name, .. } => {
            assert_eq!("_my_$table", name.to_string());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_create_transient_table() {
    let sql = "CREATE TRANSIENT TABLE CUSTOMER (id INT, name VARCHAR(255))";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::CreateTable {
            name, transient, ..
        } => {
            assert_eq!("CUSTOMER", name.to_string());
            assert!(transient)
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_single_line_tokenize() {
    let sql = "CREATE TABLE# this is a comment \ntable_1";
    let dialect = SnowflakeDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "#".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);

    let sql = "CREATE TABLE // this is a comment \ntable_1";
    let tokens = Tokenizer::new(&dialect, sql).tokenize().unwrap();

    let expected = vec![
        Token::make_keyword("CREATE"),
        Token::Whitespace(Whitespace::Space),
        Token::make_keyword("TABLE"),
        Token::Whitespace(Whitespace::Space),
        Token::Whitespace(Whitespace::SingleLineComment {
            prefix: "//".to_string(),
            comment: " this is a comment \n".to_string(),
        }),
        Token::make_word("table_1", None),
    ];

    assert_eq!(expected, tokens);
}

#[test]
fn test_sf_derived_table_in_parenthesis() {
    // Nesting a subquery in an extra set of parentheses is non-standard,
    // but supported in Snowflake SQL
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM ((SELECT 1) AS t)",
        "SELECT * FROM (SELECT 1) AS t",
    );
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (((SELECT 1) AS t))",
        "SELECT * FROM (SELECT 1) AS t",
    );
}

#[test]
fn test_single_table_in_parenthesis() {
    // Parenthesized table names are non-standard, but supported in Snowflake SQL
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN (b))",
        "SELECT * FROM (a NATURAL JOIN b)",
    );
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ((b)))",
        "SELECT * FROM (a NATURAL JOIN b)",
    );
}

#[test]
fn test_single_table_in_parenthesis_with_alias() {
    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN (b) c )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ((b)) c )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ( (b) c ) )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN ( (b) as c ) )",
        "SELECT * FROM (a NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a alias1 NATURAL JOIN ( (b) c ) )",
        "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a as alias1 NATURAL JOIN ( (b) as c ) )",
        "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT * FROM (a NATURAL JOIN b) c",
        "SELECT * FROM (a NATURAL JOIN b) AS c",
    );

    let res = snowflake().parse_sql_statements("SELECT * FROM (a b) c");
    assert_eq!(
        ParserError::ParserError("duplicate alias b".to_string().into()),
        res.unwrap_err()
    );
}

#[test]
fn parse_array() {
    let sql = "SELECT CAST(a AS ARRAY) FROM customer";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        &Expr::Cast {
            expr: Box::new(Expr::Identifier(Ident::new("a").empty_span())),
            data_type: DataType::Array(ArrayElemTypeDef::None),
            format: None,
        },
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_lateral_flatten() {
    snowflake().verified_only_select(r#"SELECT * FROM TABLE(FLATTEN(input => parse_json('{"a":1, "b":[77,88]}'), outer => true)) AS f"#);
    snowflake().verified_only_select(r#"SELECT emp.employee_ID, emp.last_name, index, value AS project_name FROM employees AS emp, LATERAL FLATTEN(INPUT => emp.project_names) AS proj_names"#);
}

#[test]
fn parse_lateral_table_function_parens() {
    // Snowflake allows wrapping a TABLE(func(...)) call in an extra pair of
    // parens after LATERAL. Accept it and unwrap to a plain TABLE(...) factor.
    snowflake().one_statement_parses_to(
        "SELECT * FROM t, LATERAL (TABLE(my_func(t.col)))",
        "SELECT * FROM t, TABLE(my_func(t.col))",
    );
}

#[test]
fn parse_within_group() {
    snowflake().verified_only_select(r#"SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY ride_duration) AS median_ride_duration FROM rides"#);
}

#[test]
fn parse_json_using_colon() {
    let sql = "SELECT a:b FROM t";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(
            Expr::JsonAccess {
                left: Box::new(Expr::Identifier(Ident::new("a").empty_span())),
                operator: JsonOperator::Colon,
                right: Box::new(Expr::Value(Value::UnQuotedString("b".to_string()))),
            }
            .empty_span()
        )
        .empty_span(),
        select.projection[0]
    );

    let sql = "SELECT a:type FROM t";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(
            Expr::JsonAccess {
                left: Box::new(Expr::Identifier(Ident::new("a").empty_span())),
                operator: JsonOperator::Colon,
                right: Box::new(Expr::Value(Value::UnQuotedString("type".to_string()))),
            }
            .empty_span()
        )
        .empty_span(),
        select.projection[0]
    );

    let sql = "SELECT a:location FROM t";
    let select = snowflake().verified_only_select(sql);
    assert_eq!(
        SelectItem::UnnamedExpr(
            Expr::JsonAccess {
                left: Box::new(Expr::Identifier(Ident::new("a").empty_span())),
                operator: JsonOperator::Colon,
                right: Box::new(Expr::Value(Value::UnQuotedString("location".to_string()))),
            }
            .empty_span()
        )
        .empty_span(),
        select.projection[0]
    );

    snowflake().one_statement_parses_to("SELECT a:b::int FROM t", "SELECT CAST(a:b AS INT) FROM t");
}

#[test]
fn parse_json_using_colon_and_keyword() {
    snowflake().one_statement_parses_to(
        "select to_varchar(payload:status:error), reason:metadata, reason:error, reason:name::string as main_tag from foo where reason:group::string = 'helmet'",
        "SELECT to_varchar(payload:status:error), reason:metadata, reason:error, CAST(reason:name AS STRING) AS main_tag FROM foo WHERE CAST(reason:group AS STRING) = 'helmet'"
    );
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = snowflake().verified_only_select(
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
            with_ordinality: _,
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
            approximate: false,
            special: false,
            order_by: vec![],
            limit: None,
            on_overflow: None,
            null_treatment: None,
            within_group: None,
            having_bound: None,
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

    snowflake().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    snowflake().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = snowflake().verified_only_select(sql);
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
        // With backslash escaping, '\\' in SQL tokenizes as single backslash
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' ESCAPE '\\\\'",
            if negated { "NOT " } else { "" }
        );
        let stmts = snowflake().parse_sql_statements(sql).unwrap();
        let select = match &stmts[0] {
            Statement::Query(q) => match q.body.as_ref() {
                SetExpr::Select(s) => s.as_ref(),
                _ => panic!("Expected Select"),
            },
            _ => panic!("Expected Query"),
        };
        assert_eq!(
            Expr::Like {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }
            .empty_span(),
            select.selection.clone().unwrap()
        );

        // This statement tests that LIKE and NOT LIKE have the same precedence.
        // This was previously mishandled (#81).
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = snowflake().verified_only_select(sql);
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
        let select = snowflake().verified_only_select(sql);
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
        // With backslash escaping, '\\' in SQL tokenizes as single backslash
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\\\'",
            if negated { "NOT " } else { "" }
        );
        let stmts = snowflake().parse_sql_statements(sql).unwrap();
        let select = match &stmts[0] {
            Statement::Query(q) => match q.body.as_ref() {
                SetExpr::Select(s) => s.as_ref(),
                _ => panic!("Expected Select"),
            },
            _ => panic!("Expected Query"),
        };
        assert_eq!(
            Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }
            .empty_span(),
            select.selection.clone().unwrap()
        );

        // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\\\' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let stmts = snowflake().parse_sql_statements(sql).unwrap();
        let select = match &stmts[0] {
            Statement::Query(q) => match q.body.as_ref() {
                SetExpr::Select(s) => s.as_ref(),
                _ => panic!("Expected Select"),
            },
            _ => panic!("Expected Query"),
        };
        assert_eq!(
            Expr::IsNull(Box::new(Expr::SimilarTo {
                expr: Box::new(Expr::Identifier(Ident::new("name").empty_span())),
                negated,
                pattern: Box::new(Expr::Value(Value::SingleQuotedString("%a".to_string()))),
                escape_char: Some('\\'),
            }))
            .empty_span(),
            select.selection.clone().unwrap()
        );
    }
    chk(false);
    chk(true);
}

#[test]
fn test_array_agg_func() {
    for sql in [
        "SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY x) AS a FROM T",
        "SELECT ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY x ASC) FROM tbl",
        "SELECT ARRAY_AGG(x ORDER BY x) AS a FROM T",
    ] {
        snowflake().verified_stmt(sql);
    }
}

#[test]
fn test_array_type_with_element_type() {
    // Snowflake supports ARRAY without element type
    snowflake().verified_stmt("SELECT CAST(x AS ARRAY) FROM t");
    // Snowflake also supports ARRAY(element_type) with parenthesized element type
    // Display uses ClickHouse-style "Array(type)" casing
    snowflake().one_statement_parses_to(
        "SELECT CAST([1, 2, 3] AS ARRAY(INT)) FROM t",
        "SELECT CAST([1, 2, 3] AS Array(INT)) FROM t",
    );
    snowflake().one_statement_parses_to(
        "SELECT CAST(col AS ARRAY(DECIMAL(38, 0))) FROM t",
        "SELECT CAST(col AS Array(DECIMAL(38,0))) FROM t",
    );
    snowflake().one_statement_parses_to(
        "SELECT CAST(NULL AS ARRAY(VARCHAR(30))) FROM t",
        "SELECT CAST(NULL AS Array(VARCHAR(30))) FROM t",
    );
}

#[test]
fn test_clone_before() {
    // Snowflake CLONE with time travel BEFORE clause
    snowflake().one_statement_parses_to(
        "CREATE TABLE orders_clone_restore CLONE orders BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')",
        "CREATE TABLE orders_clone_restore CLONE orders",
    );
}

#[test]
fn test_match_recognize() {
    // MATCH_RECOGNIZE is consumed as opaque balanced parens
    snowflake().one_statement_parses_to(
        "SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY col ORDER BY col2 MEASURES col3 AS m ONE ROW PER MATCH PATTERN (a+) DEFINE a AS col > 0)",
        "SELECT * FROM t MATCH_RECOGNIZE (...)",
    );
    // With alias
    snowflake().one_statement_parses_to(
        "SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY col MEASURES col2 AS m ONE ROW PER MATCH PATTERN (a) DEFINE a AS col > 0) AS mr",
        "SELECT * FROM t MATCH_RECOGNIZE (...) AS mr",
    );
}

fn snowflake() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {})],
        options: None,
    }
}

fn snowflake_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(SnowflakeDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

#[test]
fn test_select_wildcard_with_exclude() {
    let select = snowflake_and_generic().verified_only_select("SELECT * EXCLUDE (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![ObjectName(vec![
            Ident::new("col_a"),
        ])])),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic()
        .verified_only_select("SELECT name.* EXCLUDE department_id FROM employee_table");
    let expected = SelectItem::QualifiedWildcard(
        ObjectName(vec![Ident::new("name")]),
        WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Single(ObjectName(vec![Ident::new(
                "department_id",
            )]))),
            ..Default::default()
        },
    )
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic()
        .verified_only_select("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![
            ObjectName(vec![Ident::new("department_id")]),
            ObjectName(vec![Ident::new("employee_id")]),
        ])),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_rename() {
    let select =
        snowflake_and_generic().verified_only_select("SELECT * RENAME col_a AS col_b FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_rename: Some(RenameSelectItem::Single(IdentWithAlias {
            ident: Ident::new("col_a"),
            alias: Ident::new("col_b"),
        })),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = snowflake_and_generic().verified_only_select(
        "SELECT name.* RENAME (department_id AS new_dep, employee_id AS new_emp) FROM employee_table",
    );
    let expected = SelectItem::QualifiedWildcard(
        ObjectName(vec![Ident::new("name")]),
        WildcardAdditionalOptions {
            opt_rename: Some(RenameSelectItem::Multiple(vec![
                IdentWithAlias {
                    ident: Ident::new("department_id"),
                    alias: Ident::new("new_dep"),
                },
                IdentWithAlias {
                    ident: Ident::new("employee_id"),
                    alias: Ident::new("new_emp"),
                },
            ])),
            ..Default::default()
        },
    )
    .empty_span();
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn test_select_wildcard_with_exclude_and_rename() {
    let select = snowflake_and_generic()
        .verified_only_select("SELECT * EXCLUDE col_z RENAME col_a AS col_b FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Single(ObjectName(vec![Ident::new(
            "col_z",
        )]))),
        opt_rename: Some(RenameSelectItem::Single(IdentWithAlias {
            ident: Ident::new("col_a"),
            alias: Ident::new("col_b"),
        })),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    // rename cannot precede exclude
    assert_eq!(
        snowflake_and_generic()
            .parse_sql_statements("SELECT * RENAME col_a AS col_b EXCLUDE col_z FROM data")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected end of statement, found: EXCLUDE\nNear ` * RENAME col_a AS col_b`"
    );
}

#[test]
fn test_alter_table_cluster_by() {
    snowflake_and_generic().verified_stmt("ALTER TABLE t1 CLUSTER BY (col_1)");
    snowflake_and_generic().verified_stmt("ALTER TABLE t1 CLUSTER BY (date, id)");
    snowflake_and_generic().verified_stmt("ALTER TABLE table1 CLUSTER BY (name DESC)");
}

#[test]
fn test_alter_table_swap_with() {
    let sql = "ALTER TABLE tab1 SWAP WITH tab2";
    match alter_table_op_with_name(snowflake_and_generic().verified_stmt(sql), "tab1") {
        AlterTableOperation::SwapWith { table_name } => {
            assert_eq!("tab2", table_name.to_string());
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_alter_table_row_access_policy() {
    // Single DROP ROW ACCESS POLICY
    let sql = "ALTER TABLE t1 DROP ROW ACCESS POLICY rap_t1";
    snowflake_and_generic().verified_stmt(sql);

    // Single ADD ROW ACCESS POLICY
    let sql = "ALTER TABLE t1 ADD ROW ACCESS POLICY rap_t1 ON (empl_id)";
    snowflake_and_generic().verified_stmt(sql);

    // Combined DROP + ADD (comma-separated operations)
    let sql = "ALTER TABLE t1 DROP ROW ACCESS POLICY rap_t1_version_1, ADD ROW ACCESS POLICY rap_t1_version_2 ON (empl_id)";
    match snowflake_and_generic().verified_stmt(sql) {
        Statement::AlterTable {
            name, operations, ..
        } => {
            assert_eq!("t1", name.to_string());
            assert_eq!(2, operations.len());
            match &operations[0] {
                AlterTableOperation::DropRowAccessPolicy { policy } => {
                    assert_eq!("rap_t1_version_1", policy.to_string());
                }
                _ => unreachable!(),
            }
            match &operations[1] {
                AlterTableOperation::AddRowAccessPolicy { policy, on } => {
                    assert_eq!("rap_t1_version_2", policy.to_string());
                    assert_eq!(
                        vec!["empl_id"],
                        on.iter().map(|i| i.to_string()).collect::<Vec<_>>()
                    );
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }

    // Multiple columns in ON clause
    let sql = "ALTER TABLE t1 ADD ROW ACCESS POLICY rap ON (col1, col2, col3)";
    snowflake_and_generic().verified_stmt(sql);
}

#[test]
fn test_drop_stage() {
    match snowflake_and_generic().verified_stmt("DROP STAGE s1") {
        Statement::Drop {
            names, if_exists, ..
        } => {
            assert!(!if_exists);
            assert_eq!("s1", names[0].to_string());
        }
        _ => unreachable!(),
    };
    match snowflake_and_generic().verified_stmt("DROP STAGE IF EXISTS s1") {
        Statement::Drop {
            names, if_exists, ..
        } => {
            assert!(if_exists);
            assert_eq!("s1", names[0].to_string());
        }
        _ => unreachable!(),
    };

    snowflake_and_generic().one_statement_parses_to("DROP STAGE s1", "DROP STAGE s1");

    snowflake_and_generic()
        .one_statement_parses_to("DROP STAGE IF EXISTS s1", "DROP STAGE IF EXISTS s1");
}

#[test]
fn test_create_stage() {
    let sql = "CREATE STAGE s1.s2";
    match snowflake().verified_stmt(sql) {
        Statement::CreateStage {
            or_replace,
            temporary,
            if_not_exists,
            name,
            comment,
            ..
        } => {
            assert!(!or_replace);
            assert!(!temporary);
            assert!(!if_not_exists);
            assert_eq!("s1.s2", name.to_string());
            assert!(comment.is_none());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    let extended_sql = concat!(
        "CREATE OR REPLACE TEMPORARY STAGE IF NOT EXISTS s1.s2 ",
        "COMMENT='some-comment'"
    );
    match snowflake().verified_stmt(extended_sql) {
        Statement::CreateStage {
            or_replace,
            temporary,
            if_not_exists,
            name,
            stage_params,
            comment,
            ..
        } => {
            assert!(or_replace);
            assert!(temporary);
            assert!(if_not_exists);
            assert!(stage_params.url.is_none());
            assert!(stage_params.endpoint.is_none());
            assert_eq!("s1.s2", name.to_string());
            assert_eq!("some-comment", comment.unwrap());
        }
        _ => unreachable!(),
    };
    assert_eq!(
        snowflake().verified_stmt(extended_sql).to_string(),
        extended_sql
    );
}

#[test]
fn test_create_stage_with_stage_params() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "STORAGE_INTEGRATION=myint ",
        "ENDPOINT='<s3_api_compatible_endpoint>' ",
        "CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') ",
        "ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CreateStage { stage_params, .. } => {
            assert_eq!("s3://load/files/", stage_params.url.unwrap());
            assert_eq!("myint", stage_params.storage_integration.unwrap());
            assert_eq!(
                "<s3_api_compatible_endpoint>",
                stage_params.endpoint.unwrap()
            );
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_KEY_ID".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "1a2b3c".to_string()
                }));
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_SECRET_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "4x5y6z".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "MASTER_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "key".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "TYPE".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "AWS_SSE_KMS".to_string()
                }));
        }
        _ => unreachable!(),
    };

    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_create_stage_with_directory_table_params() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "DIRECTORY=(ENABLE=TRUE REFRESH_ON_CREATE=FALSE NOTIFICATION_INTEGRATION='some-string')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CreateStage {
            directory_table_params,
            ..
        } => {
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "ENABLE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "REFRESH_ON_CREATE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "FALSE".to_string()
            }));
            assert!(directory_table_params.options.contains(&DataLoadingOption {
                option_name: "NOTIFICATION_INTEGRATION".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: "some-string".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_create_stage_with_file_format() {
    // Use non-backslash escape char to avoid backslash-escape tokenizer issues
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='|')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CreateStage { file_format, .. } => {
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "COMPRESSION".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "AUTO".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "BINARY_FORMAT".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "HEX".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "ESCAPE".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: "|".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_create_stage_with_copy_options() {
    let sql = concat!(
        "CREATE OR REPLACE STAGE my_ext_stage ",
        "URL='s3://load/files/' ",
        "COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CreateStage { copy_options, .. } => {
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "ON_ERROR".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "CONTINUE".to_string()
            }));
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "FORCE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv'"
    );
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            into,
            from_stage,
            files,
            pattern,
            validation_mode,
            ..
        } => {
            assert_eq!(
                into,
                ObjectName(vec![Ident::new("my_company"), Ident::new("emp_basic")])
            );
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::with_quote('\'', "gcs://mybucket/./../a.csv")])
            );
            assert!(files.is_none());
            assert!(pattern.is_none());
            assert!(validation_mode.is_none());
        }
        _ => unreachable!(),
    };
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    // Test with columns and various options (ON_ERROR extracted from COPY_OPTIONS to top-level field)
    let input_sql = concat!(
        "COPY INTO my_schema.my_table (\"COL_A\", \"COL_B\") ",
        "FROM stage1 ",
        "FILES = ('file1.csv') ",
        "PATTERN = '.*[.]csv' ",
        "FILE_FORMAT=(TYPE='csv' SKIP_HEADER=1 COMPRESSION='zstd') ",
        "COPY_OPTIONS=(ON_ERROR=CONTINUE)"
    );
    let expected_sql = concat!(
        "COPY INTO my_schema.my_table (\"COL_A\", \"COL_B\") ",
        "FROM stage1 ",
        "FILES = ('file1.csv') ",
        "PATTERN = '.*[.]csv' ",
        "FILE_FORMAT=(TYPE='csv' SKIP_HEADER=1 COMPRESSION='zstd') ",
        "ON_ERROR = CONTINUE"
    );
    match snowflake().one_statement_parses_to(input_sql, expected_sql) {
        Statement::CopyIntoSnowflake {
            into,
            columns,
            pattern,
            on_error,
            copy_options,
            ..
        } => {
            assert_eq!(
                into,
                ObjectName(vec![Ident::new("my_schema"), Ident::new("my_table")])
            );
            assert_eq!(columns.len(), 2);
            assert!(pattern.is_some());
            assert_eq!(on_error, Some("CONTINUE".to_string()));
            assert!(copy_options.options.is_empty());
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_copy_into_with_stage_params() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 's3://load/files/' ",
        "STORAGE_INTEGRATION=myint ",
        "ENDPOINT='<s3_api_compatible_endpoint>' ",
        "CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') ",
        "ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_stage,
            stage_params,
            ..
        } => {
            //assert_eq!("s3://load/files/", stage_params.url.unwrap());
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::with_quote('\'', "s3://load/files/")])
            );
            assert_eq!("myint", stage_params.storage_integration.unwrap());
            assert_eq!(
                "<s3_api_compatible_endpoint>",
                stage_params.endpoint.unwrap()
            );
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_KEY_ID".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "1a2b3c".to_string()
                }));
            assert!(stage_params
                .credentials
                .options
                .contains(&DataLoadingOption {
                    option_name: "AWS_SECRET_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "4x5y6z".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "MASTER_KEY".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "key".to_string()
                }));
            assert!(stage_params
                .encryption
                .options
                .contains(&DataLoadingOption {
                    option_name: "TYPE".to_string(),
                    option_type: DataLoadingOptionType::STRING,
                    value: "AWS_SSE_KMS".to_string()
                }));
        }
        _ => unreachable!(),
    };

    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);

    // stage params within copy into with transformations
    let sql = concat!(
        "COPY INTO my_company.emp_basic FROM ",
        "(SELECT t1.$1 FROM 's3://load/files/' STORAGE_INTEGRATION=myint)",
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_stage,
            stage_params,
            ..
        } => {
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::with_quote('\'', "s3://load/files/")])
            );
            assert_eq!("myint", stage_params.storage_integration.unwrap());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_copy_into_with_files_and_pattern_and_verification() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' AS some_alias ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "VALIDATION_MODE = RETURN_7_ROWS"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            files,
            pattern,
            validation_mode,
            from_stage_alias,
            ..
        } => {
            assert_eq!(files.unwrap(), vec!["file1.json", "file2.json"]);
            assert_eq!(pattern.unwrap(), ".*employees0[1-5].csv.gz");
            assert_eq!(validation_mode.unwrap(), "RETURN_7_ROWS");
            assert_eq!(from_stage_alias.unwrap(), Ident::new("some_alias"));
        }
        _ => unreachable!(),
    }
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_with_transformations() {
    let sql = concat!(
        "COPY INTO my_company.emp_basic FROM ",
        "(SELECT t1.$1:st AS st, $1:index, t2.$1 FROM @schema.general_finished AS T) ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "VALIDATION_MODE = RETURN_7_ROWS"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            from_stage,
            from_transformations,
            ..
        } => {
            assert_eq!(
                from_stage,
                ObjectName(vec![Ident::new("@schema.general_finished")])
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[0],
                StageLoadSelectItem {
                    alias: Some(Ident::new("t1")),
                    file_col_num: 1,
                    element: Some(Ident::new("st")),
                    item_as: Some(Ident::new("st"))
                }
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[1],
                StageLoadSelectItem {
                    alias: None,
                    file_col_num: 1,
                    element: Some(Ident::new("index")),
                    item_as: None
                }
            );
            assert_eq!(
                from_transformations.as_ref().unwrap()[2],
                StageLoadSelectItem {
                    alias: Some(Ident::new("t2")),
                    file_col_num: 1,
                    element: None,
                    item_as: None
                }
            );
        }
        _ => unreachable!(),
    }
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_unload_from_query() {
    // COPY INTO @stage FROM (<query>) — unload form, full query as source.
    let sql = "COPY INTO @my_stage FROM (SELECT * FROM orderstiny LIMIT 5)";
    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake {
            into, from_query, ..
        } => {
            assert_eq!(into, ObjectName(vec![Ident::new("@my_stage")]));
            let q = from_query.expect("from_query should be Some");
            // Ensure the table reference inside the subquery is preserved
            // (critical for column-level lineage).
            assert!(q.to_string().contains("orderstiny"));
        }
        _ => unreachable!(),
    }
    assert_eq!(snowflake().verified_stmt(sql).to_string(), sql);
}

#[test]
fn test_copy_into_file_format() {
    // Use SQL without backslash escaping issues in string literals
    let sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='|')"
    );

    match snowflake().verified_stmt(sql) {
        Statement::CopyIntoSnowflake { file_format, .. } => {
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "COMPRESSION".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "AUTO".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "BINARY_FORMAT".to_string(),
                option_type: DataLoadingOptionType::ENUM,
                value: "HEX".to_string()
            }));
            assert!(file_format.options.contains(&DataLoadingOption {
                option_name: "ESCAPE".to_string(),
                option_type: DataLoadingOptionType::STRING,
                value: "|".to_string()
            }));
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_copy_into_on_error_before_file_format() {
    // ON_ERROR as bare option before FILE_FORMAT should parse correctly
    let input_sql = concat!(
        "COPY INTO sch1.tbl1 (\"COL_A\") ",
        "FROM stage1 ",
        "PATTERN = '.*[.]csv' ",
        "ON_ERROR = CONTINUE ",
        "FILE_FORMAT = (FORMAT_NAME='my_csv')"
    );
    let expected_sql = concat!(
        "COPY INTO sch1.tbl1 (\"COL_A\") ",
        "FROM stage1 ",
        "PATTERN = '.*[.]csv' ",
        "FILE_FORMAT=(FORMAT_NAME='my_csv') ",
        "ON_ERROR = CONTINUE"
    );
    match snowflake().one_statement_parses_to(input_sql, expected_sql) {
        Statement::CopyIntoSnowflake {
            on_error,
            file_format,
            ..
        } => {
            assert_eq!(on_error, Some("CONTINUE".to_string()));
            assert!(file_format
                .options
                .iter()
                .any(|o| o.option_name == "FORMAT_NAME"));
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_copy_into_without_from() {
    // COPY INTO with bare options and no FROM clause
    let input_sql = "COPY INTO mytable PURGE = TRUE";
    let expected_sql = "COPY INTO mytable COPY_OPTIONS=(PURGE=TRUE)";
    match snowflake().one_statement_parses_to(input_sql, expected_sql) {
        Statement::CopyIntoSnowflake {
            into, copy_options, ..
        } => {
            assert_eq!(into, ObjectName(vec![Ident::new("mytable")]));
            assert!(copy_options
                .options
                .iter()
                .any(|o| o.option_name == "PURGE"));
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_copy_into_copy_options() {
    // ON_ERROR is extracted from COPY_OPTIONS into the top-level on_error field
    let input_sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)"
    );
    let expected_sql = concat!(
        "COPY INTO my_company.emp_basic ",
        "FROM 'gcs://mybucket/./../a.csv' ",
        "FILES = ('file1.json', 'file2.json') ",
        "PATTERN = '.*employees0[1-5].csv.gz' ",
        "ON_ERROR = CONTINUE ",
        "COPY_OPTIONS=(FORCE=TRUE)"
    );

    match snowflake().one_statement_parses_to(input_sql, expected_sql) {
        Statement::CopyIntoSnowflake {
            on_error,
            copy_options,
            ..
        } => {
            assert_eq!(on_error, Some("CONTINUE".to_string()));
            assert!(copy_options.options.contains(&DataLoadingOption {
                option_name: "FORCE".to_string(),
                option_type: DataLoadingOptionType::BOOLEAN,
                value: "TRUE".to_string()
            }));
        }
        _ => unreachable!(),
    };
}

#[test]
fn test_snowflake_stage_object_names() {
    let allowed_formatted_names = [
        "my_company.emp_basic",
        "@namespace.%table_name",
        "@namespace.%table_name/path",
        "@namespace.stage_name/path",
        "@~/path",
    ];
    let mut allowed_object_names = vec![
        ObjectName(vec![Ident::new("my_company"), Ident::new("emp_basic")]),
        ObjectName(vec![Ident::new("@namespace.%table_name")]),
        ObjectName(vec![Ident::new("@namespace.%table_name/path")]),
        ObjectName(vec![Ident::new("@namespace.stage_name/path")]),
        ObjectName(vec![Ident::new("@~/path")]),
    ];

    for it in allowed_formatted_names
        .iter()
        .zip(allowed_object_names.iter_mut())
    {
        let (formatted_name, object_name) = it;
        let sql = format!(
            "COPY INTO {} FROM 'gcs://mybucket/./../a.csv'",
            formatted_name
        );
        match snowflake().verified_stmt(&sql) {
            Statement::CopyIntoSnowflake { into, .. } => {
                assert_eq!(into.0, object_name.0)
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn test_snowflake_stage_with_quoted_identifiers() {
    // Stage paths with quoted identifiers should be collapsed into a single Ident
    // just like unquoted stage paths (e.g., @namespace.stage/path).
    let sql = r#"SELECT * FROM @"myschema"."mystage"/file.gz"#;
    let select = snowflake().verified_only_select(sql);
    match &select.from[0].relation {
        TableFactor::Table { name, .. } => {
            assert_eq!(name.0, vec![Ident::new(r#"@"myschema"."mystage"/file.gz"#)]);
        }
        _ => unreachable!(),
    }

    let sql2 = r#"SELECT * FROM @"my_DB"."schEMA1".mystage/file.gz"#;
    let select2 = snowflake().verified_only_select(sql2);
    match &select2.from[0].relation {
        TableFactor::Table { name, .. } => {
            assert_eq!(
                name.0,
                vec![Ident::new(r#"@"my_DB"."schEMA1".mystage/file.gz"#)]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_snowflake_trim() {
    let real_sql = r#"SELECT customer_id, TRIM(sub_items.value:item_price_id, '"', "a") AS item_price_id FROM models_staging.subscriptions"#;
    assert_eq!(snowflake().verified_stmt(real_sql).to_string(), real_sql);

    let sql_only_select = "SELECT TRIM('xyz', 'a')";
    let select = snowflake().verified_only_select(sql_only_select);
    assert_eq!(
        &Expr::Trim {
            expr: Box::new(Expr::Value(Value::SingleQuotedString("xyz".to_owned()))),
            trim_where: None,
            trim_what: None,
            trim_characters: Some(vec![Expr::Value(Value::SingleQuotedString("a".to_owned()))]),
        },
        expr_from_projection(only(&select.projection))
    );

    // missing comma separation
    let error_sql = "SELECT TRIM('xyz' 'a')";
    assert_eq!(
        ParserError::ParserError(
            "Expected ), found: 'a'\nNear `SELECT TRIM('xyz'`"
                .to_owned()
                .into()
        ),
        snowflake().parse_sql_statements(error_sql).unwrap_err()
    );
}

#[test]
fn parse_division_correctly() {
    snowflake_and_generic().one_statement_parses_to(
        "SELECT field/1000 FROM tbl1",
        "SELECT field / 1000 FROM tbl1",
    );

    snowflake_and_generic().one_statement_parses_to(
        "SELECT tbl1.field/tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
        "SELECT tbl1.field / tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
    );
}

#[test]
fn parse_position_not_function_columns() {
    snowflake_and_generic()
        .verified_stmt("SELECT position FROM tbl1 WHERE position NOT IN ('first', 'last')");
}

#[test]
fn parse_subquery_function_argument() {
    // Snowflake allows passing an unparenthesized subquery as the single
    // argument to a function.
    snowflake().one_statement_parses_to(
        "SELECT parse_json(SELECT '{}')",
        "SELECT parse_json((SELECT '{}'))",
    );

    // Subqueries that begin with WITH work too.
    snowflake().one_statement_parses_to(
        "SELECT parse_json(WITH q AS (SELECT '{}' AS foo) SELECT foo FROM q)",
        "SELECT parse_json((WITH q AS (SELECT '{}' AS foo) SELECT foo FROM q))",
    );

    // Commas are parsed as part of the subquery, not additional arguments to
    // the function.
    snowflake().one_statement_parses_to("SELECT func(SELECT 1, 2)", "SELECT func((SELECT 1, 2))");
}

#[test]
fn parse_semi_structured_single() {
    snowflake().verified_only_select("SELECT src:salesperson FROM car_sales");
}

#[test]
fn parse_semi_structured_dot_notation() {
    snowflake().verified_only_select("SELECT src:salesperson.name FROM car_sales");
}

#[test]
fn parse_semi_structured_bracket_notation() {
    snowflake().verified_only_select("SELECT src['salesperson']['name'] FROM car_sales");
}

#[test]
fn parse_semi_structured_from_repeating() {
    snowflake().verified_only_select("SELECT src:vehicle[0] FROM car_sales");
}

#[test]
fn parse_semi_structured_from_repeating_dot() {
    snowflake().verified_only_select("SELECT src:vehicle[0].make FROM car_sales");
}

#[test]
fn parse_array_index() {
    snowflake().verified_only_select("SELECT src[0] FROM car_sales");
}

#[test]
fn parse_function_result_subscript() {
    // Function call result with numeric index
    snowflake().verified_only_select("SELECT SPLIT(col, '/')[0] FROM t");
    // Function call result with expression index (ARRAY_SIZE(...) - 1)
    snowflake()
        .verified_only_select("SELECT SPLIT(col, '/')[ARRAY_SIZE(SPLIT(col, '/')) - 1] FROM t");
    // Nested: function inside TRIM with subscript
    snowflake().verified_only_select("SELECT TRIM(SPLIT(col, '/')[0]) FROM t");
}

#[test]
fn parse_array_subscript_expr() {
    // Simple column reference as index
    snowflake().verified_only_select("SELECT arr[idx] FROM t");
    // Arithmetic expression as index: col - 1
    snowflake().verified_only_select("SELECT arr[idx - 1] FROM t");
    // Compound identifier as index
    snowflake().verified_only_select("SELECT arr[t.idx] FROM t");
    // Real-world pattern: GET_PATH with arithmetic subscript
    snowflake().verified_only_select("SELECT CAST(GET_PATH(col[n - 1], 'id') AS VARCHAR) FROM t");
}

#[test]
fn parse_array_index_json_colon() {
    snowflake().verified_only_select("SELECT src[0]:order_number FROM car_sales");
}

#[test]
fn parse_object_constants() {
    snowflake().verified_stmt("SELECT { 'Manitoba': 'Winnipeg' } AS province_capital");
    snowflake().verified_stmt("SELECT {} AS province_capital");
    snowflake().verified_stmt(
        "UPDATE my_table SET my_object = { 'Alberta': 'Edmonton', 'Manitoba': 'Winnipeg' }",
    );
    snowflake().verified_stmt("UPDATE my_table SET my_object = OBJECT_CONSTRUCT('Alberta', 'Edmonton', 'Manitoba', 'Winnipeg')");
}

#[test]
fn parse_object_constants_expr() {
    snowflake().verified_stmt("SELECT { 'foo': bar.baz } AS r FROM tbl AS bar");
}

#[test]
fn parse_array_index_json_dot() {
    let stmt = snowflake().verified_only_select("SELECT src[0].order_number FROM car_sales");

    assert_eq!(
        stmt.projection[0],
        SelectItem::UnnamedExpr(
            Expr::JsonAccess {
                left: Box::new(MapAccess {
                    column: Box::new(Expr::Identifier(Ident::new("src").empty_span())),
                    keys: vec![Expr::Value(number("0")),],
                }),
                operator: JsonOperator::Period,
                right: Box::new(Expr::Value(Value::UnQuotedString(
                    "order_number".to_string()
                ))),
            }
            .empty_span()
        )
        .empty_span()
    );
}

#[test]
fn parse_array_index_json_with_cast() {
    snowflake().one_statement_parses_to(
        "SELECT src[0]:order_number::string FROM car_sales",
        "SELECT CAST(src[0]:order_number AS STRING) FROM car_sales",
    );
}

#[test]
fn parse_pivot_of_table_factor_derived() {
    snowflake().verified_stmt(
        "SELECT * FROM (SELECT place_id, weekday, open FROM times AS p) PIVOT(max(open) FOR weekday IN (0, 1, 2, 3, 4, 5, 6)) AS p (place_id, open_sun, open_mon, open_tue, open_wed, open_thu, open_fri, open_sat)"
    );
}

#[test]
fn parse_pivot_with_subquery_in_clause() {
    // Snowflake supports subqueries in PIVOT's IN clause
    snowflake().verified_stmt(
        "SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (SELECT DISTINCT quarter FROM ad_campaign_types_by_quarter WHERE television = true ORDER BY quarter)) ORDER BY empid",
    );
    snowflake().verified_stmt(
        "SELECT * FROM tbl PIVOT(AVG(col_5) FOR col_4 IN (SELECT DISTINCT col_4 FROM tbl))",
    );
}

#[test]
fn parse_pivot_with_expression_values() {
    // Snowflake allows arbitrary expressions (e.g. CAST) in PIVOT's IN value list
    snowflake().verified_stmt(
        "SELECT * FROM tbl PIVOT(SUM(col_2) FOR col_3 IN (CAST('2025-11-01' AS DATE), CAST('2025-12-01' AS DATE)))",
    );
}

#[test]
fn parse_pivot_default_on_null() {
    // Snowflake supports DEFAULT ON NULL clause in PIVOT
    snowflake().verified_stmt(
        "SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN ('2023_Q1', '2023_Q2') DEFAULT ON NULL (0)) ORDER BY empid",
    );
    // DEFAULT ON NULL with ANY ORDER BY
    snowflake().verified_stmt(
        "SELECT * FROM quarterly_sales PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter) DEFAULT ON NULL (0)) ORDER BY empid",
    );
}

#[test]
fn parse_create_table_column_comment() {
    snowflake()
        .verified_stmt("CREATE TABLE my_table (my_column STRING COMMENT 'this is comment3')");
}

#[test]
fn parse_create_view_comment() {
    snowflake().verified_stmt(
        "CREATE VIEW my_view COMMENT='this is comment5' AS (SELECT * FROM my_table)",
    );
}

#[test]
fn parse_create_view_change_tracking() {
    // Snowflake: CHANGE_TRACKING = TRUE|FALSE view property, appearing before AS.
    // May be combined with COMMENT in either order.
    snowflake().one_statement_parses_to(
        "CREATE VIEW v2(one COMMENT 'bar') CHANGE_TRACKING = true AS SELECT a FROM my_table",
        "CREATE VIEW v2 (one) AS SELECT a FROM my_table",
    );
    snowflake().one_statement_parses_to(
        "CREATE VIEW v2(one) COMMENT = 'fff' CHANGE_TRACKING = true AS SELECT a FROM my_table",
        "CREATE VIEW v2 (one) COMMENT='fff' AS SELECT a FROM my_table",
    );
}

#[test]
fn parse_create_view_column_comment() {
    snowflake()
        .one_statement_parses_to(r#"CREATE OR REPLACE VIEW DB.SCHEMA.STORAGE_USAGE_HISTORY (DATE COMMENT 'Date of this storage usage record.', AVERAGE_STAGE_BYTES COMMENT 'Number of bytes of stage storage used.') COMMENT='See https://docs.snowflake.com/en/sql-reference/account-usage/stage_storage_usage_history.html' AS (SELECT usage_date AS date, average_stage_bytes FROM snowflake.account_usage.stage_storage_usage_history)"#,
     r#"CREATE OR REPLACE VIEW DB.SCHEMA.STORAGE_USAGE_HISTORY (DATE, AVERAGE_STAGE_BYTES) COMMENT='See https://docs.snowflake.com/en/sql-reference/account-usage/stage_storage_usage_history.html' AS (SELECT usage_date AS date, average_stage_bytes FROM snowflake.account_usage.stage_storage_usage_history)"#);
}

#[test]
fn parse_create_table_cluster_by() {
    snowflake().verified_stmt(
        "CREATE OR REPLACE TABLE t3 (vc VARCHAR) CLUSTER BY (SUBSTRING(vc FROM 5 FOR 5))",
    );
    snowflake().verified_stmt(
        "CREATE OR REPLACE TABLE t1 (c1 DATE, c2 STRING, c3 NUMBER) CLUSTER BY (c1, c2)",
    );
    snowflake().verified_stmt("CREATE OR REPLACE TABLE t2 (c1 TIMESTAMP, c2 STRING, c3 NUMBER) CLUSTER BY (TO_DATE(C1), SUBSTRING(c2 FROM 0 FOR 10))");
    snowflake().verified_stmt(r#"CREATE OR REPLACE TABLE T3 (t TIMESTAMP, v variant) CLUSTER BY (CAST(v:Data:id AS number))"#);

    snowflake().verified_stmt(
            "CREATE OR REPLACE TRANSIENT TABLE clustered_table (locker_number NUMBER(38, 0), MONTH DATE, CAPACITY NUMBER(38, 0)) CLUSTER BY (locker_number)"
    );

    snowflake().one_statement_parses_to(
        "create or replace TRANSIENT TABLE clustered_table cluster by (locker_number)(
	locker_number NUMBER(38,0),
	MONTH DATE,
	CAPACITY NUMBER(38,0))",
        "CREATE OR REPLACE TRANSIENT TABLE clustered_table (locker_number NUMBER(38, 0), MONTH DATE, CAPACITY NUMBER(38, 0)) CLUSTER BY (locker_number)"
    );
}

#[test]
fn parse_extract_custom_part() {
    let sql = "SELECT EXTRACT(eod FROM d)";
    let select = snowflake_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Custom(Ident::new("eod")),
            expr: Box::new(Expr::Identifier(Ident::new("d").empty_span())),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn parse_extract_comma_form() {
    // Snowflake also accepts the comma form `EXTRACT(<part>, <expr>)` as a
    // shorthand for the standard `EXTRACT(<part> FROM <expr>)`.
    // https://docs.snowflake.com/en/sql-reference/functions/extract
    let select = snowflake().one_statement_parses_to(
        "SELECT extract(year, birthdate) AS year_of_birth FROM t",
        "SELECT EXTRACT(YEAR FROM birthdate) AS year_of_birth FROM t",
    );
    assert!(matches!(select, Statement::Query(_)));
}

#[test]
fn parse_pg_cast_with_collate() {
    // Snowflake's COPY-loaded INSERT/SELECT bodies routinely emit
    //   $1::VARCHAR(255) COLLATE 'spec'
    // The COLLATE suffix on a `::` cast wasn't being consumed because `::`
    // is parsed as an infix in `parse_subexpr`, after `parse_prefix`'s
    // trailing-COLLATE check. `parse_pg_cast` now accepts an optional
    // COLLATE clause and wraps the cast in `Expr::Collate`.
    snowflake().one_statement_parses_to(
        "INSERT INTO t (a) (SELECT $1::VARCHAR(255) COLLATE 'spec' AS a FROM s)",
        "INSERT INTO t (a) (SELECT CAST($1 AS VARCHAR(255)) COLLATE 'spec' AS a FROM s)",
    );
    snowflake().one_statement_parses_to(
        "SELECT col::TEXT COLLATE 'spec' FROM t",
        "SELECT CAST(col AS TEXT) COLLATE 'spec' FROM t",
    );
}

#[test]
fn parse_create_table_comment() {
    snowflake().verified_stmt("CREATE TABLE my_table (my_column STRING COMMENT 'column comment')");
    snowflake().one_statement_parses_to(
        "CREATE TABLE my_table (my_column STRING COMMENT 'column comment') COMMENT='table comment'",
        "CREATE TABLE my_table (my_column STRING COMMENT 'column comment') COMMENT 'table comment'",
    );
}

#[test]
fn parse_interval_as_alias() {
    snowflake().verified_stmt("SELECT interval.start FROM intervals AS interval");
    snowflake().verified_stmt("SELECT interval.interval FROM intervals AS interval");
    snowflake().verified_stmt("SELECT interval, foo FROM intervals");
    snowflake().verified_stmt("SELECT * FROM intervals AS i JOIN interval_id_join AS interval ON intervals.interval_id = interval.interval_id");
    //FIXME:
    //snowflake().verified_stmt("SELECT interval FROM intervals");
}

#[test]
fn parse_regexp() {
    snowflake_and_generic().verified_stmt(r#"SELECT v FROM strings WHERE v REGEXP 'San* [fF].*'"#);
    // Generic dialect doesn't unescape backslashes, so round-trips fine
    TestedDialects {
        dialects: vec![Box::new(GenericDialect {})],
        options: None,
    }
    .verified_stmt(r#"SELECT v, v REGEXP 'San\\b.*' AS ok FROM strings"#);
    // Snowflake unescapes backslash sequences, verify it parses
    let stmts = snowflake()
        .parse_sql_statements(r#"SELECT v, v REGEXP 'San\\b.*' AS ok FROM strings"#)
        .unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn parse_tablesample() {
    snowflake().verified_stmt("SELECT * FROM testtable SAMPLE (10)");
    snowflake().verified_stmt("SELECT * FROM testtable TABLESAMPLE BERNOULLI (20.3)");
    snowflake().verified_stmt("SELECT * FROM testtable TABLESAMPLE (100)");
    snowflake().verified_stmt("SELECT * FROM (SELECT * FROM example_table) SAMPLE (1) SEED (99)");
    snowflake().verified_stmt("SELECT * FROM testtable SAMPLE ROW (0)");
    snowflake().verified_stmt(
        "SELECT i, j FROM table1 AS t1 SAMPLE (25) JOIN table2 AS t2 SAMPLE (50) WHERE t2.j = t1.i",
    );
    snowflake().verified_stmt(
        "SELECT i, j FROM table1 AS t1 JOIN table2 AS t2 SAMPLE (50) WHERE t2.j = t1.i",
    );
    snowflake().verified_stmt("SELECT * FROM testtable SAMPLE (10 ROWS)");
    let actual_select_only = snowflake()
        .verified_only_select("SELECT * FROM testtable SAMPLE BLOCK (0.012) REPEATABLE (99992)");
    let expected = Select {
        distinct: None,
        top: None,
        projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
            opt_exclude: None,
            opt_except: None,
            opt_rename: None,
            opt_replace: None,
            opt_apply: vec![],
        })
        .empty_span()],
        into: None,
        from: vec![TableWithJoins {
            relation: TableFactor::TableSample {
                table: Box::new(TableFactor::Table {
                    name: ObjectName(vec![Ident::new("testtable")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                }),
                sample: true,
                sampling_method: Some(SamplingMethod::Block),
                to_return: SelectionCount::FractionBased(number("0.012")),
                seed: Some(TableSampleSeed::Repeatable(number("99992"))),
            },
            joins: vec![],
        }],
        lateral_views: vec![],
        sample: None,
        selection: None,
        group_by: GroupByExpr::Expressions(vec![], vec![]),
        cluster_by: vec![],
        distribute_by: vec![],
        sort_by: vec![],
        having: None,
        named_window: vec![],
        qualify: None,
        value_table_mode: None,
        start_with: None,
        connect_by: None,
    };
    assert_eq!(actual_select_only, expected);
}

#[test]
fn parse_constraints() {
    snowflake().verified_stmt(
        r#"CREATE TABLE foo (id VARCHAR(32), CONSTRAINT "id_uk" UNIQUE (id) NOVALIDATE RELY)"#,
    );
    snowflake().verified_stmt(
        r#"ALTER TABLE foo ADD CONSTRAINT "bar" FOREIGN KEY (baz) REFERENCES othertable(baz) ON DELETE NO ACTION NORELY"#,
    );
}

#[test]
fn test_sf_trailing_commas() {
    snowflake().verified_only_select_with_canonical("SELECT 1, 2, FROM t", "SELECT 1, 2 FROM t");
}

#[test]
fn test_sf_trailing_commas_in_from_clause() {
    // Snowflake allows trailing commas in FROM clause, particularly useful
    // when using LATERAL FLATTEN with comma-separated table references
    snowflake().verified_only_select_with_canonical(
        "SELECT * FROM t1, t2, WHERE x = 1",
        "SELECT * FROM t1, t2 WHERE x = 1",
    );

    // With LATERAL FLATTEN - the common use case from PR-6610
    snowflake().verified_only_select_with_canonical(
        r#"SELECT a, b FROM t, LATERAL FLATTEN(INPUT => t.arr) AS f, WHERE a IS NOT NULL"#,
        r#"SELECT a, b FROM t, LATERAL FLATTEN(INPUT => t.arr) AS f WHERE a IS NOT NULL"#,
    );
}

#[test]
fn test_alter_session() {
    snowflake().verified_stmt("ALTER SESSION SET LOCK_TIMEOUT = 3600");
    snowflake().one_statement_parses_to(
        "ALTER SESSION SET autocommit = FALSE, QUERY_TAG = 'qtag', JSON_INDENT = 1",
        "ALTER SESSION SET autocommit = false, QUERY_TAG = 'qtag', JSON_INDENT = 1",
    );
    snowflake().verified_stmt("ALTER SESSION UNSET LOCK_TIMEOUT");
}

#[test]
fn test_copy_grants() {
    snowflake().verified_stmt("CREATE OR REPLACE TABLE tbl (EMPLOYEE_SK VARCHAR(32), EMPLOYEE_ID VARCHAR(16777216)) COPY GRANTS AS SELECT * FROM tbl2");
    snowflake().verified_stmt(
        "CREATE OR REPLACE VIEW v (EMPLOYEE_SK, EMPLOYEE_ID) COPY GRANTS AS SELECT * FROM tbl2",
    );
    // COMMENT after COPY GRANTS (Snowflake)
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE VIEW v COPY GRANTS COMMENT = 'my view comment' AS SELECT * FROM tbl",
        "CREATE OR REPLACE VIEW v COPY GRANTS COMMENT='my view comment' AS SELECT * FROM tbl",
    );
    // COMMENT with column list and COPY GRANTS
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE VIEW v (col1, col2) COPY GRANTS COMMENT = 'auto-generated view' AS SELECT col1, col2 FROM tbl",
        "CREATE OR REPLACE VIEW v (col1, col2) COPY GRANTS COMMENT='auto-generated view' AS SELECT col1, col2 FROM tbl",
    );
    // COPY GRANTS followed by column list (Snowflake MATERIALIZED VIEW syntax)
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE MATERIALIZED VIEW v COPY GRANTS (col1, col2) AS SELECT col1, col2 FROM tbl",
        "CREATE OR REPLACE MATERIALIZED VIEW v (col1, col2) COPY GRANTS AS SELECT col1, col2 FROM tbl",
    );
}

#[test]
fn test_column_with_masking() {
    snowflake().verified_stmt("CREATE OR REPLACE TABLE tbl (EMPLOYEE_SK VARCHAR(32) WITH MASKING POLICY unknown_policy, EMPLOYEE_ID VARCHAR(16777216) WITH MASKING POLICY unknown_policy)");
    // Qualified (multi-part) policy name
    snowflake().verified_stmt(
        "CREATE OR REPLACE TABLE tbl (EMPLOYEE_SK VARCHAR(32) WITH MASKING POLICY db1.sch1.unknown_policy)",
    );
}

#[test]
fn test_table_with_tag() {
    // Simple tag name
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216)) WITH TAG (UNKNOWN_TAG='#UNKNOWN_VALUE')",
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216))"
    );

    // Schema-qualified tag name
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216)) WITH TAG (TAG_SCHEMA.DOMAIN_MAPPING='marketing')",
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216))"
    );

    // Fully-qualified tag name (database.schema.tag)
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216)) WITH TAG (PROD.TAG_SCHEMA.DOMAIN_MAPPING='marketing')",
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216))"
    );

    // Multiple tags with different qualification levels
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216)) WITH TAG (SIMPLE_TAG='value1', SCHEMA.TAG_NAME='value2', DB.SCHEMA.TAG_NAME='value3')",
        "CREATE OR REPLACE TABLE TBL (ID VARCHAR(16777216))"
    );

    // Real-world example from the issue (anonymized)
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE TABLE SCHEMA.DERIVED_TABLE (USER_ID VARCHAR(36), REPORTING_DATE TIMESTAMP_NTZ(9)) WITH TAG (STAGE.TAG_SCHEMA.DOMAIN_MAPPING='analytics')",
        "CREATE OR REPLACE TABLE SCHEMA.DERIVED_TABLE (USER_ID VARCHAR(36), REPORTING_DATE TIMESTAMP_NTZ(9))"
    );
}

#[test]
fn test_column_with_tag() {
    // Column-level WITH TAG (...) — Snowflake docs allow `[ WITH ] TAG (...)` on columns,
    // same shape as the table-level form.
    snowflake().one_statement_parses_to(
        "CREATE TABLE t (col NUMBER(18, 4) WITH TAG (a.b.c = 'True'))",
        "CREATE TABLE t (col NUMBER(18, 4))",
    );
    // Multiple columns, mixed with regular options.
    snowflake().one_statement_parses_to(
        "CREATE TABLE t (id INT, amt NUMBER(18, 4) WITH TAG (schema.tag = 'val'), name VARCHAR)",
        "CREATE TABLE t (id INT, amt NUMBER(18, 4), name VARCHAR)",
    );
    // Bare TAG (no WITH) still works (existing behavior).
    snowflake().one_statement_parses_to(
        "CREATE TABLE t (col NUMBER(18, 4) TAG (a.b = 'v'))",
        "CREATE TABLE t (col NUMBER(18, 4))",
    );
}

#[test]
fn test_describe_table() {
    snowflake().verified_stmt(r#"DESCRIBE TABLE "DW_PROD"."SCH"."TBL""#);
}

#[test]
fn test_describe_object_types() {
    // DESCRIBE DATABASE
    snowflake().verified_stmt("DESCRIBE DATABASE desc_demo");
    // DESCRIBE WAREHOUSE
    snowflake().verified_stmt("DESCRIBE WAREHOUSE temporary_warehouse");
    // DESC SEQUENCE (DESC is alias for DESCRIBE)
    snowflake()
        .one_statement_parses_to("DESC SEQUENCE my_sequence", "DESCRIBE SEQUENCE my_sequence");
    // DESC STREAM (DESC is alias for DESCRIBE)
    snowflake().one_statement_parses_to("DESC STREAM mystream", "DESCRIBE STREAM mystream");
    // DESCRIBE VIEW
    snowflake().verified_stmt("DESCRIBE VIEW db.table");
    // DESCRIBE SCHEMA
    snowflake().verified_stmt("DESCRIBE SCHEMA my_schema");
    // DESCRIBE FUNCTION with parameter types
    snowflake().verified_stmt("DESCRIBE FUNCTION my_echo_udf(VARCHAR)");
    // DESC FUNCTION with TABLE parameter type
    snowflake().one_statement_parses_to(
        "DESC FUNCTION governance.dmfs.count_positive_numbers(TABLE(NUMBER, NUMBER, NUMBER))",
        "DESCRIBE FUNCTION governance.dmfs.count_positive_numbers(TABLE(NUMBER, NUMBER, NUMBER))",
    );
    // DESCRIBE TABLE with type=stage option
    snowflake().one_statement_parses_to(
        r#"DESCRIBE TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."WEB_SITE" type=stage"#,
        r#"DESCRIBE TABLE "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."WEB_SITE" type = stage"#,
    );
}

#[test]
fn test_asof_join() {
    snowflake().verified_stmt("SELECT * FROM table1 ASOF JOIN table2 MATCH_CONDITION (table1.timestamp <= table2.timestamp) ON table1.id = table2.id");
}

#[test]
fn test_insert_with_parenthesized_select() {
    // Test INSERT INTO with parenthesized SELECT subquery
    // This syntax is valid in many SQL dialects including Snowflake
    snowflake_and_generic().verified_stmt("INSERT INTO t (a, b) (SELECT x, y FROM s)");

    // With table alias
    snowflake_and_generic().verified_stmt("INSERT INTO t (a, b) (SELECT s.x, s.y FROM s AS s)");

    // Without column list
    snowflake_and_generic().verified_stmt("INSERT INTO t (SELECT x, y FROM s)");

    // With unquoted table alias (PR-6612 reproducer)
    snowflake_and_generic()
        .verified_stmt(r#"INSERT INTO "db"."schema"."t" ("a", "b") (SELECT "s"."a", "s"."b" FROM "db"."schema"."s" AS s)"#);
}

#[test]
fn test_snowflake_autoincrement_start_increment() {
    // AUTOINCREMENT with START/INCREMENT/ORDER
    let stmts = snowflake()
        .parse_sql_statements(
            "CREATE TABLE t (id INT AUTOINCREMENT START 1 INCREMENT 1 ORDER, name VARCHAR)",
        )
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // AUTOINCREMENT with START/INCREMENT/NOORDER
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (id INT AUTOINCREMENT START 1 INCREMENT 1 NOORDER)")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // AUTOINCREMENT with parenthesized seed/increment
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (id INT AUTOINCREMENT(1, 1))")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // AUTOINCREMENT without START/INCREMENT (plain)
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (id INT AUTOINCREMENT)")
        .unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn test_snowflake_identity() {
    // IDENTITY with parenthesized seed/increment (Snowflake synonym for AUTOINCREMENT)
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (id INT IDENTITY(1, 1))")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // IDENTITY without parameters
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (id INT IDENTITY)")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // IDENTITY with START/INCREMENT
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (id INT IDENTITY START 1 INCREMENT 1 ORDER)")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // ALTER TABLE ADD COLUMN with IDENTITY
    snowflake()
        .parse_sql_statements("ALTER TABLE foo ADD COLUMN id INT IDENTITY(1, 1)")
        .unwrap();
}

#[test]
fn test_snowflake_tag_clause() {
    // Table-level TAG (skipped in AST, not round-tripped)
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (a INT) TAG (db.schema.tag_name = 'value')")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // Table-level TAG with COMMENT
    let stmts = snowflake()
        .parse_sql_statements(
            "CREATE TABLE t (a INT) COMMENT='test' TAG (db.schema.tag_name = 'value')",
        )
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // View-level TAG before AS
    let stmts = snowflake()
        .parse_sql_statements("CREATE VIEW v TAG (db.schema.tag_name = 'value') AS SELECT 1")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // Column-level TAG
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (a INT TAG (db.schema.tag_name = 'value'), b INT)")
        .unwrap();
    assert_eq!(stmts.len(), 1);
    // Multiple tags
    let stmts = snowflake()
        .parse_sql_statements("CREATE TABLE t (a INT) TAG (s.t1 = 'v1', s.t2 = 'v2')")
        .unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn test_snowflake_backslash_escape_in_strings() {
    // Snowflake supports backslash escapes in strings like MySQL/BigQuery
    // Backslash-escaped quotes parse correctly (round-trip uses '' style)
    snowflake().one_statement_parses_to(r"SELECT 'it\'s a test'", "SELECT 'it''s a test'");
    // Ensure WHERE clause after string with escaped quote works
    snowflake().one_statement_parses_to(
        r"SELECT * FROM t WHERE c = 'it\'s'",
        "SELECT * FROM t WHERE c = 'it''s'",
    );
    // Backslash-backslash is consumed as single backslash
    let stmts = snowflake().parse_sql_statements(r"SELECT 'a\\b'").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn parse_wildcard_exclude_in_function_args() {
    // HASH(* EXCLUDE (col1, col2)) - Snowflake function with wildcard EXCLUDE
    snowflake().verified_stmt("SELECT HASH(* EXCLUDE (col1, col2)) FROM t1");

    // OBJECT_CONSTRUCT(* EXCLUDE province) - single column EXCLUDE
    snowflake().verified_stmt("SELECT OBJECT_CONSTRUCT(* EXCLUDE province) FROM t1");
}

#[test]
fn parse_create_view_with_masking_policy() {
    // View columns with MASKING POLICY
    snowflake().one_statement_parses_to(
        "CREATE VIEW v1 (col1, col2 MASKING POLICY policy1, col3 MASKING POLICY policy2) AS SELECT * FROM t1",
        "CREATE VIEW v1 (col1, col2, col3) AS SELECT * FROM t1",
    );

    // View column with MASKING POLICY and COMMENT
    snowflake().one_statement_parses_to(
        "CREATE VIEW v1 (col1 MASKING POLICY p1 COMMENT 'test') AS SELECT * FROM t1",
        "CREATE VIEW v1 (col1) AS SELECT * FROM t1",
    );

    // View column with MASKING POLICY, TAG and COMMENT
    snowflake().one_statement_parses_to(
        "CREATE VIEW v1 (col1 MASKING POLICY p1 TAG (t1 = 'v1') COMMENT 'test') AS SELECT * FROM t1",
        "CREATE VIEW v1 (col1) AS SELECT * FROM t1",
    );

    // Qualified policy name (db.schema.policy)
    snowflake().one_statement_parses_to(
        "CREATE VIEW v1 (col1 MASKING POLICY db1.sch1.pol1) AS SELECT * FROM t1",
        "CREATE VIEW v1 (col1) AS SELECT * FROM t1",
    );

    // WITH prefix on MASKING POLICY, PROJECTION POLICY and TAG (Snowflake view column clauses)
    snowflake().one_statement_parses_to(
        "CREATE VIEW v1 (col1, col2 WITH MASKING POLICY p1, col3 WITH TAG (t1 = 'v1')) AS SELECT * FROM t1",
        "CREATE VIEW v1 (col1, col2, col3) AS SELECT * FROM t1",
    );
    snowflake().one_statement_parses_to(
        "CREATE VIEW v1 (col1 WITH MASKING POLICY p1 USING (col1, col2) WITH PROJECTION POLICY p2 WITH TAG (t1 = 'v1') COMMENT 'c') AS SELECT * FROM t1",
        "CREATE VIEW v1 (col1) AS SELECT * FROM t1",
    );
}

#[test]
fn parse_create_table_with_row_access_policy() {
    // Snowflake CREATE TABLE trailing [WITH] ROW ACCESS POLICY <name> ON (cols)
    // and [WITH] TAG (...). The WITH prefix is optional per
    // https://docs.snowflake.com/en/sql-reference/sql/create-table
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (id VARCHAR, dept VARCHAR) WITH ROW ACCESS POLICY p1 ON (id)",
        "CREATE TABLE t1 (id VARCHAR, dept VARCHAR)",
    );
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (id VARCHAR) ROW ACCESS POLICY p1 ON (id)",
        "CREATE TABLE t1 (id VARCHAR)",
    );
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (id VARCHAR) WITH ROW ACCESS POLICY p1 ON (id) WITH TAG (k = 'v')",
        "CREATE TABLE t1 (id VARCHAR)",
    );
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (id VARCHAR) WITH TAG (k = 'v') WITH ROW ACCESS POLICY p1 ON (id)",
        "CREATE TABLE t1 (id VARCHAR)",
    );
    // Snowflake's GET_DDL omits `ON (cols)` when the caller lacks privilege to
    // see the policy — it returns `WITH ROW ACCESS POLICY unknown_policy` only.
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (id VARCHAR) WITH ROW ACCESS POLICY unknown_policy",
        "CREATE TABLE t1 (id VARCHAR)",
    );
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (id VARCHAR) ROW ACCESS POLICY unknown_policy",
        "CREATE TABLE t1 (id VARCHAR)",
    );
    snowflake().one_statement_parses_to(
        "CREATE VIEW v1 WITH ROW ACCESS POLICY unknown_policy AS SELECT * FROM t1",
        "CREATE VIEW v1 AS SELECT * FROM t1",
    );
}

#[test]
fn parse_snowflake_show_parameters_then_merge() {
    // SHOW PARAMETERS uses the loose parse_identifiers, which previously
    // consumed tokens across statement boundaries until it hit `=`. That
    // swallowed the trailing MERGE in dbt-emitted multi-statement bodies.
    // The fix: parse_identifiers stops at `;` in addition to `=` / EOF.
    let sql = "show parameters like 'query_tag' in session; \
               merge into t as dst using s as src on (dst.k = src.k) \
               when matched then update set \"c1\" = src.\"c1\"";
    let stmts = snowflake().parse_sql_statements(sql).unwrap();
    assert_eq!(stmts.len(), 2);
    assert!(matches!(
        stmts[0],
        sqlparser::ast::Statement::ShowVariable { .. }
    ));
    assert!(matches!(stmts[1], sqlparser::ast::Statement::Merge { .. }));
}

#[test]
fn parse_snowflake_scripting_declare_block() {
    // Snowflake scripting DECLARE with typed variables and CURSOR FOR.
    // https://docs.snowflake.com/en/sql-reference/snowflake-scripting/declare
    let sql = "declare \
               sql_text string; \
               batches cursor for (SELECT DISTINCT seq FROM src); \
               res resultset; \
               begin \
               insert into tgt select 1; \
               end";
    let stmts = snowflake().parse_sql_statements(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        sqlparser::ast::Statement::SnowflakeBlock {
            declarations, body, ..
        } => {
            assert_eq!(declarations.len(), 3);
            match &declarations[1] {
                sqlparser::ast::SnowflakeBlockDeclaration::Cursor { name, query } => {
                    assert_eq!(name.value, "batches");
                    let q = format!("{query}");
                    assert!(q.contains("SELECT"));
                    assert!(q.contains("src"));
                }
                d => panic!("expected cursor declaration, got {:?}", d),
            }
            match &declarations[2] {
                sqlparser::ast::SnowflakeBlockDeclaration::Resultset { name, .. } => {
                    assert_eq!(name.value, "res");
                }
                d => panic!("expected resultset declaration, got {:?}", d),
            }
            assert_eq!(body.len(), 1);
            assert!(matches!(body[0], sqlparser::ast::Statement::Insert { .. }));
        }
        _ => panic!("expected SnowflakeBlock, got {:?}", stmts[0]),
    }
}

#[test]
fn parse_snowflake_begin_end_block_no_declare() {
    // Top-level BEGIN ... END without DECLARE (Snowflake scripting).
    let sql = "begin insert into tgt select 1; end";
    let stmts = snowflake().parse_sql_statements(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    assert!(matches!(
        stmts[0],
        sqlparser::ast::Statement::SnowflakeBlock { .. }
    ));
}

#[test]
fn parse_snowflake_stage_file_operations() {
    // Snowflake file-management commands against internal stages.
    // Parsed but the body is opaque — no lineage payload.
    // https://docs.snowflake.com/en/sql-reference/sql/remove
    // https://docs.snowflake.com/en/sql-reference/sql/put
    let stmt = snowflake()
        .parse_sql_statements("REMOVE @~/staging_dir/")
        .unwrap();
    assert_eq!(stmt.len(), 1);
    match &stmt[0] {
        sqlparser::ast::Statement::StageFileOperation { command, body } => {
            assert_eq!(command, "REMOVE");
            assert!(body.starts_with("@"));
        }
        _ => panic!("expected StageFileOperation, got {:?}", stmt[0]),
    }

    let stmt = snowflake()
        .parse_sql_statements("PUT file:///tmp/data.csv @mystage")
        .unwrap();
    assert_eq!(stmt.len(), 1);
    match &stmt[0] {
        sqlparser::ast::Statement::StageFileOperation { command, .. } => {
            assert_eq!(command, "PUT");
        }
        _ => panic!("expected StageFileOperation, got {:?}", stmt[0]),
    }
}

#[test]
fn parse_create_view_with_tag_and_row_access_policy() {
    // Snowflake CREATE VIEW allows WITH TAG (...) and WITH ROW ACCESS POLICY before AS.
    // https://docs.snowflake.com/en/sql-reference/sql/create-view
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE VIEW v1 WITH TAG (dept = 'finance') AS SELECT 1",
        "CREATE OR REPLACE VIEW v1 AS SELECT 1",
    );
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE VIEW v1 (col1, col2) WITH TAG (dept = 'finance') AS SELECT 1, 2",
        "CREATE OR REPLACE VIEW v1 (col1, col2) AS SELECT 1, 2",
    );
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE VIEW v1 WITH ROW ACCESS POLICY p1 ON (id) AS SELECT id FROM t",
        "CREATE OR REPLACE VIEW v1 AS SELECT id FROM t",
    );
}

#[test]
fn parse_column_comment_after_masking_policy() {
    // Column COMMENT after MASKING POLICY in CREATE TABLE
    // Display outputs COMMENT before MASKING POLICY (options before policy)
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (col1 VARCHAR MASKING POLICY p1 COMMENT 'description')",
        "CREATE TABLE t1 (col1 VARCHAR COMMENT 'description' MASKING POLICY p1)",
    );

    // Column COMMENT after TAG in CREATE TABLE
    snowflake().one_statement_parses_to(
        "CREATE TABLE t1 (col1 VARCHAR TAG (t1 = 'v1') COMMENT 'description')",
        "CREATE TABLE t1 (col1 VARCHAR COMMENT 'description')",
    );
}

#[test]
fn test_not_aggregate_over_window() {
    // Parenthesized function with OVER
    snowflake().one_statement_parses_to(
        "SELECT (BOOLOR_AGG(col_22)) OVER (PARTITION BY col_1) FROM t",
        "SELECT BOOLOR_AGG(col_22) OVER (PARTITION BY col_1) FROM t",
    );
    // NOT applied to aggregate with OVER window clause
    snowflake().one_statement_parses_to(
        "SELECT NOT (BOOLOR_AGG(col_22)) OVER (PARTITION BY col_1) AS IS_MAGIC_RIDE FROM t",
        "SELECT NOT BOOLOR_AGG(col_22) OVER (PARTITION BY col_1) AS IS_MAGIC_RIDE FROM t",
    );
}

#[test]
fn test_show_columns_in_table() {
    // Basic SHOW COLUMNS IN TABLE
    snowflake().verified_stmt("SHOW COLUMNS IN TABLE test_show_columns");
    // Quoted schema with unquoted table
    snowflake().verified_stmt(r#"SHOW COLUMNS IN TABLE "sch_1".tbl_1"#);
    // Fully qualified name
    snowflake().verified_stmt(r#"SHOW COLUMNS IN TABLE "db"."schema".my_table"#);
    // SHOW COLUMNS IN VIEW
    snowflake().verified_stmt("SHOW COLUMNS IN VIEW my_view");
}

#[test]
fn test_use_secondary_roles() {
    // USE SECONDARY ROLES with a single role
    snowflake().verified_stmt("USE SECONDARY ROLES ALL");
    snowflake().verified_stmt("USE SECONDARY ROLES NONE");
    // USE SECONDARY ROLES with comma-separated roles
    snowflake().verified_stmt("USE SECONDARY ROLES test_role_1, test_role_2");
    snowflake().verified_stmt("USE SECONDARY ROLES a, b, c");
    // Basic USE statements
    snowflake().verified_stmt("USE ROLE my_role");
    snowflake().verified_stmt("USE DATABASE my_db");
    snowflake().verified_stmt("USE SCHEMA my_schema");
    snowflake().verified_stmt("USE WAREHOUSE my_wh");
}

#[test]
fn test_snowflake_create_table_using_template() {
    // Simple USING TEMPLATE with subquery
    snowflake().verified_stmt(
        "CREATE TABLE mytable USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(LOCATION => '@mystage', FILE_FORMAT => 'my_parquet_format')))",
    );

    // Corpus example with WITHIN GROUP and OR REPLACE
    snowflake().one_statement_parses_to(
        "CREATE TABLE mytable USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY order_id) FROM TABLE(INFER_SCHEMA(LOCATION=>'@mystage', FILE_FORMAT=>'my_parquet_format')))",
        "CREATE TABLE mytable USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY order_id) FROM TABLE(INFER_SCHEMA(LOCATION => '@mystage', FILE_FORMAT => 'my_parquet_format')))",
    );

    // Verify the AST fields
    match snowflake().verified_stmt(
        "CREATE TABLE mytable USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(LOCATION => '@mystage', FILE_FORMAT => 'my_parquet_format')))",
    ) {
        Statement::CreateTable {
            name,
            using_template,
            ..
        } => {
            assert_eq!("mytable", name.to_string());
            assert!(using_template.is_some());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_revoke_from_application() {
    // REVOKE with APPLICATION grantee type and VIEW object type
    snowflake().verified_stmt(
        "REVOKE SELECT ON VIEW data.views.credit_usage FROM APPLICATION app_snowflake_credits RESTRICT",
    );
    // Without RESTRICT (defaults to RESTRICT in output)
    snowflake().one_statement_parses_to(
        "REVOKE SELECT ON VIEW data.views.credit_usage FROM APPLICATION app_snowflake_credits",
        "REVOKE SELECT ON VIEW data.views.credit_usage FROM APPLICATION app_snowflake_credits RESTRICT",
    );
}

#[test]
fn test_revoke_grantee_identifier_wrapper() {
    // Snowflake IDENTIFIER(...) wrapper evaluates to an object name at runtime.
    // Accept it on the grantee name and unwrap to the inner literal.
    snowflake().one_statement_parses_to(
        "REVOKE SELECT ON TABLE tbl_1 FROM ROLE IDENTIFIER('DUST_AI_TEAM') CASCADE",
        "REVOKE SELECT ON TABLE tbl_1 FROM ROLE 'DUST_AI_TEAM' CASCADE",
    );
    snowflake().one_statement_parses_to(
        "GRANT SELECT ON TABLE tbl_1 TO ROLE IDENTIFIER('analyst')",
        "GRANT SELECT ON TABLE tbl_1 TO ROLE 'analyst'",
    );
}

#[test]
fn test_snowflake_model_method_syntax() {
    // Simple model method call
    snowflake().verified_stmt("SELECT model!PREDICT(1)");

    // Model method with named argument and object wildcard
    snowflake().verified_stmt("SELECT m!PREDICT(INPUT_DATA => {*}) AS p FROM tbl");

    // Model method with qualified object wildcard
    snowflake().verified_stmt("SELECT m!PREDICT(INPUT_DATA => {tbl.*}) AS p FROM tbl");

    // Model method call on a function-result receiver, e.g. MODEL(...)!PREDICT(...)
    snowflake()
        .verified_stmt("SELECT MODEL(schema.classifier, 'DEFAULT')!PREDICT(col_1, col_2) FROM tbl");
}

#[test]
fn test_placeholder_field_access() {
    // Snowflake positional column reference with field access
    snowflake().verified_stmt("SELECT $1.elem");

    // Multi-level field access
    snowflake().verified_stmt("SELECT $1.elem.sub");
}

#[test]
fn test_create_function_dollar_quoted() {
    // Snowflake CREATE FUNCTION with $$...$$ body (extra clauses like RUNTIME_VERSION are consumed but not serialized)
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE FUNCTION py_udf() RETURNS VARIANT LANGUAGE PYTHON RUNTIME_VERSION = '3.10' HANDLER = 'udf' AS $$\nimport numpy as np\ndef udf():\n    return [np.__version__]\n$$",
        "CREATE OR REPLACE FUNCTION py_udf RETURNS VARIANT LANGUAGE PYTHON AS $$\nimport numpy as np\ndef udf():\n    return [np.__version__]\n$$",
    );

    // Simple function with $$...$$ body (no extra clauses)
    snowflake().one_statement_parses_to(
        "CREATE FUNCTION echo_varchar(x VARCHAR) RETURNS VARCHAR LANGUAGE SCALA AS $$\n  class Echo {\n    def echoVarchar(x : String): String = {\n      return x\n    }\n  }\n  $$",
        "CREATE FUNCTION echo_varchar(x VARCHAR) RETURNS VARCHAR LANGUAGE SCALA AS $$\n  class Echo {\n    def echoVarchar(x : String): String = {\n      return x\n    }\n  }\n  $$",
    );
}

#[test]
fn test_create_external_function() {
    // Snowflake CREATE EXTERNAL FUNCTION — API_INTEGRATION clause consumed by the generic fallback
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE EXTERNAL FUNCTION local_echo(string_col VARCHAR) RETURNS VARIANT API_INTEGRATION = demo_api AS 'https://xyz.example.com/echo'",
        "CREATE OR REPLACE FUNCTION local_echo(string_col VARCHAR) RETURNS VARIANT AS 'https://xyz.example.com/echo'",
    );
    snowflake().one_statement_parses_to(
        "CREATE OR REPLACE EXTERNAL FUNCTION db.sch.fn(INPUT VARCHAR) RETURNS VARIANT COMMENT='External fn' API_INTEGRATION=API_INT AS 'https://x'",
        "CREATE OR REPLACE FUNCTION db.sch.fn(INPUT VARCHAR) RETURNS VARIANT COMMENT 'External fn' AS 'https://x'",
    );
}

#[test]
fn parse_create_sequence_with() {
    // Snowflake WITH syntax: key=value, comma-separated
    snowflake().one_statement_parses_to(
        "CREATE SEQUENCE seq1 WITH START=1, INCREMENT=1 ORDER",
        "CREATE SEQUENCE seq1 START 1 INCREMENT 1 ORDER",
    );
}

#[test]
fn test_primary_as_column_name() {
    // PRIMARY is a keyword but should be usable as a column name
    snowflake().verified_stmt(
        "CREATE OR REPLACE TABLE t1 (id VARCHAR(16777216), PRIMARY VARCHAR(16777216))",
    );
}

#[test]
fn test_positional_column_parameters() {
    // Snowflake positional parameters for staged file columns: :1, :2, etc.
    snowflake().verified_stmt("SELECT :1");
    snowflake().verified_stmt("SELECT :1, :2");
    snowflake().verified_stmt("SELECT :1 + :2");
}

#[test]
fn test_set_tuple_assignment() {
    // Snowflake SET with tuple assignment: SET (var1, var2) = (expr1, expr2)
    snowflake().one_statement_parses_to(
        "SET (V1, V2) = (10, 'example')",
        "SET (V1, V2) = (10, 'example')",
    );
    snowflake().one_statement_parses_to("SET (min, max) = (40, 70)", "SET (min, max) = (40, 70)");
    // With session variable references
    snowflake().one_statement_parses_to(
        "SET (min, max) = (50, 2 * $min)",
        "SET (min, max) = (50, 2 * $min)",
    );
}

#[test]
fn parse_positional_column_references_after_dot() {
    // Snowflake $N positional column references after dot (e.g., t.$1)
    snowflake().verified_stmt("SELECT t.$1, t.$2 FROM @mystage1 AS t");
    snowflake().verified_stmt("SELECT v1.$2 FROM (VALUES (1, 'one')) AS v1 WHERE v1.$1 = 1");
}

#[test]
fn parse_positional_column_references() {
    // Snowflake $N positional column references after dot (e.g., t.$1)
    snowflake().verified_stmt("SELECT t.$1, t.$2 FROM @mystage1 AS t");
    // In WHERE clause
    snowflake().verified_stmt("SELECT v1.$2 FROM (VALUES (1, 'one')) AS v1 WHERE v1.$1 = 1");
}

#[test]
fn test_grouping_sets_without_inner_parens() {
    // GROUPING SETS (a, b) without inner parens is valid SQL - treated as two single-column sets
    // The parser normalizes to GROUPING SETS ((a), (b)) on output
    snowflake_and_generic().one_statement_parses_to(
        "SELECT a FROM t GROUP BY GROUPING SETS (a, b)",
        "SELECT a FROM t GROUP BY GROUPING SETS ((a), (b))",
    );
    // GROUPING() function in CASE WHEN with GROUPING SETS
    snowflake_and_generic().one_statement_parses_to(
        "SELECT CASE WHEN GROUPING(a, b) = 1 THEN 1 ELSE 0 END FROM t GROUP BY GROUPING SETS (a, b)",
        "SELECT CASE WHEN GROUPING(a, b) = 1 THEN 1 ELSE 0 END FROM t GROUP BY GROUPING SETS ((a), (b))",
    );
    // Mixed parens: one multi-column set (with parens) and one single (without)
    snowflake_and_generic().one_statement_parses_to(
        "SELECT a FROM t GROUP BY GROUPING SETS ((a, b), c)",
        "SELECT a FROM t GROUP BY GROUPING SETS ((a, b), (c))",
    );
    // Empty set still works
    snowflake_and_generic().verified_stmt("SELECT a FROM t GROUP BY GROUPING SETS ((a, b), ())");
}

#[test]
fn test_alter_table_set_tag() {
    // Snowflake: ALTER TABLE ... SET TAG schema.tag_name = 'value'
    // The TAG keyword is consumed and the tag name is used as the option name
    snowflake().one_statement_parses_to(
        "ALTER TABLE tbl SET TAG STAGE.schema.tag_col = 'finance'",
        "ALTER TABLE tbl SET (STAGE.schema.tag_col = 'finance')",
    );
    // Simple tag name without schema qualification
    snowflake().one_statement_parses_to(
        "ALTER TABLE tbl SET TAG my_tag = 'value'",
        "ALTER TABLE tbl SET (my_tag = 'value')",
    );
}

#[test]
fn test_snowflake_select_wildcard_replace() {
    // Snowflake supports SELECT * REPLACE (expr AS col_name, ...)
    snowflake().verified_stmt("SELECT * REPLACE (col1 + 1 AS col1) FROM t");
    snowflake().verified_stmt(
        "SELECT t.* REPLACE (REGEXP_REPLACE(col1, 'a', 'b') AS col1, col2 * 2 AS col2) FROM t",
    );
    // Inside subquery
    snowflake().verified_stmt("SELECT * FROM (SELECT * REPLACE (x + 1 AS x) FROM t) AS sub");
}

#[test]
fn test_snowflake_unpivot_column_alias() {
    // Snowflake supports UNPIVOT with identifier aliases in the IN clause
    snowflake().verified_stmt(
        "SELECT * FROM monthly_sales UNPIVOT (sales FOR month IN (jan AS january, feb AS february, mar AS march, apr AS april)) ORDER BY empid",
    );
    // Quoted identifier aliases (e.g., numeric-looking names)
    snowflake()
        .verified_stmt("SELECT * FROM t UNPIVOT (val FOR col IN (col1 AS \"a\", col2 AS \"b\"))");
}

#[test]
fn test_snowflake_projection_as_column_name() {
    // PROJECTION should be usable as a column name in Snowflake (and generic) CREATE TABLE
    // It is only a special table-level syntax in ClickHouse
    snowflake_and_generic()
        .verified_stmt("CREATE TABLE t (PROJECTION DECIMAL(38,0), score DECIMAL(38,0))");
}

#[test]
fn test_snowflake_connect_by() {
    // Basic CONNECT BY hierarchical query
    snowflake_and_generic().verified_stmt(
        "SELECT col_id, col_parent_id FROM tbl CONNECT BY col_parent_id = PRIOR col_id",
    );
    // With START WITH
    snowflake_and_generic().verified_stmt(
        "SELECT col_id, col_parent_id FROM tbl START WITH col_parent_id IS NULL CONNECT BY col_parent_id = PRIOR col_id",
    );
    // With NOCYCLE
    snowflake_and_generic().verified_stmt(
        "SELECT col_id FROM tbl START WITH col_parent_id IS NULL CONNECT BY NOCYCLE col_parent_id = PRIOR col_id",
    );
    // CONNECT_BY_ROOT prefix operator
    snowflake_and_generic().verified_stmt(
        "SELECT CONNECT_BY_ROOT col_id AS root_id FROM tbl CONNECT BY col_parent_id = PRIOR col_id",
    );
    // SYS_CONNECT_BY_PATH usage (regular function call, non-roundtrip due to SUBSTRING special parsing)
    snowflake_and_generic().one_statement_parses_to(
        "SELECT LENGTH(SYS_CONNECT_BY_PATH(col_name, ' -> ')) AS path FROM tbl CONNECT BY col_parent_id = PRIOR col_id",
        "SELECT LENGTH(SYS_CONNECT_BY_PATH(col_name, ' -> ')) AS path FROM tbl CONNECT BY col_parent_id = PRIOR col_id",
    );
    // In CTE context
    snowflake_and_generic().verified_stmt(
        "WITH hier AS (SELECT col_id, CONNECT_BY_ROOT col_id AS root_id FROM tbl START WITH col_parent_id IS NULL CONNECT BY col_parent_id = PRIOR col_id) SELECT * FROM hier",
    );
}

#[test]
fn test_snowflake_oracle_outer_join_marker() {
    // Snowflake / Oracle legacy (+) outer-join marker in WHERE clauses.
    snowflake_and_generic()
        .verified_stmt("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)");
    snowflake_and_generic().verified_stmt(
        "SELECT t1.c1 FROM t1, t2, t3 WHERE t1.c1 = t2.c2 (+) AND t2.c2 = t3.c3 (+)",
    );
    // No space between column and `(+)` should also parse.
    snowflake_and_generic().one_statement_parses_to(
        "SELECT t1.c1 FROM t1, t2 WHERE t1.c1 = t2.c2(+)",
        "SELECT t1.c1 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)",
    );
}

#[test]
fn test_snowflake_double_with_precision() {
    // Snowflake query logs emit DOUBLE(p) as a synonym for DOUBLE; accept and preserve it.
    snowflake_and_generic().verified_stmt("SELECT CAST(x AS DOUBLE(6)) FROM t");
    snowflake_and_generic().verified_stmt("SELECT CAST(x AS DOUBLE) FROM t");
}

#[test]
fn test_snowflake_alter_dynamic_table() {
    // Snowflake dynamic tables: SUSPEND/RESUME/REFRESH. ALTER DYNAMIC TABLE is
    // normalized to ALTER TABLE in the AST so lineage can treat both uniformly.
    let cases = [
        ("ALTER DYNAMIC TABLE foo SUSPEND", "ALTER TABLE foo SUSPEND"),
        ("ALTER DYNAMIC TABLE foo RESUME", "ALTER TABLE foo RESUME"),
        ("ALTER DYNAMIC TABLE foo REFRESH", "ALTER TABLE foo REFRESH"),
        (
            "ALTER DYNAMIC TABLE IF EXISTS db.sch.foo REFRESH",
            "ALTER TABLE IF EXISTS db.sch.foo REFRESH",
        ),
    ];
    for (input, expected) in cases {
        let stmt = snowflake().one_statement_parses_to(input, expected);
        match stmt {
            Statement::AlterTable { name, .. } => {
                assert!(!name.0.is_empty(), "name should be preserved");
            }
            other => panic!("expected AlterTable, got {other:?}"),
        }
    }
}

#[test]
fn test_snowflake_column_collate_then_default() {
    // Real customer DDL puts COLLATE between NOT NULL and DEFAULT/COMMENT.
    // The parser must resume consuming column options after COLLATE.
    // Display normalizes COLLATE before other options — accept the rewrite.
    snowflake_and_generic().one_statement_parses_to(
        "CREATE TABLE t (c VARCHAR(32) NOT NULL COLLATE 'en-cs' DEFAULT '-' COMMENT 'd')",
        "CREATE TABLE t (c VARCHAR(32) COLLATE 'en-cs' NOT NULL DEFAULT '-' COMMENT 'd')",
    );
}

#[test]
fn test_snowflake_create_secure_view() {
    // Snowflake `SECURE` modifier on CREATE VIEW must set CreateView { secure: true, .. }
    // — not fall through to a generic Comment statement (silent round-trip corruption
    // that also hides the view from lineage visitors).
    let cases = [
        ("CREATE SECURE VIEW v AS SELECT id FROM t", None),
        (
            "CREATE OR REPLACE SECURE VIEW s.v AS SELECT id, name FROM s.t",
            None,
        ),
        (
            "CREATE OR REPLACE SECURE VIEW v COMMENT = 'desc' AS SELECT id FROM t",
            Some("CREATE OR REPLACE SECURE VIEW v COMMENT='desc' AS SELECT id FROM t"),
        ),
    ];
    for (sql, canonical) in cases {
        let stmt = if let Some(c) = canonical {
            snowflake().one_statement_parses_to(sql, c)
        } else {
            snowflake().verified_stmt(sql)
        };
        match stmt {
            Statement::CreateView {
                secure,
                materialized,
                name,
                query,
                ..
            } => {
                assert!(secure, "secure flag should be set for {sql}");
                assert!(!materialized, "materialized should be false for {sql}");
                assert!(!name.0.is_empty());
                // Query must still hold the inner SELECT so lineage can walk it.
                assert!(!matches!(*query, Query { .. } if false));
                let _ = query; // silence unused
            }
            other => panic!("expected CreateView, got {other:?}"),
        }
    }
}

#[test]
fn test_snowflake_create_secure_materialized_view() {
    let sql = "CREATE OR REPLACE SECURE MATERIALIZED VIEW mv AS SELECT product_id, SUM(qty) AS n FROM oi GROUP BY product_id";
    let stmt = snowflake().verified_stmt(sql);
    match stmt {
        Statement::CreateView {
            secure,
            materialized,
            or_replace,
            ..
        } => {
            assert!(secure);
            assert!(materialized);
            assert!(or_replace);
        }
        other => panic!("expected CreateView, got {other:?}"),
    }
}

#[test]
fn test_snowflake_create_secure_function() {
    // Snowflake `SECURE` modifier on CREATE FUNCTION must set CreateFunction { secure: true, .. }.
    let sql = "CREATE OR REPLACE SECURE FUNCTION safe_div(a NUMBER, b NUMBER) RETURNS NUMBER AS $$ CASE WHEN b = 0 THEN NULL ELSE a / b END $$";
    let stmt = snowflake().verified_stmt(sql);
    match stmt {
        Statement::CreateFunction {
            secure,
            or_replace,
            name,
            ..
        } => {
            assert!(secure);
            assert!(or_replace);
            assert_eq!(name.to_string(), "safe_div");
        }
        other => panic!("expected CreateFunction, got {other:?}"),
    }
}

#[test]
fn test_snowflake_create_dynamic_table() {
    // CREATE DYNAMIC TABLE must route through CreateTable (not silently
    // become a Comment statement) and preserve TARGET_LAG / WAREHOUSE so
    // visitors can see the referenced warehouse.
    let sql = "CREATE OR REPLACE DYNAMIC TABLE d TARGET_LAG = '30 minutes' WAREHOUSE = wh AS SELECT a, b FROM t";
    let stmt = snowflake().parse_sql_statements(sql).unwrap().remove(0);
    match stmt {
        Statement::CreateTable {
            dynamic,
            iceberg,
            hybrid,
            or_replace,
            name,
            query,
            table_options,
            ..
        } => {
            assert!(dynamic);
            assert!(!iceberg);
            assert!(!hybrid);
            assert!(or_replace);
            assert_eq!(name.to_string(), "d");
            assert!(query.is_some());
            let keys: Vec<String> = table_options
                .iter()
                .map(|o| o.name.to_string().to_uppercase())
                .collect();
            assert!(keys.iter().any(|k| k == "TARGET_LAG"), "keys={keys:?}");
            assert!(keys.iter().any(|k| k == "WAREHOUSE"), "keys={keys:?}");
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }
}

#[test]
fn test_snowflake_create_dynamic_table_full_options() {
    let sql = "CREATE DYNAMIC TABLE d TARGET_LAG = DOWNSTREAM WAREHOUSE = wh REFRESH_MODE = INCREMENTAL INITIALIZE = ON_CREATE AS SELECT * FROM t";
    let stmt = snowflake().parse_sql_statements(sql).unwrap().remove(0);
    match stmt {
        Statement::CreateTable {
            dynamic,
            table_options,
            ..
        } => {
            assert!(dynamic);
            let keys: Vec<String> = table_options
                .iter()
                .map(|o| o.name.to_string().to_uppercase())
                .collect();
            for k in ["TARGET_LAG", "WAREHOUSE", "REFRESH_MODE", "INITIALIZE"] {
                assert!(keys.iter().any(|x| x == k), "missing {k} in {keys:?}");
            }
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }
}

#[test]
fn test_snowflake_create_hybrid_table() {
    let sql = "CREATE HYBRID TABLE prefs (customer_id INT NOT NULL, pref_key VARCHAR(64) NOT NULL, PRIMARY KEY (customer_id, pref_key))";
    let stmt = snowflake().parse_sql_statements(sql).unwrap().remove(0);
    match stmt {
        Statement::CreateTable {
            hybrid,
            dynamic,
            name,
            columns,
            ..
        } => {
            assert!(hybrid);
            assert!(!dynamic);
            assert_eq!(name.to_string(), "prefs");
            assert!(!columns.is_empty());
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }
}

#[test]
fn test_snowflake_execute_immediate_from() {
    // Snowflake `EXECUTE IMMEDIATE FROM <stage_path>` runs SQL stored in a file.
    snowflake()
        .parse_sql_statements("EXECUTE IMMEDIATE FROM './insert-inventory.sql'")
        .unwrap();
    snowflake()
        .parse_sql_statements("EXECUTE IMMEDIATE FROM '@mystage/script.sql'")
        .unwrap();
}

#[test]
fn test_snowflake_sort_as_table_alias() {
    // SORT is reserved only because of `SORT BY` (Hive/Spark). In Snowflake
    // and most other dialects, `sort` is a regular identifier and may be
    // used as a table alias — e.g. `FROM t SORT` followed by `SORT.col`.
    snowflake()
        .parse_sql_statements("SELECT SORT.NAME AS SALES_ORDER_TYPE_NAME FROM t SORT")
        .unwrap();
    snowflake()
        .parse_sql_statements("SELECT sort.id FROM (SELECT 1 AS id) AS sort")
        .unwrap();
}

#[test]
fn test_snowflake_view_column_comment_then_policy() {
    // Snowflake CREATE VIEW column list permits per-column annotations in any
    // order: COMMENT '...', MASKING POLICY p [USING (...)], PROJECTION POLICY p,
    // TAG (k='v', ...). Generators emit them after the COMMENT clause.
    snowflake()
        .parse_sql_statements(
            "CREATE OR REPLACE VIEW v (c1, c2 COMMENT 'x' MASKING POLICY p) AS SELECT a, b FROM t",
        )
        .unwrap();
    snowflake()
        .parse_sql_statements(
            "CREATE OR REPLACE VIEW v (c1 COMMENT 'a', c2 COMMENT 'b' WITH TAG (env = 'prod')) AS SELECT a, b FROM t",
        )
        .unwrap();
    snowflake()
        .parse_sql_statements(
            "CREATE OR REPLACE VIEW v (c1 MASKING POLICY p COMMENT 'a') AS SELECT a FROM t",
        )
        .unwrap();
}

#[test]
fn test_snowflake_at_as_table_alias() {
    // Snowflake AT/BEFORE time travel must only be consumed when followed
    // by `(`, otherwise `at` is a plain table alias — e.g.
    // `INNER JOIN absencetype at ON at.id = ...`.
    snowflake()
        .parse_sql_statements(
            "SELECT * FROM t1 INNER JOIN absencetype at ON at.id = t1.absencetype_id",
        )
        .unwrap();
    snowflake()
        .parse_sql_statements("SELECT * FROM t1 LEFT JOIN absencetype before ON before.id = t1.id")
        .unwrap();
    // Time travel still parses normally.
    snowflake()
        .parse_sql_statements("SELECT * FROM t AT (TIMESTAMP => '2024-01-01'::TIMESTAMP)")
        .unwrap();
}
