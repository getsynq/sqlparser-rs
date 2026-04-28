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

use sqlparser::ast;
use std::ops::Deref;

use sqlparser::ast::*;
use sqlparser::dialect::{BigQueryDialect, GenericDialect};
use sqlparser::parser::{ParserError, ParserOptions};
use sqlparser::tokenizer::*;
use test_utils::*;

#[test]
fn parse_literal_string() {
    let sql = r#"SELECT 'single', "double""#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedString("single".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::DoubleQuotedString("double".to_string())),
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_byte_literal() {
    let sql = r#"SELECT B'abc', B"abc""#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Value(Value::SingleQuotedByteStringLiteral("abc".to_string())),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Value(Value::DoubleQuotedByteStringLiteral("abc".to_string())),
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT b'abc', b"abc""#;
    bigquery().one_statement_parses_to(sql, r#"SELECT B'abc', B"abc""#);
}

#[test]
fn parse_raw_literal() {
    let sql = r#"SELECT R'abc', R"abc", R'f\(abc,(.*),def\)', R"f\(abc,(.*),def\)""#;
    let stmt = bigquery_unescaped().one_statement_parses_to(
        sql,
        r"SELECT R'abc', R'abc', R'f\(abc,(.*),def\)', R'f\(abc,(.*),def\)'",
    );
    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = *query.body {
            assert_eq!(4, select.projection.len());
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral("abc".to_string())),
                expr_from_projection(&select.projection[0])
            );
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral("abc".to_string())),
                expr_from_projection(&select.projection[1])
            );
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral(r"f\(abc,(.*),def\)".to_string())),
                expr_from_projection(&select.projection[2])
            );
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral(r"f\(abc,(.*),def\)".to_string())),
                expr_from_projection(&select.projection[3])
            );
            return;
        }
    }
    panic!("invalid query")
}

#[test]
fn parse_raw_literal_triple_quoted() {
    // Simple triple-quoted raw string
    let sql = "SELECT R\"\"\"hello world\"\"\"";
    let stmt = bigquery_unescaped().one_statement_parses_to(sql, "SELECT R'hello world'");
    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = *query.body {
            assert_eq!(1, select.projection.len());
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral("hello world".to_string())),
                expr_from_projection(&select.projection[0])
            );
        } else {
            panic!("invalid query body")
        }
    } else {
        panic!("invalid statement")
    }

    // Triple-quoted raw string with embedded quotes and newlines
    let sql = "SELECT R\"\"\"\n  return x*y;\n\"\"\"";
    let stmt = bigquery_unescaped().one_statement_parses_to(sql, "SELECT R'\n  return x*y;\n'");
    if let Statement::Query(query) = stmt {
        if let SetExpr::Select(select) = *query.body {
            assert_eq!(1, select.projection.len());
            assert_eq!(
                &Expr::Value(Value::RawStringLiteral("\n  return x*y;\n".to_string())),
                expr_from_projection(&select.projection[0])
            );
        } else {
            panic!("invalid query body")
        }
    } else {
        panic!("invalid statement")
    }
}

#[test]
fn parse_create_function_with_triple_quoted_raw_string() {
    // BigQuery CREATE TEMP FUNCTION with r"""...""" body (JS UDF)
    let sql = "CREATE TEMP FUNCTION multiplyInputs(x FLOAT64, y FLOAT64) RETURNS FLOAT64 LANGUAGE js AS r\"\"\"\n  return x*y;\n\"\"\"";
    bigquery_unescaped().one_statement_parses_to(
        sql,
        "CREATE TEMPORARY FUNCTION multiplyInputs(x FLOAT64, y FLOAT64) RETURNS FLOAT64 LANGUAGE js AS '\n  return x*y;\n'",
    );
}

#[test]
fn parse_nested_data_types() {
    let sql = "CREATE TABLE table (x STRUCT<a ARRAY<INT64>, b BYTES(42)>, y ARRAY<STRUCT<INT64>>)";
    match bigquery().one_statement_parses_to(sql, sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, ObjectName(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("x").empty_span(),
                        data_type: DataType::Struct(vec![
                            StructField {
                                field_name: Some(Ident::new("a").empty_span()),
                                field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(
                                    Box::new(DataType::Int64)
                                )),
                                colon: false,
                                not_null: false,
                                options: vec![],
                            },
                            StructField {
                                field_name: Some(Ident::new("b").empty_span()),
                                field_type: DataType::Bytes(Some(42)),
                                colon: false,
                                not_null: false,
                                options: vec![],
                            },
                        ]),
                        collation: None,
                        codec: None,
                        options: vec![],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                    ColumnDef {
                        name: Ident::new("y").empty_span(),
                        data_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                            DataType::Struct(vec![StructField {
                                field_name: None,
                                field_type: DataType::Int64,
                                colon: false,
                                not_null: false,
                                options: vec![],
                            }]),
                        ))),
                        collation: None,
                        codec: None,
                        options: vec![],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_deeply_nested_struct_types() {
    // STRUCT containing a field with ARRAY<STRING> - the >> closing pattern
    let sql = "CREATE TABLE tbl (metadata STRUCT<uuid STRING, sort_keys ARRAY<STRING>>)";
    bigquery().one_statement_parses_to(sql, sql);

    // Nested STRUCT inside STRUCT
    let sql = "CREATE TABLE tbl (sla STRUCT<policy STRUCT<id INT64, title STRING>>)";
    bigquery().one_statement_parses_to(sql, sql);

    // Triple nesting
    let sql = "CREATE TABLE tbl (x STRUCT<y STRUCT<z ARRAY<INT64>>>)";
    bigquery().one_statement_parses_to(sql, sql);
}

#[test]
fn parse_invalid_brackets() {
    let sql = "SELECT STRUCT<INT64>>(NULL)";
    assert_eq!(
        bigquery().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError(
            "Expected (, found: >\nNear `SELECT STRUCT<INT64>`"
                .to_string()
                .into()
        )
    );

    let sql = "SELECT STRUCT<STRUCT<INT64>>>(NULL)";
    assert_eq!(
        bigquery().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError(
            "Expected (, found: >\nNear `SELECT STRUCT<STRUCT<INT64>>`"
                .to_string()
                .into()
        )
    );

    let sql = "CREATE TABLE table (x STRUCT<STRUCT<INT64>>>)";
    assert_eq!(
        bigquery().parse_sql_statements(sql).unwrap_err(),
        ParserError::ParserError("Expected ',' or ')' after column definition, found: >\nNear `(x STRUCT<STRUCT<INT64>>`".to_string().into())
    );
}

#[test]
fn parse_tuple_struct_literal() {
    // tuple syntax: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#tuple_syntax
    // syntax: (expr1, expr2 [, ... ])
    let sql = "SELECT (1, 2, 3), (1, 1.0, '123', true)";
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Tuple(vec![
            Expr::Value(number("1")),
            Expr::Value(number("2")),
            Expr::Value(number("3")),
        ]),
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Tuple(vec![
            Expr::Value(number("1")),
            Expr::Value(number("1.0")),
            Expr::Value(Value::SingleQuotedString("123".to_string())),
            Expr::Value(Value::Boolean(true)),
        ]),
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_typeless_struct_syntax() {
    // typeless struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typeless_struct_syntax
    // syntax: STRUCT( expr1 [AS field_name] [, ... ])
    let sql = "SELECT STRUCT(1, 2, 3), STRUCT('abc'), STRUCT(1, t.str_col), STRUCT(1 AS a, 'abc' AS b), STRUCT(str_col AS abc)";
    let select = bigquery().verified_only_select(sql);
    assert_eq!(5, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::Value(number("2")),
                Expr::Value(number("3")),
            ],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[0])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::SingleQuotedString("abc".to_string()))],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::CompoundIdentifier(
                    vec![Ident::from("t"), Ident::from("str_col")].empty_span()
                ),
            ],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Named {
                    expr: Expr::Value(number("1")).into(),
                    name: Ident::from("a").empty_span(),
                },
                Expr::Named {
                    expr: Expr::Value(Value::SingleQuotedString("abc".to_string())).into(),
                    name: Ident::from("b").empty_span(),
                },
            ],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[3])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Named {
                expr: Expr::Identifier(Ident::from("str_col").empty_span()).into(),
                name: Ident::from("abc").empty_span(),
            }],
            fields: Default::default(),
        },
        expr_from_projection(&select.projection[4])
    );
}

#[test]
fn parse_typed_struct_syntax() {
    // typed struct syntax https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#typed_struct_syntax
    // syntax: STRUCT<[field_name] field_type, ...>( expr1 [, ... ])

    let sql = r#"SELECT STRUCT<INT64>(5), STRUCT<x INT64, y STRING, timezone STRING>(1, t.str_col, timezone), STRUCT<arr ARRAY<FLOAT64>, str STRUCT<BOOL>>(nested_col)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![
                Expr::Value(number("1")),
                Expr::CompoundIdentifier(
                    vec![
                        Ident {
                            value: "t".into(),
                            quote_style: None,
                        },
                        Ident {
                            value: "str_col".into(),
                            quote_style: None,
                        },
                    ]
                    .empty_span()
                ),
                Expr::Identifier(Ident::new("timezone").empty_span()),
            ],
            fields: vec![
                StructField {
                    field_name: Some(Ident::new("x").empty_span()),
                    field_type: DataType::Int64,
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
                StructField {
                    field_name: Some(Ident::new("y").empty_span()),
                    field_type: DataType::String(None),
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
                StructField {
                    field_name: Some(Ident::new("timezone").empty_span()),
                    field_type: DataType::String(None),
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
            ],
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Identifier(Ident::new("nested_col").empty_span())],
            fields: vec![
                StructField {
                    field_name: Some(Ident::new("arr").empty_span()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Float64
                    ))),
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
                StructField {
                    field_name: Some(Ident::new("str").empty_span()),
                    field_type: DataType::Struct(vec![StructField {
                        field_name: None,
                        field_type: DataType::Bool,
                        colon: false,
                        not_null: false,
                        options: vec![],
                    }]),
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
            ],
        },
        expr_from_projection(&select.projection[2])
    );

    let sql = r#"SELECT STRUCT<x STRUCT, y ARRAY<STRUCT>>(nested_col)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Identifier(Ident::new("nested_col").empty_span())],
            fields: vec![
                StructField {
                    field_name: Some(Ident::new("x").empty_span()),
                    field_type: DataType::Struct(Default::default()),
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
                StructField {
                    field_name: Some(Ident::new("y").empty_span()),
                    field_type: DataType::Array(ArrayElemTypeDef::AngleBracket(Box::new(
                        DataType::Struct(Default::default())
                    ))),
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
            ],
        },
        expr_from_projection(&select.projection[0])
    );

    let sql = r#"SELECT STRUCT<BOOL>(true), STRUCT<BYTES(42)>(B'abc')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::Boolean(true))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bool,
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::SingleQuotedByteStringLiteral(
                "abc".into()
            )),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Bytes(Some(42)),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<DATE>("2011-05-05"), STRUCT<DATETIME>(DATETIME '1999-01-01 01:23:34.45'), STRUCT<FLOAT64>(5.0), STRUCT<INT64>(1)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(4, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString(
                "2011-05-05".to_string()
            )),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Date,
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Datetime(None, None),
                value: "1999-01-01 01:23:34.45".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Datetime(None, None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[1])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5.0"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Float64,
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[2])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("1"))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Int64,
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[3])
    );

    let sql = r#"SELECT STRUCT<INTERVAL>(INTERVAL '1-2 3 4:5:6.789999'), STRUCT<JSON>(JSON '{"class" : {"students" : [{"name" : "Jane"}]}}')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Interval(ast::Interval {
                value: Box::new(Expr::Value(Value::SingleQuotedString(
                    "1-2 3 4:5:6.789999".to_string()
                ))),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            }),],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Interval(None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::JSON(None),
                value: r#"{"class" : {"students" : [{"name" : "Jane"}]}}"#.to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::JSON(None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<STRING(42)>("foo"), STRUCT<TIMESTAMP>(TIMESTAMP '2008-12-25 15:30:00 America/Los_Angeles'), STRUCT<TIME>(TIME '15:30:00')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(3, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString("foo".to_string()))],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::String(Some(42)),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Timestamp(None, TimezoneInfo::None),
                value: "2008-12-25 15:30:00 America/Los_Angeles".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Timestamp(None, TimezoneInfo::None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[1])
    );

    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Time(None, TimezoneInfo::None),
                value: "15:30:00".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Time(None, TimezoneInfo::None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[2])
    );

    let sql = r#"SELECT STRUCT<NUMERIC>(NUMERIC '1'), STRUCT<BIGNUMERIC>(BIGNUMERIC '1')"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::Numeric(ExactNumberInfo::None),
                value: "1".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::Numeric(ExactNumberInfo::None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::TypedString {
                data_type: DataType::BigNumeric(ExactNumberInfo::None),
                value: "1".to_string(),
            },],
            fields: vec![StructField {
                field_name: None,
                field_type: DataType::BigNumeric(ExactNumberInfo::None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[1])
    );
}

#[test]
fn parse_typed_struct_with_field_name() {
    let sql = r#"SELECT STRUCT<x INT64>(5), STRUCT<y STRING>("foo")"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(2, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5"))],
            fields: vec![StructField {
                field_name: Some(Ident::from("x").empty_span()),
                field_type: DataType::Int64,
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[0])
    );
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(Value::DoubleQuotedString("foo".to_string()))],
            fields: vec![StructField {
                field_name: Some(Ident::from("y").empty_span()),
                field_type: DataType::String(None),
                colon: false,
                not_null: false,
                options: vec![],
            }],
        },
        expr_from_projection(&select.projection[1])
    );

    let sql = r#"SELECT STRUCT<x INT64, y INT64>(5, 5)"#;
    let select = bigquery().verified_only_select(sql);
    assert_eq!(1, select.projection.len());
    assert_eq!(
        &Expr::Struct {
            values: vec![Expr::Value(number("5")), Expr::Value(number("5"))],
            fields: vec![
                StructField {
                    field_name: Some(Ident::from("x").empty_span()),
                    field_type: DataType::Int64,
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
                StructField {
                    field_name: Some(Ident::from("y").empty_span()),
                    field_type: DataType::Int64,
                    colon: false,
                    not_null: false,
                    options: vec![],
                },
            ],
        },
        expr_from_projection(&select.projection[0])
    );
}

#[test]
fn parse_table_identifiers() {
    /// Parses a table identifier ident and verifies that re-serializing the
    /// parsed identifier produces the original ident string.
    ///
    /// In some cases, re-serializing the result of the parsed ident is not
    /// expected to produce the original ident string. canonical is provided
    /// instead as the canonical representation of the identifier for comparison.
    /// For example, re-serializing the result of ident `foo.bar` produces
    /// the equivalent canonical representation `foo`.`bar`
    fn test_table_ident(ident: &str, canonical: Option<&str>, expected: Vec<Ident>) {
        let sql = format!("SELECT 1 FROM {ident}");
        let canonical = canonical.map(|ident| format!("SELECT 1 FROM {ident}"));

        let select = if let Some(canonical) = canonical {
            bigquery().verified_only_select_with_canonical(&sql, canonical.deref())
        } else {
            bigquery().verified_only_select(&sql)
        };

        assert_eq!(
            select.from,
            vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(expected),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                    with_ordinality: false,
                },
                joins: vec![],
            },]
        );
    }

    fn test_table_ident_err(ident: &str) {
        let sql = format!("SELECT 1 FROM {ident}");
        assert!(bigquery().parse_sql_statements(&sql).is_err());
    }

    test_table_ident("`spa ce`", None, vec![Ident::with_quote('`', "spa ce")]);

    test_table_ident(
        "`!@#$%^&*()-=_+`",
        None,
        vec![Ident::with_quote('`', "!@#$%^&*()-=_+")],
    );

    test_table_ident(
        "_5abc.dataField",
        None,
        vec![Ident::new("_5abc"), Ident::new("dataField")],
    );
    test_table_ident(
        "`5abc`.dataField",
        None,
        vec![Ident::with_quote('`', "5abc"), Ident::new("dataField")],
    );

    test_table_ident_err("5abc.dataField");

    test_table_ident(
        "abc5.dataField",
        None,
        vec![Ident::new("abc5"), Ident::new("dataField")],
    );

    test_table_ident_err("abc5!.dataField");

    test_table_ident(
        "`GROUP`.dataField",
        None,
        vec![Ident::with_quote('`', "GROUP"), Ident::new("dataField")],
    );

    // TODO: this should be error
    // test_table_ident_err("GROUP.dataField");

    test_table_ident(
        "abc5.GROUP",
        None,
        vec![Ident::new("abc5"), Ident::new("GROUP")],
    );

    test_table_ident(
        "`foo.bar.baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`foo.bar`.`baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`foo`.`bar.baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`foo`.`bar`.`baz`",
        Some("`foo`.`bar`.`baz`"),
        vec![
            Ident::with_quote('`', "foo"),
            Ident::with_quote('`', "bar"),
            Ident::with_quote('`', "baz"),
        ],
    );

    test_table_ident(
        "`5abc.dataField`",
        Some("`5abc`.`dataField`"),
        vec![
            Ident::with_quote('`', "5abc"),
            Ident::with_quote('`', "dataField"),
        ],
    );

    test_table_ident(
        "`_5abc.da-sh-es`",
        Some("`_5abc`.`da-sh-es`"),
        vec![
            Ident::with_quote('`', "_5abc"),
            Ident::with_quote('`', "da-sh-es"),
        ],
    );

    test_table_ident(
        "foo-bar.baz-123",
        Some("foo-bar.baz-123"),
        vec![Ident::new("foo-bar"), Ident::new("baz-123")],
    );

    test_table_ident_err("foo-`bar`");
    test_table_ident_err("`foo`-bar");
    test_table_ident_err("foo-123a");
    test_table_ident_err("foo - bar");
    test_table_ident_err("123-bar");
    test_table_ident_err("bar-");
}

#[test]
fn parse_hyphenated_table_identifiers() {
    bigquery().one_statement_parses_to(
        "select * from foo-bar f join baz-qux b on f.id = b.id",
        "SELECT * FROM foo-bar AS f JOIN baz-qux AS b ON f.id = b.id",
    );

    bigquery().verified_stmt(
        "CREATE TABLE data-ci.spreadsheets.api_clients_revenue AS SELECT * FROM corp-business-intelligence.spreadsheets.api_clients_revenue",
    );

    assert_eq!(
        bigquery()
            .verified_only_select_with_canonical(
                "SELECT foo-bar.x FROM t",
                "SELECT foo - bar.x FROM t"
            )
            .projection[0],
        SelectItem::UnnamedExpr(
            Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("foo").empty_span())),
                op: BinaryOperator::Minus,
                right: Box::new(Expr::CompoundIdentifier(
                    vec![Ident::new("bar"), Ident::new("x"),].empty_span()
                ))
            }
            .empty_span()
        )
        .empty_span()
    );

    let error_sql = "select foo-bar.* from foo-bar";
    assert!(bigquery().parse_sql_statements(error_sql).is_err());

    bigquery().verified_stmt(
        "CREATE MATERIALIZED VIEW project-id.my_dataset.my_mv_table AS SELECT date FROM project-id.my_dataset.my_base_table",
    );
    bigquery().verified_stmt("CREATE VIEW project-id.my_dataset.my_view AS SELECT 1");
}

#[test]
fn parse_table_time_travel() {
    let version = "2023-08-18 23:08:18".to_string();
    let sql = format!("SELECT 1 FROM t1 FOR SYSTEM_TIME AS OF '{version}'");
    let select = bigquery().verified_only_select(&sql);
    assert_eq!(
        select.from,
        vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(vec![Ident::new("t1")]),
                alias: None,
                args: None,
                with_hints: vec![],
                version: Some(TableVersion::ForSystemTimeAsOf(Expr::Value(
                    Value::SingleQuotedString(version)
                ))),
                partitions: vec![],
                with_ordinality: false,
            },
            joins: vec![],
        },]
    );

    let sql = "SELECT 1 FROM t1 FOR SYSTEM TIME AS OF 'some_timestamp'".to_string();
    assert!(bigquery().parse_sql_statements(&sql).is_err());
}

#[test]
fn parse_join_constraint_unnest_alias() {
    assert_eq!(
        only(
            bigquery()
                .verified_only_select("SELECT * FROM t1 JOIN UNNEST(t1.a) AS f ON c1 = c2")
                .from
        )
        .joins,
        vec![Join {
            relation: TableFactor::UNNEST {
                alias: table_alias("f"),
                array_exprs: vec![Expr::CompoundIdentifier(
                    vec![Ident::new("t1"), Ident::new("a")].empty_span()
                )],
                with_offset: false,
                with_offset_alias: None,
                with_ordinality: false,
            },
            join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("c1").empty_span())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Identifier(Ident::new("c2").empty_span())),
            })),
        }]
    );
}

#[test]
fn parse_trailing_comma() {
    for (sql, canonical) in [
        ("SELECT a,", "SELECT a"),
        ("SELECT 1,", "SELECT 1"),
        ("SELECT 1,2,", "SELECT 1, 2"),
        ("SELECT a, b,", "SELECT a, b"),
        ("SELECT a, b AS c,", "SELECT a, b AS c"),
        ("SELECT a, b AS c, FROM t", "SELECT a, b AS c FROM t"),
        ("SELECT a, b, FROM t", "SELECT a, b FROM t"),
        ("SELECT a, b, LIMIT 1", "SELECT a, b LIMIT 1"),
        ("SELECT a, (SELECT 1, )", "SELECT a, (SELECT 1)"),
        // Clause keywords after a trailing comma must terminate the projection
        // list even when immediately followed by `(` (subquery in FROM).
        // PR-7170: dbt-generated BigQuery `SELECT a, FROM (SELECT ...)`
        (
            "SELECT a, FROM (SELECT 1 AS a, 2 AS b)",
            "SELECT a FROM (SELECT 1 AS a, 2 AS b)",
        ),
        (
            "SELECT *, FROM (SELECT 1 AS a)",
            "SELECT * FROM (SELECT 1 AS a)",
        ),
    ] {
        bigquery().one_statement_parses_to(sql, canonical);
    }
}

#[test]
fn parse_cast_type() {
    let sql = r#"SELECT SAFE_CAST(1 AS INT64)"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_date_format() {
    let sql =
        r#"SELECT CAST(date_valid_from AS DATE FORMAT 'YYYY-MM-DD') AS date_valid_from FROM foo"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_time_format() {
    let sql = r#"SELECT CAST(TIME '21:30:00' AS STRING FORMAT 'PM') AS date_time_to_string"#;
    bigquery().verified_only_select(sql);
}

#[test]
#[ignore] // TODO: fix
fn parse_cast_timestamp_format_tz() {
    let sql = r#"SELECT CAST(TIMESTAMP '2008-12-25 00:00:00+00:00' AS STRING FORMAT 'TZH' AT TIME ZONE 'Asia/Kolkata') AS date_time_to_string"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_string_to_bytes_format() {
    let sql = r#"SELECT CAST('Hello' AS BYTES FORMAT 'ASCII') AS string_to_bytes"#;
    bigquery().verified_only_select(sql);
}

#[test]
fn parse_cast_bytes_to_string_format() {
    let sql = r#"SELECT CAST(B'\x48\x65\x6c\x6c\x6f' AS STRING FORMAT 'ASCII') AS bytes_to_string"#;
    bigquery_unescaped().verified_only_select(sql);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select_with_canonical(sql, "");
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
            "SELECT * FROM customers WHERE name {}LIKE '%a' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select(sql);
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
        let select = bigquery().verified_only_select_with_canonical(sql, "");
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
        let select = bigquery().verified_only_select_with_canonical(sql, "");
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

#[test]
fn parse_array_agg_func() {
    for sql in [
        "SELECT ARRAY_AGG(x ORDER BY x) AS a FROM T",
        "SELECT ARRAY_AGG(x ORDER BY x LIMIT 2) FROM tbl",
        "SELECT ARRAY_AGG(DISTINCT x ORDER BY x LIMIT 2) FROM tbl",
    ] {
        bigquery().verified_stmt(sql);
    }
}

#[test]
fn test_select_wildcard_with_except() {
    let select = bigquery_and_generic().verified_only_select("SELECT * EXCEPT (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_except: Some(ExceptSelectItem {
            first_element: Ident::new("col_a").empty_span(),
            additional_elements: vec![],
        }),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = bigquery_and_generic()
        .verified_only_select("SELECT * EXCEPT (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_except: Some(ExceptSelectItem {
            first_element: Ident::new("department_id").empty_span(),
            additional_elements: vec![Ident::new("employee_id").empty_span()],
        }),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    assert_eq!(
        bigquery_and_generic()
            .parse_sql_statements("SELECT * EXCEPT () FROM employee_table")
            .unwrap_err()
            .to_string(),
        "sql parser error: Expected identifier, found: )\nNear `SELECT * EXCEPT ()`"
    );
}

#[test]
fn test_select_agg_ignore_nulls() {
    bigquery().one_statement_parses_to(
        "SELECT last_value(user_id IGNORE NULLS) OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
        "SELECT last_value(user_id) IGNORE NULLS OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
    );
}

#[test]
fn test_select_agg_order_by() {
    bigquery().verified_only_select(
        "SELECT last_value(user_id ORDER BY user_id) OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
    );
}

#[test]
fn test_select_agg_ignore_nulls_order_by() {
    bigquery().one_statement_parses_to(
        "SELECT last_value(user_id IGNORE NULLS ORDER BY user_id) OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
        "SELECT last_value(user_id ORDER BY user_id) IGNORE NULLS OVER (PARTITION BY anonymous_id ORDER BY tstamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS user_id FROM table1",
    );
}

#[test]
fn test_select_wildcard_with_replace() {
    let select = bigquery_and_generic()
        .verified_only_select(r#"SELECT * REPLACE ('widget' AS item_name) FROM orders"#);
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![Box::new(ReplaceSelectElement {
                expr: Expr::Value(Value::SingleQuotedString("widget".to_owned())),
                column_name: Ident::new("item_name"),
                as_keyword: true,
            })],
        }),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = bigquery_and_generic().verified_only_select(
        r#"SELECT * REPLACE (quantity / 2 AS quantity, 3 AS order_id) FROM orders"#,
    );
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_replace: Some(ReplaceSelectItem {
            items: vec![
                Box::new(ReplaceSelectElement {
                    expr: Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("quantity").empty_span())),
                        op: BinaryOperator::Divide,
                        right: Box::new(Expr::Value(number("2"))),
                    },
                    column_name: Ident::new("quantity"),
                    as_keyword: true,
                }),
                Box::new(ReplaceSelectElement {
                    expr: Expr::Value(number("3")),
                    column_name: Ident::new("order_id"),
                    as_keyword: true,
                }),
            ],
        }),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);
}

fn bigquery() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {})],
        options: None,
    }
}

fn bigquery_unescaped() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {})],
        options: Some(ParserOptions::new().with_unescape(false)),
    }
}

fn bigquery_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(BigQueryDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

#[test]
fn parse_map_access_offset() {
    let sql = "SELECT d[offset(0)]";
    let _select = bigquery().verified_only_select(sql);
    assert_eq!(
        _select.projection[0],
        SelectItem::UnnamedExpr(
            Expr::MapAccess {
                column: Box::new(Expr::Identifier(
                    Ident {
                        value: "d".to_string(),
                        quote_style: None,
                    }
                    .empty_span()
                )),
                keys: vec![Expr::Function(Function {
                    name: ObjectName(vec!["offset".into()]),
                    args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        number("0")
                    ))),],
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
                })],
            }
            .empty_span()
        )
        .empty_span()
    );

    // test other operators
    for sql in [
        "SELECT d[SAFE_OFFSET(0)]",
        "SELECT d[ORDINAL(0)]",
        "SELECT d[SAFE_ORDINAL(0)]",
    ] {
        bigquery().verified_only_select(sql);
    }
}

#[test]
fn test_array_agg_over() {
    let sql = r"SELECT array_agg(account_combined_id) OVER (PARTITION BY shareholder_id ORDER BY date_from ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS previous_combined_id FROM foo";
    bigquery().verified_only_select(sql);
}

#[test]
fn test_trim() {
    bigquery_unescaped().verified_only_select(r#"SELECT CAST(TRIM(NULLIF(TRIM(JSON_QUERY(json_dump, "$.email_verified")), ''), '\"') AS BOOL) AS is_email_verified FROM foo"#);
    bigquery_unescaped().verified_only_select(r#"SELECT CAST(LTRIM(NULLIF(TRIM(JSON_QUERY(json_dump, "$.email_verified")), ''), '\"') AS BOOL) AS is_email_verified FROM foo"#);
    bigquery_unescaped().verified_only_select(r#"SELECT CAST(RTRIM(NULLIF(TRIM(JSON_QUERY(json_dump, "$.email_verified")), ''), '\"') AS BOOL) AS is_email_verified FROM foo"#);
}

#[test]
fn test_external_query() {
    bigquery().verified_only_select("SELECT * FROM EXTERNAL_QUERY(\"projects/bq-proj/locations/EU/connections/connection_name\",\"SELECT * FROM public.auth0_user \")");
    bigquery().verified_only_select("SELECT * FROM EXTERNAL_QUERY(\"projects/bq-proj/locations/EU/connections/connection_name\",\"SELECT * FROM public.auth0_user \", '{\"default_type_for_decimal_columns\":\"numeric\"}')");
    bigquery().verified_only_select("SELECT * FROM EXTERNAL_QUERY('connection_id','''SELECT * FROM customers AS c ORDER BY c.customer_id''')");
    bigquery().verified_only_select("SELECT * FROM EXTERNAL_QUERY('conn','SELECT 1') AS rq");
}

#[test]
fn parse_extract_weekday() {
    let sql = "SELECT EXTRACT(WEEK(MONDAY) FROM d)";
    let select = bigquery_and_generic().verified_only_select(sql);
    assert_eq!(
        &Expr::Extract {
            field: DateTimeField::Week(Some(Ident::new("MONDAY"))),
            expr: Box::new(Expr::Identifier(Ident::new("d").empty_span())),
        },
        expr_from_projection(only(&select.projection)),
    );
}

#[test]
fn test_select_as_struct() {
    bigquery().verified_only_select("SELECT * FROM (SELECT AS VALUE STRUCT(123 AS a, false AS b))");
    let select = bigquery().verified_only_select("SELECT AS STRUCT 1 AS a, 2 AS b");
    assert_eq!(Some(ValueTableMode::AsStruct), select.value_table_mode);
    assert_eq!(None, select.distinct);

    // BigQuery supports SELECT DISTINCT AS STRUCT
    let select = bigquery().verified_only_select("SELECT DISTINCT AS STRUCT 1 AS a, 2 AS b");
    assert_eq!(Some(ValueTableMode::AsStruct), select.value_table_mode);
    assert_eq!(Some(Distinct::Distinct), select.distinct);
}

#[test]
fn test_select_as_value() {
    bigquery().verified_only_select(
        "SELECT * FROM (SELECT AS VALUE STRUCT(5 AS star_rating, false AS up_down_rating))",
    );
    let select = bigquery().verified_only_select("SELECT AS VALUE STRUCT(1 AS a, 2 AS b) AS xyz");
    assert_eq!(Some(ValueTableMode::AsValue), select.value_table_mode);
}

#[test]
fn test_select_array_item_field() {
    bigquery().verified_only_select(
        "SELECT arr[SAFE_OFFSET(0)].id AS arr_id FROM `proj`.`dataset`.`table`",
    );
}

#[test]
fn test_select_array_item_field_in_function() {
    bigquery().verified_only_select(
        "SELECT LOWER(arr[SAFE_OFFSET(0)].id) AS arr_id FROM `proj`.`dataset`.`table`",
    );
}

#[test]
fn test_typed_array_constructor() {
    // Simple typed array
    bigquery().verified_expr("ARRAY<INT64>[1, 2, 3]");

    // Empty typed array
    bigquery().verified_expr("ARRAY<INT64>[]");

    // Typed array with STRUCT type
    bigquery().verified_expr("ARRAY<STRUCT<x INT64>>[STRUCT(1), STRUCT(2)]");

    // Typed array with STRUCT containing multiple fields
    bigquery().verified_expr(
        "ARRAY<STRUCT<warehouse STRING, state STRING>>[('warehouse #1', 'WA'), ('warehouse #2', 'CA')]",
    );

    // Empty typed array with complex type
    bigquery().verified_expr("ARRAY<STRUCT<x INT64>>[]");

    // Typed array in SELECT context
    bigquery().verified_only_select("SELECT ARRAY<INT64>[1, 2, 3] AS arr");

    // Typed array in UNNEST
    bigquery().verified_only_select("SELECT * FROM UNNEST(ARRAY<STRUCT<x INT64>>[])");
}

#[test]
fn test_select_json_field() {
    let _select = bigquery().verified_only_select(
        "SELECT JSON_VALUE(PARSE_JSON(response_json).user.username) AS arr_id FROM `proj`.`dataset`.`table`",
    );

    assert_eq!(
        SelectItem::ExprWithAlias {
            expr: Expr::Function(Function {
                name: ObjectName(vec!["JSON_VALUE".into()]),
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                    Expr::JsonAccess {
                        left: Box::new(Expr::Function(Function {
                            name: ObjectName(vec!["PARSE_JSON".into()]),
                            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                Expr::Identifier(
                                    Ident::new("response_json".to_string()).empty_span()
                                )
                            ))],
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
                        })),
                        operator: JsonOperator::Period,
                        right: Box::new(Expr::Value(Value::UnQuotedString(
                            "user.username".to_string()
                        ))),
                    }
                ))],
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
            })
            .empty_span(),
            alias: Ident::new("arr_id").empty_span(),
        }
        .empty_span(),
        _select.projection[0]
    );
}

#[test]
fn test_bigquery_single_line_comment_tokenize() {
    let sql = "CREATE TABLE# this is a comment \ntable_1";
    let dialect = BigQueryDialect {};
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
}

#[test]
fn test_bigquery_single_line_comment_parsing() {
    bigquery().verified_only_select_with_canonical(
        "SELECT book# this is a comment \n FROM library",
        "SELECT book FROM library",
    );
}

#[test]
fn test_regexp_string_double_quote() {
    bigquery_unescaped().verified_stmt(r"SELECT 'I\'m fine'");
    bigquery_unescaped().verified_stmt(r"SELECT 'I\\\'m fine'");
    bigquery_unescaped().verified_stmt(r#"SELECT 'I''m fine'"#);
    bigquery_unescaped().verified_stmt(r#"SELECT "I'm ''fine''""#);
    bigquery_unescaped().verified_stmt(r#"SELECT "I\\\"m fine""#);
    bigquery_unescaped().verified_stmt(r#"SELECT "[\"\\[\\]]""#);
}

#[test]
fn test_create_table_with_partition_by() {
    bigquery().verified_stmt(
        "CREATE TABLE mytable (id INT64, timestamp TIMESTAMP) PARTITION BY DATE(timestamp)",
    );
}

#[test]
fn test_create_external_table_with_options() {
    bigquery().verified_stmt(
        r#"CREATE EXTERNAL TABLE mytable (id INT64, timestamp TIMESTAMP) OPTIONS (sheet_range = "synq", skip_leading_rows = 1, format = "GOOGLE_SHEETS", uris = ["https://docs.google.com/spreadsheets/d/1g3xwWi1r-Ln2VVwv4mswwmqyfMeoJglv-MS80ywASGI/edit#gid=0"])"#,
    );
}

#[test]
fn test_create_external_table_with_partition_columns() {
    // BigQuery `WITH PARTITION COLUMNS` (auto-detect) and with explicit list, plus
    // `WITH CONNECTION` — clauses are accepted; their content is not preserved in AST.
    bigquery()
        .parse_sql_statements(
            "CREATE EXTERNAL TABLE dataset.AutoHivePartitionedTable \
             WITH PARTITION COLUMNS \
             OPTIONS (uris = ['gs://bucket/path/*'], format = 'PARQUET')",
        )
        .unwrap();
    bigquery()
        .parse_sql_statements(
            "CREATE EXTERNAL TABLE dataset.CustomHivePartitionedTable \
             WITH PARTITION COLUMNS (field_1 STRING, field_2 INT64) \
             OPTIONS (uris = ['gs://bucket/path/*'], format = 'PARQUET')",
        )
        .unwrap();
    bigquery()
        .parse_sql_statements(
            "CREATE OR REPLACE EXTERNAL TABLE mydataset.newtable (x INT64, y STRING) \
             WITH CONNECTION myproject.us.myconnection \
             OPTIONS(format = \"PARQUET\")",
        )
        .unwrap();
}

#[test]
fn test_create_table_with_options() {
    bigquery().verified_stmt(
        "CREATE TABLE `pr`.`ts`.`salesforce_accounts` (account_name STRING) OPTIONS (description = \"Account name\")",
    );
}

#[test]
fn test_create_table_column_options() {
    bigquery().verified_stmt(
        "CREATE TABLE `pr`.`ts`.`salesforce_accounts` (account_name STRING OPTIONS(description = \"Account name\", label = \"dev\"))",
    );
}

#[test]
fn test_create_table_column_field_options() {
    bigquery().verified_stmt(
        "CREATE TABLE `pr`.`ts`.`salesforce_accounts` (campaign_metrics STRUCT<impression_count INT64 OPTIONS(description = \"Amount of impressions\"), click_count INT64 OPTIONS(description = \"Amount of clicks\")> OPTIONS(description = \"A group of campaign metrics\"))",
    );
}

#[test]
fn test_create_view_options() {
    bigquery().verified_stmt(
        "CREATE VIEW `myproject`.`mydataset`.`newview` OPTIONS(friendly_name = \"newview\", description = \"a view that expires in 2 days\") AS SELECT col_1 FROM `myproject`.`mydataset`.`mytable`",
    );
}

#[test]
fn test_alter_table_set_options() {
    bigquery().verified_stmt("ALTER TABLE tbl SET OPTIONS(description = \"Desc.\")");
}

#[test]
fn test_alter_view_set_options() {
    bigquery().verified_stmt("ALTER VIEW tbl SET OPTIONS (description = \"Desc.\")");
}

#[test]
fn test_alter_schema_set_options() {
    bigquery()
        .verified_stmt("ALTER SCHEMA mydataset SET OPTIONS (default_table_expiration_days = 3.75)");
}

#[test]
fn test_alter_table_alter_column_set_options() {
    bigquery()
        .verified_stmt("ALTER TABLE mydataset.mytable ALTER COLUMN price SET OPTIONS(description = 'Price per unit')");
}

#[test]
fn test_alter_set_options_labels_tuple() {
    bigquery()
        .verified_stmt("ALTER SCHEMA mydataset SET OPTIONS (labels = [('sensitivity', 'high')])");
}

#[test]
fn test_create_table_cluster_by() {
    bigquery().one_statement_parses_to(
        "CREATE TABLE `myproject`.`mydataset`.`mytable` (service_id STRING, account_id STRING, state STRING, valid_from TIMESTAMP, valid_to TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(valid_from, MONTH) CLUSTER BY account_id, state OPTIONS (description = \"State of an service at a point in time\")",
        "CREATE TABLE `myproject`.`mydataset`.`mytable` (service_id STRING, account_id STRING, state STRING, valid_from TIMESTAMP, valid_to TIMESTAMP) PARTITION BY TIMESTAMP_TRUNC(valid_from, MONTH) CLUSTER BY (account_id, state) OPTIONS (description = \"State of an service at a point in time\")"
    );
}
#[test]
fn test_create_table_cluster_by_no_parens() {
    bigquery().one_statement_parses_to(
        "CREATE OR REPLACE TABLE `proj`.`ds`.`tbl` CLUSTER BY account_number OPTIONS (description = \"\"\"summary\"\"\") AS (SELECT 1)",
        "CREATE OR REPLACE TABLE `proj`.`ds`.`tbl` CLUSTER BY (account_number) OPTIONS (description = \"summary\") AS (SELECT 1)"
    );
}

#[test]
fn test_create_table_primary_key_not_enforced() {
    bigquery().verified_stmt(
        "CREATE TABLE `myproject`.`mydataset`.`mytable` (id INT64, PRIMARY KEY (id) NOT ENFORCED)",
    );
}

#[test]
fn test_options_expression() {
    bigquery().verified_stmt(
        "CREATE TABLE `myproject`.`mydataset`.`mytable` (id INT64) OPTIONS (max_staleness = INTERVAL '0-0 0 0:15:0' YEAR TO SECOND)",
    );
}

#[test]
fn test_create_table_options_expression() {
    // BigQuery allows triple-quoted string literals (`"""..."""`) in OPTIONS.
    // The tokenizer recognizes them as a single literal and Display normalizes
    // to the plain double-quoted form when the body contains no literal quotes.
    bigquery().one_statement_parses_to(
        "CREATE OR REPLACE TABLE `proj`.`ds`.`tbl` OPTIONS (description = \"\"\"Stream base\"\"\") AS (SELECT * FROM `proj`.`ds`.`other`)",
        "CREATE OR REPLACE TABLE `proj`.`ds`.`tbl` OPTIONS (description = \"Stream base\") AS (SELECT * FROM `proj`.`ds`.`other`)",
    );
    // Triple-quoted with embedded double quotes survives the roundtrip through
    // escape_double_quote_string (which doubles embedded `"`).
    bigquery().one_statement_parses_to(
        r#"CREATE TABLE t (a INT64) OPTIONS (description = """has "q" inside""")"#,
        r#"CREATE TABLE t (a INT64) OPTIONS (description = "has ""q"" inside")"#,
    );
    // Triple-quoted with backslash-escaped quotes inside (`\"`): the escape
    // sequence is part of the string content and must not participate in
    // closing-quote detection. Display roundtripping doubles the quote, so we
    // just verify the input parses successfully.
    bigquery()
        .parse_sql_statements(
            r#"CREATE TABLE t (a INT64) OPTIONS (description = """\"escaped\"""")"#,
        )
        .unwrap();
}

#[test]
fn test_create_table_copy() {
    bigquery().one_statement_parses_to(
        "CREATE OR REPLACE TABLE `db_1.sch_1.tbl_1` COPY `db_1.sch_2.tbl_1`",
        "CREATE OR REPLACE TABLE `db_1`.`sch_1`.`tbl_1` COPY `db_1`.`sch_2`.`tbl_1`",
    );
    bigquery().verified_stmt("CREATE TABLE t COPY src_table");
}

#[test]
fn test_create_table_options_empty() {
    bigquery().one_statement_parses_to(
        "CREATE OR REPLACE TABLE `myproject`.`mydataset`.`mytable` OPTIONS () AS (SELECT * FROM `myproject`.`mydataset`.`othertable`)",
        "CREATE OR REPLACE TABLE `myproject`.`mydataset`.`mytable` AS (SELECT * FROM `myproject`.`mydataset`.`othertable`)",
    );
}

#[test]
fn test_create_schema_options() {
    bigquery().one_statement_parses_to(
        "CREATE SCHEMA mydataset OPTIONS(is_case_insensitive = TRUE)",
        "CREATE SCHEMA mydataset OPTIONS(is_case_insensitive = true)",
    );
    bigquery().verified_stmt("CREATE SCHEMA mydataset DEFAULT COLLATE 'und:ci'");
}

#[test]
fn test_create_table_default_collate() {
    bigquery().verified_stmt(
        "CREATE TABLE mydataset.mytable (number INT64, word STRING) DEFAULT COLLATE 'und:ci'",
    );
}

#[test]
fn test_aggregate_having_bound() {
    bigquery().verified_stmt("SELECT ANY_VALUE(fruit HAVING MAX sold) FROM fruits");
    bigquery().verified_stmt("SELECT ANY_VALUE(fruit HAVING MIN sold) FROM fruits");
    bigquery().verified_stmt(
        "SELECT category, ANY_VALUE(product HAVING MAX price), ANY_VALUE(product HAVING MIN cost) FROM products GROUP BY category",
    );
}

#[test]
fn test_alter_table_set_default_collate() {
    bigquery().verified_stmt("ALTER TABLE mydataset.mytable SET DEFAULT COLLATE ''");
    bigquery().one_statement_parses_to(
        "ALTER TABLE table_name SET DEFAULT COLLATE collate_specification",
        "ALTER TABLE table_name SET DEFAULT COLLATE 'collate_specification'",
    );
}

#[test]
fn test_table_cte() {
    bigquery().verified_only_select("WITH table AS (SELECT * FROM tbl) SELECT * FROM table");
}

#[test]
fn test_qualify_before_window() {
    // BigQuery supports QUALIFY before WINDOW clause
    // Display always outputs WINDOW before QUALIFY
    bigquery().one_statement_parses_to(
        "SELECT col_1, SUM(col_2) OVER w FROM tbl QUALIFY ROW_NUMBER() OVER w = 1 WINDOW w AS (PARTITION BY col_1)",
        "SELECT col_1, SUM(col_2) OVER w FROM tbl WINDOW w AS (PARTITION BY col_1) QUALIFY ROW_NUMBER() OVER w = 1",
    );

    // Standard ordering (WINDOW before QUALIFY) also works
    bigquery().verified_only_select(
        "SELECT col_1, SUM(col_2) OVER w FROM tbl WINDOW w AS (PARTITION BY col_1) QUALIFY ROW_NUMBER() OVER w = 1",
    );
}

#[test]
fn test_json_number_start() {
    bigquery().verified_only_select("SELECT field.5k_clients_target AS clients_5k_target FROM tbl");
}

#[test]
fn parse_bigquery_create_table_function() {
    // CREATE TABLE FUNCTION with RETURNS TABLE<...> and AS query (non-roundtrip due to backtick parsing)
    bigquery().one_statement_parses_to(
        "CREATE OR REPLACE TABLE FUNCTION mydataset.names_by_year(y INT64) RETURNS TABLE<name STRING, year INT64, total INT64> AS SELECT year, name, SUM(number) AS total FROM `bigquery-public-data.usa_names.usa_1910_current` WHERE year = y GROUP BY year, name",
        "",
    );

    // CREATE TABLE FUNCTION without RETURNS (inferred return type)
    bigquery().one_statement_parses_to(
        "CREATE OR REPLACE TABLE FUNCTION mydataset.names_by_year(y INT64) AS SELECT year, name, SUM(number) AS total FROM `bigquery-public-data.usa_names.usa_1910_current` WHERE year = y GROUP BY year, name",
        "",
    );

    // Simple CREATE TABLE FUNCTION - roundtrip
    bigquery().verified_stmt(
        "CREATE TABLE FUNCTION a(x INT64) RETURNS TABLE<q STRING, r INT64> AS SELECT s, t",
    );

    // Verify the AST structure
    let sql = "CREATE TABLE FUNCTION a(x INT64) RETURNS TABLE<q STRING, r INT64> AS SELECT s, t";
    match bigquery().verified_stmt(sql) {
        Statement::CreateFunction {
            or_replace,
            temporary,
            table_function,
            name,
            args,
            return_type,
            ..
        } => {
            assert!(!or_replace);
            assert!(!temporary);
            assert!(table_function);
            assert_eq!(name.to_string(), "a");
            assert!(args.is_some());
            assert!(return_type.is_some());
            match return_type.unwrap() {
                DataType::Table(fields) => {
                    assert_eq!(fields.len(), 2);
                }
                other => panic!("Expected DataType::Table, got {:?}", other),
            }
        }
        other => panic!("Expected CreateFunction, got {:?}", other),
    }
}

#[test]
fn parse_bigquery_format_function() {
    // format() as first select item
    bigquery().verified_stmt("SELECT format('%f', round(col_15, 6)) AS swap_spread FROM t2");

    // format() as non-first select item - tests trailing comma detection
    // with RESERVED_FOR_COLUMN_ALIAS keywords followed by '('
    bigquery().verified_stmt("SELECT col_1, format('test', col_2) AS c FROM t1");

    // format() in CTE
    bigquery().one_statement_parses_to(
        "WITH scope AS (SELECT col_1, format('test', col_2) AS c FROM t1) SELECT * FROM tbl_1",
        "",
    );
}

#[test]
fn parse_update_from_multiple_tables() {
    let sql = "UPDATE dataset.DetailedInventory SET supply_constrained = true FROM dataset.NewArrivals, dataset.Warehouse WHERE DetailedInventory.product = NewArrivals.product AND NewArrivals.warehouse = Warehouse.warehouse AND Warehouse.state = 'WA'";
    bigquery().verified_stmt(sql);
}

#[test]
fn parse_cast_field_access() {
    // Single-level field access on CAST
    bigquery().verified_stmt("SELECT CAST(col AS STRUCT<fld1 INT64>).fld1");
    // Multi-level field access on CAST
    bigquery().one_statement_parses_to(
        "SELECT CAST(col AS STRUCT<fld1 STRUCT<fld2 INT64>>).fld1.fld2",
        "SELECT CAST(col AS STRUCT<fld1 STRUCT<fld2 INT64>>).fld1.fld2",
    );
}

#[test]
fn parse_expr_wildcard() {
    // BigQuery struct wildcard expansion: CAST(expr AS STRUCT<...>).*
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_replace
    bigquery().verified_stmt("SELECT CAST(STRUCT(col1) AS STRUCT<BOOL>).* FROM t");
    bigquery().verified_stmt(
        "SELECT CAST(STRUCT((SELECT ARRAY_AGG(col_1) AS emails FROM tbl_1 WHERE col_2 IS NULL)) AS STRUCT<ARRAY<STRING>>).* FROM t",
    );
    // EXISTS adds a space in output: `EXISTS (` instead of `EXISTS(`
    bigquery().one_statement_parses_to(
        "SELECT CAST(STRUCT(EXISTS(SELECT 1 FROM tbl_1 WHERE col_1 = 'val')) AS STRUCT<BOOL>).* FROM t",
        "SELECT CAST(STRUCT(EXISTS (SELECT 1 FROM tbl_1 WHERE col_1 = 'val')) AS STRUCT<BOOL>).* FROM t",
    );
    // Generic function-call struct wildcard: `IF(...).*`
    bigquery().verified_stmt("SELECT IF(a IS NULL, b, c).* FROM t");
    bigquery().verified_stmt(
        "SELECT t.*, IF(NOT a.b IS NULL, a, c).*, x FROM t LEFT JOIN u ON t.a = u.a",
    );
}

#[test]
fn parse_wildcard_table() {
    // BigQuery wildcard table syntax
    // https://cloud.google.com/bigquery/docs/querying-wildcard-tables
    bigquery().verified_stmt("SELECT * FROM x.y*");
    // Backtick-quoted identifiers with dots get split into parts
    bigquery().one_statement_parses_to(
        "SELECT * FROM `project.dataset.table_prefix*`",
        "SELECT * FROM `project`.`dataset`.`table_prefix*`",
    );
    bigquery()
        .verified_stmt("SELECT * FROM x.y* WHERE _TABLE_SUFFIX BETWEEN '20230101' AND '20231231'");
}

#[test]
fn parse_bigquery_ml_predict() {
    // MODEL keyword-prefixed table reference (backtick dotted names split into parts)
    bigquery().one_statement_parses_to(
        "SELECT * FROM ML.PREDICT(MODEL `my_project.my_dataset.my_model`, (SELECT * FROM input_data))",
        "SELECT * FROM ML.PREDICT(MODEL `my_project`.`my_dataset`.`my_model`, (SELECT * FROM input_data))",
    );
    // MODEL with TABLE keyword
    bigquery().one_statement_parses_to(
        "SELECT * FROM ML.PREDICT(MODEL `mydataset.mymodel`, TABLE `mydataset.mytable`)",
        "SELECT * FROM ML.PREDICT(MODEL `mydataset`.`mymodel`, TABLE `mydataset`.`mytable`)",
    );
    // MODEL with subquery and STRUCT
    bigquery().one_statement_parses_to(
        "SELECT * FROM ML.PREDICT(MODEL `mydataset.mymodel`, (SELECT custom_label, column1, column2 FROM `mydataset.mytable`), STRUCT(0.55 AS threshold))",
        "SELECT * FROM ML.PREDICT(MODEL `mydataset`.`mymodel`, (SELECT custom_label, column1, column2 FROM `mydataset`.`mytable`), STRUCT(0.55 AS threshold))",
    );
    // ML.FORECAST with STRUCT
    bigquery().one_statement_parses_to(
        "SELECT * FROM ML.FORECAST(MODEL `mydataset.mymodel`, STRUCT(2 AS horizon))",
        "SELECT * FROM ML.FORECAST(MODEL `mydataset`.`mymodel`, STRUCT(2 AS horizon))",
    );
    // MODEL with unquoted names
    bigquery().verified_stmt(
        "SELECT * FROM ML.PREDICT(MODEL mydataset.mymodel, (SELECT * FROM input_data))",
    );
    // ML.TRANSLATE with TABLE and STRUCT
    bigquery().one_statement_parses_to(
        "SELECT * FROM ML.TRANSLATE(MODEL `mydataset.mytranslatemodel`, TABLE `mydataset.mybqtable`, STRUCT('translate_text' AS translate_mode, 'zh-CN' AS target_language_code))",
        "SELECT * FROM ML.TRANSLATE(MODEL `mydataset`.`mytranslatemodel`, TABLE `mydataset`.`mybqtable`, STRUCT('translate_text' AS translate_mode, 'zh-CN' AS target_language_code))",
    );
}

#[test]
fn parse_bigquery_vector_search() {
    // VECTOR_SEARCH with TABLE keyword
    bigquery().verified_stmt(
        "SELECT * FROM VECTOR_SEARCH(TABLE mydataset.base_table, 'column_to_search', TABLE mydataset.query_table)",
    );
    // VECTOR_SEARCH with named arguments
    bigquery().verified_stmt(
        "SELECT * FROM VECTOR_SEARCH(TABLE mydataset.base_table, 'column_to_search', TABLE mydataset.query_table, 'query_column_to_search', top_k => 2, distance_type => 'cosine')",
    );
}

#[test]
fn parse_bigquery_gap_fill() {
    // GAP_FILL with TABLE and named arguments
    bigquery().verified_stmt(
        "SELECT * FROM GAP_FILL(TABLE device_data, ts_column => 'time', bucket_width => INTERVAL '1' MINUTE)",
    );
}

#[test]
fn parse_bigquery_chained_subscript_access() {
    // col[OFFSET(0)].field
    bigquery().verified_stmt("SELECT col[OFFSET(0)].source_type AS x FROM t1");
    // col[SAFE_OFFSET(0)].field[SAFE_OFFSET(0)]
    bigquery().verified_stmt("SELECT col[SAFE_OFFSET(0)].source_ids[SAFE_OFFSET(0)] AS x FROM t1");
    // col[OFFSET(0)].field[OFFSET(0)] chained
    bigquery().verified_stmt("SELECT col[OFFSET(0)].ids[OFFSET(0)] AS x FROM t1");
    // Simple subscript still works
    bigquery().verified_stmt("SELECT col[OFFSET(0)] AS x FROM t1");
    // Subscript with field access
    bigquery().verified_stmt("SELECT col[0].field AS x FROM t1");
}

#[test]
fn parse_bigquery_empty_struct() {
    // STRUCT() - empty struct literal
    bigquery().verified_stmt("SELECT STRUCT()");
    // Empty struct as function argument
    bigquery().one_statement_parses_to(
        "SELECT * FROM ML.FORECAST(MODEL `mydataset.mymodel`, (SELECT * FROM t1), STRUCT())",
        "SELECT * FROM ML.FORECAST(MODEL `mydataset`.`mymodel`, (SELECT * FROM t1), STRUCT())",
    );
}

#[test]
fn parse_struct_field_not_null() {
    // BigQuery supports NOT NULL constraints on struct fields
    bigquery().verified_stmt(
        "CREATE TABLE t (x INT64 NOT NULL, y STRUCT<a ARRAY<STRING>, b BOOL NOT NULL, c FLOAT64> NOT NULL, z STRING)",
    );
    bigquery().verified_stmt("SELECT STRUCT<x INT64 NOT NULL>(1)");
    bigquery().verified_stmt("ALTER TABLE mydataset.mytable ADD COLUMN A STRUCT<B GEOGRAPHY, C ARRAY<INT64>, D INT64 NOT NULL, E TIMESTAMP OPTIONS(description = 'creation time')>");
}

#[test]
fn parse_bigquery_alter_table_drop_primary_key() {
    bigquery().verified_stmt("ALTER TABLE tab DROP PRIMARY KEY");
    bigquery().one_statement_parses_to(
        "ALTER TABLE myTable DROP PRIMARY KEY",
        "ALTER TABLE myTable DROP PRIMARY KEY",
    );
}

#[test]
fn parse_bigquery_unpivot_multi_column() {
    // Multi-column UNPIVOT with tuple value and tuple IN entries with string aliases
    bigquery().verified_stmt(
        "SELECT * FROM Produce UNPIVOT ((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 'semester_1', (Q3, Q4) AS 'semester_2'))",
    );

    // Multi-column UNPIVOT with tuple value and tuple IN entries with numeric aliases
    bigquery().verified_stmt(
        "SELECT * FROM Produce UNPIVOT ((first_half_sales, second_half_sales) FOR semesters IN ((Q1, Q2) AS 1, (Q3, Q4) AS 2))",
    );

    // Single-value in tuple syntax: (c) should roundtrip as plain c
    bigquery().one_statement_parses_to(
        "SELECT * FROM (SELECT * FROM `t`) AS a UNPIVOT((c) FOR c_name IN (v1, v2))",
        "SELECT * FROM (SELECT * FROM `t`) AS a UNPIVOT (c FOR c_name IN (v1, v2))",
    );

    // Multi-column UNPIVOT with tuple IN entries without aliases
    bigquery().verified_stmt(
        "SELECT * FROM tbl_1 UNPIVOT ((col_4, col_12, col_13, col_14) FOR col_15 IN ((col_9, col_30, col_31, col_32), (col_10, col_33, col_34, col_35), (col_11, col_36, col_37, col_38)))",
    );
}

#[test]
fn test_bigquery_window_named_ref() {
    // BigQuery supports WINDOW clause where a named window references another by name without parens
    bigquery().verified_stmt(
        "SELECT item, purchases, category, LAST_VALUE(item) OVER (c ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS most_popular FROM Produce WINDOW a AS (PARTITION BY category), b AS (a ORDER BY purchases), c AS b",
    );

    // Simple case: one window referencing another
    bigquery().verified_stmt("SELECT SUM(x) OVER (b) FROM t WINDOW a AS (ORDER BY x), b AS a");
}

#[test]
fn test_bigquery_cluster_as_identifier() {
    // CLUSTER is reserved only because of `CLUSTER BY` (Hive/Spark SELECT
    // clause and BigQuery DDL). When the next token isn't BY it should parse
    // as a regular alias — real-world: customers name JOIN aliases / columns
    // `cluster`.
    bigquery().one_statement_parses_to(
        "SELECT * FROM t LEFT JOIN budgets cluster ON budgets.x = cluster.y",
        "SELECT * FROM t LEFT JOIN budgets AS cluster ON budgets.x = cluster.y",
    );
    bigquery().verified_stmt("SELECT cluster.x AS cluster FROM (SELECT 1 AS x) AS cluster");
    // CLUSTER BY in Hive/Spark must still be recognized as the keyword.
    sqlparser::test_utils::TestedDialects {
        dialects: vec![Box::new(sqlparser::dialect::HiveDialect {})],
        options: None,
    }
    .verified_stmt("SELECT * FROM t CLUSTER BY x");
}

#[test]
fn test_bigquery_window_order_by_offset_alias() {
    // BigQuery's `UNNEST(arr) WITH OFFSET AS offset` introduces a column named
    // `offset` (overlapping the OFFSET keyword). The window spec's ORDER BY
    // must accept it as a regular identifier — previously the trailing-comma
    // logic left over from BigQuery projections treated `, offset` as a
    // trailing-comma terminator and looked for a window frame instead.
    bigquery().verified_stmt(
        "SELECT ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY version_index, offset ASC) AS version_index FROM t",
    );
    // Same alias used in PARTITION BY.
    bigquery().verified_stmt("SELECT SUM(x) OVER (PARTITION BY id, offset ORDER BY x) FROM t");
}

#[test]
fn parse_bigquery_full_union_by_name() {
    // FULL UNION ALL BY NAME - BigQuery set operation matching columns by name with outer-join semantics
    bigquery().verified_query("SELECT * FROM t1 FULL UNION ALL BY NAME SELECT * FROM t2");

    // FULL OUTER UNION ALL BY NAME - OUTER is optional
    bigquery().one_statement_parses_to(
        "SELECT * FROM t1 FULL OUTER UNION ALL BY NAME SELECT * FROM t2",
        "SELECT * FROM t1 FULL UNION ALL BY NAME SELECT * FROM t2",
    );

    // FULL UNION BY NAME (without ALL)
    bigquery().verified_query("SELECT * FROM t1 FULL UNION BY NAME SELECT * FROM t2");

    // Inside a CTE
    bigquery().verified_query(
        "WITH cte AS (SELECT * FROM t1 FULL UNION ALL BY NAME SELECT * FROM t2) SELECT * FROM cte",
    );
}

#[test]
fn parse_merge_not_matched_by_source() {
    // BigQuery: WHEN NOT MATCHED BY SOURCE THEN DELETE
    bigquery().one_statement_parses_to(
        "MERGE dataset.Inventory T USING dataset.NewArrivals S ON FALSE \
        WHEN NOT MATCHED AND product LIKE '%washer%' THEN INSERT (product, quantity) VALUES (product, quantity) \
        WHEN NOT MATCHED BY SOURCE AND product LIKE '%washer%' THEN DELETE",
        "MERGE dataset.Inventory AS T USING dataset.NewArrivals AS S ON false \
        WHEN NOT MATCHED AND product LIKE '%washer%' THEN INSERT (product, quantity) VALUES (product, quantity) \
        WHEN NOT MATCHED BY SOURCE AND product LIKE '%washer%' THEN DELETE",
    );

    // BigQuery: WHEN NOT MATCHED BY SOURCE THEN UPDATE SET ...
    bigquery().verified_stmt(
        "MERGE T USING S ON T.id = S.id \
        WHEN NOT MATCHED BY SOURCE THEN UPDATE SET T.status = 'DELETED'",
    );

    // BigQuery: WHEN NOT MATCHED BY TARGET (same as NOT MATCHED)
    bigquery().one_statement_parses_to(
        "MERGE T USING S ON T.id = S.id \
        WHEN NOT MATCHED BY TARGET THEN INSERT (id) VALUES (S.id)",
        "MERGE T USING S ON T.id = S.id \
        WHEN NOT MATCHED THEN INSERT (id) VALUES (S.id)",
    );

    // BigQuery: INSERT ROW shorthand (insert all source columns)
    bigquery().verified_stmt("MERGE T USING S ON T.id = S.id WHEN NOT MATCHED THEN INSERT ROW");
    bigquery().verified_stmt(
        "MERGE T USING S ON T.id = S.id \
        WHEN NOT MATCHED AND S.active THEN INSERT ROW \
        WHEN NOT MATCHED BY SOURCE THEN DELETE",
    );
}

#[test]
fn parse_deferred_join_on_clause() {
    // BigQuery supports deferred ON clause for nested joins:
    // FROM A INNER JOIN B LEFT JOIN C ON c_cond ON a_b_cond
    // The last ON applies to the preceding join with no constraint
    bigquery().one_statement_parses_to(
        "SELECT * FROM tbl_a INNER JOIN tbl_b LEFT JOIN tbl_c ON tbl_c.id = tbl_b.id ON tbl_a.id = tbl_b.id",
        "SELECT * FROM tbl_a JOIN tbl_b ON tbl_a.id = tbl_b.id LEFT JOIN tbl_c ON tbl_c.id = tbl_b.id",
    );
}

#[test]
fn parse_deferred_join_on_followed_by_more_joins() {
    // After resolving the deferred ON, additional joins may follow.
    // FROM A JOIN B JOIN C ON c_cond ON a_b_cond JOIN D ON d_cond
    bigquery().one_statement_parses_to(
        "SELECT * FROM a LEFT JOIN b LEFT JOIN c ON b.x = c.x ON a.y = b.y LEFT JOIN d ON d.z = a.z",
        "SELECT * FROM a LEFT JOIN b ON a.y = b.y LEFT JOIN c ON b.x = c.x LEFT JOIN d ON d.z = a.z",
    );
}

#[test]
fn parse_deferred_join_on_in_cte() {
    // Test exact pattern from customer_bigquery corpus files
    bigquery().one_statement_parses_to(
        "WITH add_first AS (SELECT a.*, b.x, c.v FROM b INNER JOIN a LEFT JOIN c ON c.id = a.id ON b.id = a.id WHERE c.v <= a.v), result AS (SELECT * FROM add_first) SELECT * FROM result",
        "WITH add_first AS (SELECT a.*, b.x, c.v FROM b JOIN a ON b.id = a.id LEFT JOIN c ON c.id = a.id WHERE c.v <= a.v), result AS (SELECT * FROM add_first) SELECT * FROM result",
    );
}

#[test]
fn parse_tablesample_percent() {
    // BigQuery TABLESAMPLE supports PERCENT keyword: TABLESAMPLE SYSTEM (10 PERCENT)
    bigquery_and_generic().one_statement_parses_to(
        "SELECT * FROM dataset.my_table TABLESAMPLE SYSTEM (10 PERCENT)",
        "SELECT * FROM dataset.my_table TABLESAMPLE SYSTEM (10)",
    );
    bigquery_and_generic().one_statement_parses_to(
        "SELECT * FROM t TABLESAMPLE BERNOULLI (50 PERCENT)",
        "SELECT * FROM t TABLESAMPLE BERNOULLI (50)",
    );
}

#[test]
fn parse_bigquery_json_path_function_call() {
    // BigQuery: field access via subscript followed by .METHOD() call
    // e.g., arr[SAFE_OFFSET(0)].CURRENT_TIME() - field named CURRENT_TIME called as function
    bigquery().one_statement_parses_to(
        "SELECT arr[SAFE_OFFSET(0)].CURRENT_TIME() FROM t",
        "SELECT arr[SAFE_OFFSET(0)].CURRENT_TIME() FROM t",
    );

    // Also works for other keywords used as field names with function call syntax
    bigquery().one_statement_parses_to(
        "SELECT s[OFFSET(0)].DATE() FROM t",
        "SELECT s[OFFSET(0)].DATE() FROM t",
    );

    // Regular field access (no parens) still works
    bigquery().verified_stmt("SELECT arr[SAFE_OFFSET(0)].field_name FROM t");
}

#[test]
fn test_bigquery_set_operator_prefix() {
    // BigQuery supports `{LEFT | FULL | INNER} [OUTER] { UNION | INTERSECT | EXCEPT }`
    // as an outer-join-style set operator. The prefix must be captured on the
    // AST so lineage visitors see both branches of the union.
    let cases = [
        (
            "SELECT * FROM t1 LEFT OUTER UNION ALL BY NAME SELECT * FROM t2",
            SetPrefix::Left,
        ),
        (
            "SELECT * FROM t1 LEFT UNION ALL SELECT * FROM t2",
            SetPrefix::Left,
        ),
        (
            "SELECT * FROM t1 INNER UNION ALL SELECT * FROM t2",
            SetPrefix::Inner,
        ),
        (
            "SELECT * FROM t1 FULL OUTER UNION ALL BY NAME SELECT * FROM t2",
            SetPrefix::Full,
        ),
    ];
    for (sql, expected_prefix) in cases {
        let stmts = bigquery().parse_sql_statements(sql).unwrap();
        match &stmts[0] {
            Statement::Query(q) => match &*q.body {
                SetExpr::SetOperation {
                    set_prefix,
                    left,
                    right,
                    ..
                } => {
                    assert_eq!(
                        *set_prefix,
                        Some(expected_prefix),
                        "prefix mismatch for {sql}"
                    );
                    // Both branches must reach visitors: ensure neither is DefaultValues.
                    assert!(matches!(**left, SetExpr::Select(_)));
                    assert!(matches!(**right, SetExpr::Select(_)));
                }
                other => panic!("expected SetOperation, got {other:?}"),
            },
            other => panic!("expected Query, got {other:?}"),
        }
    }
}

#[test]
fn test_bigquery_expr_wildcard_after_subscript() {
    // BigQuery allows `ARRAY_AGG(...)[OFFSET(0)].*` — struct wildcard expansion
    // after a subscripted aggregate. Previously the post-subscript `.` was
    // greedily consumed as a Snowflake-style JSON path, eating the `*` and
    // leaving the parser confused at the next `,`.
    let sql = "SELECT ARRAY_AGG(x ORDER BY y DESC LIMIT 1)[OFFSET(0)].*, col_2 FROM t";
    let stmts = bigquery().parse_sql_statements(sql).unwrap();
    match &stmts[0] {
        Statement::Query(q) => match q.body.as_ref() {
            SetExpr::Select(s) => {
                assert!(matches!(&*s.projection[0], SelectItem::ExprWildcard { .. }));
            }
            other => panic!("expected Select, got {other:?}"),
        },
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn test_bigquery_struct_wildcard_on_parenthesized_expr() {
    // BigQuery struct field-access wildcard applied to a parenthesized
    // expression: `(struct_expr).*`, optionally followed by EXCEPT(...).
    // Previously the `.` was greedily consumed as `CompositeAccess` and the
    // parser failed at the trailing `*` with "Expected identifier".
    // Reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_modifiers
    for sql in [
        "SELECT (s).* FROM t",
        "SELECT (s).* EXCEPT (x) FROM t",
        "SELECT (s).* EXCEPT (x, y), col_2 FROM t",
    ] {
        let stmts = bigquery().parse_sql_statements(sql).unwrap();
        match &stmts[0] {
            Statement::Query(q) => match q.body.as_ref() {
                SetExpr::Select(s) => {
                    assert!(
                        matches!(&*s.projection[0], SelectItem::ExprWildcard { .. }),
                        "expected ExprWildcard for {sql}"
                    );
                }
                other => panic!("expected Select, got {other:?}"),
            },
            other => panic!("expected Query, got {other:?}"),
        }
    }
}

#[test]
fn test_bigquery_struct_wildcard_on_window_function_with_except() {
    // The real-world shape that surfaced the bug: a windowed LAST_VALUE wrapped
    // in parens, then `.*EXCEPT(date)` to drop a column from the struct
    // expansion. The query also exercises CTE chaining with QUALIFY,
    // GENERATE_DATE_ARRAY, and UNNEST.
    let sql = "WITH extractions AS (\
        SELECT *, DATE(extraction_time) AS date \
        FROM `aiven-dw-prod`.`entities_dw`.`employee_entity_stream_base` \
        QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, DATE(extraction_time) ORDER BY extraction_time DESC) = 1\
        ), \
        dates AS (\
        SELECT employee_id, GENERATE_DATE_ARRAY(MIN(date), MAX(date), INTERVAL 1 DAY) AS date_range \
        FROM extractions GROUP BY employee_id\
        ) \
        SELECT \
        (LAST_VALUE(IF(extractions.employee_id IS NOT NULL, extractions, NULL) IGNORE NULLS) \
            OVER (PARTITION BY dates.employee_id ORDER BY date_range_date \
                  RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)\
        ).* EXCEPT (date), \
        date_range_date AS date \
        FROM dates, UNNEST(dates.date_range) AS date_range_date \
        LEFT JOIN extractions ON dates.employee_id = extractions.employee_id AND date_range_date = extractions.date";
    bigquery().parse_sql_statements(sql).unwrap();
}

#[test]
fn test_bigquery_create_procedure_begin_end_body() {
    // BigQuery procedure bodies start with `BEGIN` directly — no `AS` keyword
    // before the body. The parser must populate `body: Vec<Statement>` so
    // downstream consumers (lineage, syntax-check tools) can walk the body.
    // Reference: https://cloud.google.com/bigquery/docs/procedures
    let sql = "CREATE OR REPLACE PROCEDURE proj.ds.p() BEGIN \
               CREATE TEMP TABLE t AS SELECT id FROM proj.ds.src; \
               INSERT INTO proj.ds.dst (id) SELECT id FROM t; \
               END";
    let stmts = bigquery().parse_sql_statements(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateProcedure {
            body,
            body_definition,
            ..
        } => {
            assert!(
                body_definition.is_none(),
                "expected BEGIN/END body, not string"
            );
            assert_eq!(
                body.len(),
                2,
                "expected 2 inner statements (CREATE TEMP TABLE + INSERT), got {body:?}"
            );
            assert!(matches!(body[0], Statement::CreateTable { .. }));
            assert!(matches!(body[1], Statement::Insert { .. }));
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn test_bigquery_create_procedure_empty_body() {
    // BigQuery procedure with no body (OPTIONS-only or trailing `;`) should
    // still parse successfully with `body: vec![]`.
    let sql = "CREATE PROCEDURE proj.ds.p()";
    let stmts = bigquery().parse_sql_statements(sql).unwrap();
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::CreateProcedure {
            body,
            body_definition,
            ..
        } => {
            assert!(body.is_empty());
            assert!(body_definition.is_none());
        }
        other => panic!("expected CreateProcedure, got {other:?}"),
    }
}

#[test]
fn test_bigquery_materialized_view_unparenthesized_cluster_by() {
    // BigQuery `CREATE MATERIALIZED VIEW ... CLUSTER BY col` lists the
    // clustering columns without parentheses, unlike ClickHouse.
    bigquery()
        .parse_sql_statements(
            "CREATE MATERIALIZED VIEW dataset.mv CLUSTER BY s_market_id AS (SELECT s_market_id FROM t)",
        )
        .unwrap();
    bigquery()
        .parse_sql_statements(
            "CREATE MATERIALIZED VIEW dataset.mv CLUSTER BY a, b AS (SELECT a, b FROM t)",
        )
        .unwrap();
}

#[test]
fn test_bigquery_hyphenated_project_id_with_numeric_segment() {
    // BigQuery permits hyphen-separated segments in unquoted project IDs to
    // be numeric, e.g. `root-rarity-166622.scores.ds`. The tokenizer folds
    // the trailing `.` into the number (`166622.`); the table-clause path
    // accepts that form and continues the dotted name parsing.
    bigquery()
        .parse_sql_statements("SELECT * FROM root-rarity-166622.scores.ds")
        .unwrap();
    bigquery()
        .parse_sql_statements("SELECT * FROM proj-228706.dataset.tbl")
        .unwrap();
}

#[test]
fn test_bigquery_offset_in_arithmetic_projection() {
    // BigQuery `WITH OFFSET` exposes `offset` as a column the projection
    // can use. Ensure trailing-comma handling still parses
    //   SELECT a, offset + 1 AS bar FROM t
    // (and similar arithmetic / comparison shapes) where `offset` is the
    // column on the left of an operator.
    bigquery()
        .parse_sql_statements("SELECT a, offset + 1 AS bar FROM t")
        .unwrap();
    bigquery()
        .parse_sql_statements("SELECT a, offset - 1, offset * 2 FROM t")
        .unwrap();
    bigquery()
        .parse_sql_statements("SELECT a, offset >= 0 AS pos FROM t")
        .unwrap();
}

#[test]
fn test_bigquery_raw_string_escaped_quote() {
    // BigQuery raw string literals preserve backslashes literally, but
    // `\'` (or `\"`) is a two-character sequence that does not terminate
    // the string — typically used inside regex character classes.
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#string_and_bytes_literals
    bigquery()
        .parse_sql_statements(r"SELECT REGEXP_REPLACE(x, r'[^\p{L}{0-9} \'\.\-\/\(\)]', '') FROM t")
        .unwrap();
    bigquery()
        .parse_sql_statements(r#"SELECT r"escaped \" quote stays in string""#)
        .unwrap();
}
