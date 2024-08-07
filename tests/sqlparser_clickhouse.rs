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
//! Test SQL syntax specific to ClickHouse.

#[cfg(test)]
use pretty_assertions::assert_eq;
use sqlparser::ast::Expr::{ArrayIndex, BinaryOp, Identifier};
use sqlparser::ast::Ident;
use sqlparser::ast::SelectItem::UnnamedExpr;
use sqlparser::ast::TableFactor::Table;
use sqlparser::ast::*;
use sqlparser::dialect::ClickHouseDialect;
use test_utils::*;

#[macro_use]
mod test_utils;

#[test]
fn parse_array_access_expr() {
    let sql = r#"SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos WHERE id = 'test' AND string_value[indexOf(string_name, 'app')] <> 'foo'"#;
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        Select {
            distinct: None,
            top: None,
            projection: vec![UnnamedExpr(
                ArrayIndex {
                    obj: Box::new(Identifier(
                        Ident {
                            value: "string_values".to_string(),
                            quote_style: None,
                        }
                        .empty_span()
                    )),
                    indexes: vec![Expr::Function(Function {
                        name: ObjectName(vec!["indexOf".into()]),
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                Ident::new("string_names").empty_span()
                            ))),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString("endpoint".to_string())
                            ))),
                        ],
                        over: None,
                        distinct: false,
                        special: false,
                        order_by: vec![],
                        limit: None,
                        on_overflow: None,
                        null_treatment: None,
                        within_group: None,
                    })],
                }
                .empty_span()
            )
            .empty_span()],
            into: None,
            from: vec![TableWithJoins {
                relation: Table {
                    name: ObjectName(vec![Ident::new("foos")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            selection: Some(
                BinaryOp {
                    left: Box::new(BinaryOp {
                        left: Box::new(Identifier(Ident::new("id").empty_span())),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("test".to_string()))),
                    }),
                    op: BinaryOperator::And,
                    right: Box::new(BinaryOp {
                        left: Box::new(ArrayIndex {
                            obj: Box::new(Identifier(Ident::new("string_value").empty_span())),
                            indexes: vec![Expr::Function(Function {
                                name: ObjectName(vec![Ident::new("indexOf")]),
                                args: vec![
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                                        Ident::new("string_name").empty_span()
                                    ))),
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                        Value::SingleQuotedString("app".to_string())
                                    ))),
                                ],
                                over: None,
                                distinct: false,
                                special: false,
                                order_by: vec![],
                                limit: None,
                                on_overflow: None,
                                null_treatment: None,
                                within_group: None,
                            })],
                        }),
                        op: BinaryOperator::NotEq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("foo".to_string()))),
                    }),
                }
                .empty_span()
            ),
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        },
        select
    );
}

#[test]
fn parse_array_expr() {
    let sql = "SELECT ['1', '2'] FROM test";
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &Expr::Array(Array {
            elem: vec![
                Expr::Value(Value::SingleQuotedString("1".to_string())),
                Expr::Value(Value::SingleQuotedString("2".to_string())),
            ],
            named: false,
        }),
        expr_from_projection(only(&select.projection))
    )
}

#[test]
fn parse_array_fn() {
    let sql = "SELECT array(x1, x2) FROM foo";
    let select = clickhouse().verified_only_select(sql);
    assert_eq!(
        &Expr::Function(Function {
            name: ObjectName(vec![Ident::new("array")]),
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                    Ident::new("x1").empty_span()
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(
                    Ident::new("x2").empty_span()
                ))),
            ],
            over: None,
            distinct: false,
            special: false,
            order_by: vec![],
            limit: None,
            on_overflow: None,
            null_treatment: None,
            within_group: None,
        }),
        expr_from_projection(only(&select.projection))
    );
}

#[test]
fn parse_kill() {
    let stmt = clickhouse().verified_stmt("KILL MUTATION 5");
    assert_eq!(
        stmt,
        Statement::Kill {
            modifier: Some(KillType::Mutation),
            id: 5,
        }
    );
}

#[test]
fn parse_create_table_order_by() {
    clickhouse().one_statement_parses_to(
        "CREATE TABLE analytics.int_user_stats (`user_id` String, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192",
        "CREATE TABLE analytics.int_user_stats (`user_id` String, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY () SETTINGS index_granularity = 8192"
    );
}

#[test]
fn parse_create_table_primary_key() {
    clickhouse().verified_stmt(
        "CREATE TABLE analytics.int_user_stats (`user_id` String, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') PRIMARY KEY (user_id) SETTINGS index_granularity = 8192",
    );
}

#[test]
fn parse_create_table_ttl() {
    clickhouse().verified_stmt(
        "CREATE TABLE analytics.int_user_stats (`user_id` String, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY (user_id) TTL toDateTime(state_at) + toIntervalHour(12) SETTINGS index_granularity = 8192",
    );
}

#[test]
fn parse_create_table_column_codec() {
    clickhouse().verified_stmt("CREATE TABLE anomalies.metrics_volume (`ingested_at` DateTime64(8,'UTC') CODEC(DoubleDelta, ZSTD(1))) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ingested_at) ORDER BY (workspace, integration_id, path, segment, scheduled_at) SETTINGS index_granularity = 8192");
}

#[test]
fn parse_create_table_index() {
    clickhouse().verified_stmt("CREATE TABLE default.runs (`workspace` LowCardinality(String), `id` String, `comment` String, INDEX bloom_filter_id_index_4 id TYPE bloom_filter GRANULARITY 4, INDEX comment_lowercase(lower(comment)) TYPE inverted) ENGINE=ReplacingMergeTree(ingested_at) ORDER BY (workspace, created_at, id) SETTINGS index_granularity = 2048");
}

#[test]
fn parse_delimited_identifiers() {
    // check that quoted identifiers in any position remain quoted after serialization
    let select = clickhouse().verified_only_select(
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
    match select.projection[2].clone().unwrap() {
        SelectItem::ExprWithAlias { expr, alias } => {
            assert_eq!(
                Expr::Identifier(Ident::with_quote('"', "simple id").empty_span()).empty_span(),
                expr
            );
            assert_eq!(Ident::with_quote('"', "column alias").empty_span(), alias);
        }
        _ => panic!("Expected ExprWithAlias"),
    }

    clickhouse().verified_stmt(r#"CREATE TABLE "foo" ("bar" "int")"#);
    clickhouse().verified_stmt(r#"ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)"#);
    //TODO verified_stmt(r#"UPDATE foo SET "bar" = 5"#);
}

#[test]
fn parse_like() {
    fn chk(negated: bool) {
        let sql = &format!(
            "SELECT * FROM customers WHERE name {}LIKE '%a'",
            if negated { "NOT " } else { "" }
        );
        let select = clickhouse().verified_only_select(sql);
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
            "SELECT * FROM customers WHERE name {}LIKE '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = clickhouse().verified_only_select(sql);
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
        let select = clickhouse().verified_only_select(sql);
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
        let select = clickhouse().verified_only_select(sql);
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
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\'",
            if negated { "NOT " } else { "" }
        );
        let select = clickhouse().verified_only_select(sql);
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
            "SELECT * FROM customers WHERE name {}SIMILAR TO '%a' ESCAPE '\\' IS NULL",
            if negated { "NOT " } else { "" }
        );
        let select = clickhouse().verified_only_select(sql);
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
fn parse_create_table() {
    clickhouse().verified_stmt(r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")"#);
    clickhouse().one_statement_parses_to(
        r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY "x""#,
        r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x") AS SELECT * FROM "t" WHERE true"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE TABLE "x" ("a" Nullable(DateTime64(8))) ENGINE=MergeTree ORDER BY ("x") AS SELECT * FROM "t" WHERE true"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE TABLE default.runs_buffer (`workspace` LowCardinality(String), `id` String, `assets` Array(String), `asset_types` Array(Int32), `target` Array(String), `target_type` Array(Int32), `extra_references` Array(String) DEFAULT [], `extra_reference_types` Array(Int32) DEFAULT [], `run_type` Int32, `run_status` Int32, `message` String, `created_at` DateTime64(8,'UTC'), `started_at` DateTime64(8,'UTC'), `finished_at` DateTime64(8,'UTC'), `meta` String, `exclude_status_update` Bool, `ingested_at` DateTime64(8,'UTC'), `parent_ids` Array(String), `skipped` Bool DEFAULT false) ENGINE=Buffer('default', 'runs', 4, 2, 5, 10000, 1000000, 2500000, 10000000)"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE TABLE schema.schema_migrations (`version` Int64, `dirty` UInt8, `sequence` UInt64) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY (sequence) SETTINGS index_granularity = 8192"#,
    );
}

#[test]
fn parse_create_view() {
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo (`baz` String) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo TO out (`baz` String) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt(r#"CREATE VIEW foo (`baz` String) AS SELECT bar AS baz FROM in"#);
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo (`baz` String) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo TO out (`baz` String) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt(r#"CREATE VIEW analytics.runs_audit_ingest_daily (`count` UInt64, `ts` DateTime('UTC')) AS SELECT count(*) AS count, toStartOfDay(ingested_at) AS ts FROM analytics.runs_int_runs GROUP BY ts ORDER BY ts DESC"#);
}

#[test]
fn parse_create_materialized_view() {
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo (`baz` String) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', created_at) ORDER BY (workspace, asset) SETTINGS index_granularity = 8192 AS SELECT bar AS baz FROM in"#,
    );
}

#[test]
fn parse_limit_by() {
    clickhouse().verified_stmt(
        r#"SELECT * FROM default.last_asset_runs_mv ORDER BY created_at DESC LIMIT 1 BY asset"#,
    );
}

#[test]
fn parse_array_accessor() {
    clickhouse().one_statement_parses_to(
        r#"SELECT baz.1 AS b1 FROM foo AS f"#,
        r#"SELECT baz[1] AS b1 FROM foo AS f"#,
    );
}

#[test]
fn parse_array_join() {
    clickhouse().verified_stmt(r#"SELECT asset AS a FROM runs AS r ARRAY JOIN r.assets AS asset"#);
    clickhouse().verified_stmt(r#"SELECT asset AS a FROM runs ARRAY JOIN r.assets AS asset"#);
}

#[test]
fn parse_double_equal() {
    clickhouse().one_statement_parses_to(
        r#"SELECT foo FROM bar WHERE buz == 'buz'"#,
        r#"SELECT foo FROM bar WHERE buz = 'buz'"#,
    );
}

#[test]
fn parse_select_ignore_nulls() {
    clickhouse().verified_stmt("SELECT last_value(b) IGNORE NULLS FROM test_data");
}

#[test]
fn parse_select_star_except() {
    clickhouse().verified_stmt("SELECT * EXCEPT (prev_status) FROM anomalies");
}

#[test]
fn parse_select_star_except_no_parens() {
    clickhouse().one_statement_parses_to(
        "SELECT * EXCEPT prev_status FROM anomalies",
        "SELECT * EXCEPT (prev_status) FROM anomalies",
    );
}

#[test]
fn parse_select_star_replace() {
    clickhouse().verified_stmt("SELECT * REPLACE (i + 1 AS i) FROM columns_transformers");
}

#[test]
fn parse_in_array() {
    clickhouse()
        .verified_stmt("SELECT * FROM latest_schemas WHERE workspace IN array('synq-ops', 'test')");
    clickhouse().verified_stmt("SELECT * FROM latest_schemas WHERE workspace IN array()");
}

#[test]
fn parse_in_nested() {
    clickhouse().verified_stmt("SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM foo");
}

#[test]
fn parse_in_array_square_syntax() {
    clickhouse().one_statement_parses_to(
        "SELECT * FROM latest_schemas WHERE workspace IN ['synq-ops', 'test']",
        "SELECT * FROM latest_schemas WHERE workspace IN array('synq-ops', 'test')",
    );
}

#[test]
fn parse_in_array_square_syntax_empty() {
    clickhouse().one_statement_parses_to(
        "SELECT * FROM latest_schemas WHERE workspace IN []",
        "SELECT * FROM latest_schemas WHERE workspace IN array()",
    );
}

#[test]
fn parse_add_column_after() {
    clickhouse().verified_stmt(
        "ALTER TABLE raw_relationships ADD COLUMN IF NOT EXISTS owned_by_proto TEXT AFTER used_by_proto",
    );
}

#[test]
fn parse_create_view_if_not_exists() {
    clickhouse().verified_stmt(
        "CREATE VIEW IF NOT EXISTS latest_schemas_mv AS SELECT * FROM latest_schemas",
    );

    clickhouse().verified_stmt(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS latest_schemas_mv TO latest_schemas AS SELECT * FROM latest_schemas",
    );
}

fn clickhouse() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {})],
        options: None,
    }
}
