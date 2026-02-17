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
use sqlparser::dialect::{ClickHouseDialect, GenericDialect};
use sqlparser::parser::ParserError;
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
                    with_ordinality: false,
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            sample: None,
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
                            })],
                        }),
                        op: BinaryOperator::NotEq,
                        right: Box::new(Expr::Value(Value::SingleQuotedString("foo".to_string()))),
                    }),
                }
                .empty_span()
            ),
            group_by: GroupByExpr::Expressions(vec![], vec![]),
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
        "CREATE TABLE analytics.int_user_stats (`user_id` STRING, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY tuple() SETTINGS index_granularity = 8192",
        "CREATE TABLE analytics.int_user_stats (`user_id` STRING, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY () SETTINGS index_granularity = 8192"
    );
}

#[test]
fn parse_create_table_primary_key() {
    clickhouse().verified_stmt(
        "CREATE TABLE analytics.int_user_stats (`user_id` STRING, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') PRIMARY KEY (user_id) SETTINGS index_granularity = 8192",
    );
}

#[test]
fn parse_create_table_ttl() {
    clickhouse().verified_stmt(
        "CREATE TABLE analytics.int_user_stats (`user_id` STRING, `num_events` UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY (user_id) TTL toDateTime(state_at) + toIntervalHour(12) SETTINGS index_granularity = 8192",
    );
}

#[test]
fn parse_create_table_column_codec() {
    clickhouse().verified_stmt("CREATE TABLE anomalies.metrics_volume (`ingested_at` DateTime64(8, 'UTC') CODEC(DoubleDelta, ZSTD(1))) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', ingested_at) ORDER BY (workspace, integration_id, path, segment, scheduled_at) SETTINGS index_granularity = 8192"    );
}

#[test]
fn parse_create_table_index() {
    clickhouse().verified_stmt("CREATE TABLE default.runs (`workspace` LowCardinality(STRING), `id` STRING, `comment` STRING, INDEX bloom_filter_id_index_4 id TYPE bloom_filter GRANULARITY 4, INDEX comment_lowercase (lower(comment)) TYPE inverted) ENGINE=ReplacingMergeTree(ingested_at) ORDER BY (workspace, created_at, id) SETTINGS index_granularity = 2048");
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
        r#"CREATE TABLE default.runs_buffer (`workspace` LowCardinality(STRING), `id` STRING, `assets` Array(STRING), `asset_types` Array(Int32), `target` Array(STRING), `target_type` Array(Int32), `extra_references` Array(STRING) DEFAULT [], `extra_reference_types` Array(Int32) DEFAULT [], `run_type` Int32, `run_status` Int32, `message` STRING, `created_at` DateTime64(8, 'UTC'), `started_at` DateTime64(8, 'UTC'), `finished_at` DateTime64(8, 'UTC'), `meta` STRING, `exclude_status_update` BOOL, `ingested_at` DateTime64(8, 'UTC'), `parent_ids` Array(STRING), `skipped` BOOL DEFAULT false) ENGINE=Buffer('default', 'runs', 4, 2, 5, 10000, 1000000, 2500000, 10000000)"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE TABLE schema.schema_migrations (`version` INT64, `dirty` UInt8, `sequence` UInt64) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') ORDER BY (sequence) SETTINGS index_granularity = 8192"#,
    );
}

#[test]
fn parse_optimize_table() {
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE db.t0");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 ON CLUSTER 'cluster'");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 ON CLUSTER 'cluster' FINAL");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 FINAL DEDUPLICATE");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 DEDUPLICATE");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 DEDUPLICATE BY id");
    clickhouse_and_generic().verified_stmt("OPTIMIZE TABLE t0 FINAL DEDUPLICATE BY id");
    clickhouse_and_generic()
        .verified_stmt("OPTIMIZE TABLE t0 PARTITION tuple('2023-04-22') DEDUPLICATE BY id");
    match clickhouse_and_generic().verified_stmt(
        "OPTIMIZE TABLE t0 ON CLUSTER cluster PARTITION ID '2024-07' FINAL DEDUPLICATE BY id",
    ) {
        Statement::OptimizeTable {
            name,
            on_cluster,
            partition,
            include_final,
            deduplicate,
            ..
        } => {
            assert_eq!(name.to_string(), "t0");
            assert_eq!(on_cluster, Some(Ident::new("cluster").empty_span()));
            assert_eq!(
                partition,
                Some(Partition::Identifier(
                    Ident::with_quote('\'', "2024-07").empty_span()
                ))
            );
            assert!(include_final);
            assert_eq!(
                deduplicate,
                Some(Deduplicate::ByExpression(Identifier(
                    Ident::new("id").empty_span()
                )))
            );
        }
        _ => unreachable!(),
    }

    // negative cases
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("OPTIMIZE TABLE t0 DEDUPLICATE BY")
            .unwrap_err(),
        ParserError::ParserError(
            "Expected an expression:, found: EOF\nNear `OPTIMIZE TABLE t0 DEDUPLICATE BY`"
                .to_string()
        )
    );
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("OPTIMIZE TABLE t0 PARTITION")
            .unwrap_err(),
        ParserError::ParserError(
            "Expected an expression:, found: EOF\nNear `OPTIMIZE TABLE t0 PARTITION`".to_string()
        )
    );
    assert_eq!(
        clickhouse_and_generic()
            .parse_sql_statements("OPTIMIZE TABLE t0 PARTITION ID")
            .unwrap_err(),
        ParserError::ParserError(
            "Expected identifier, found: EOF\nNear `OPTIMIZE TABLE t0 PARTITION ID`".to_string()
        )
    );
}

#[test]
fn parse_create_view() {
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo (`baz` STRING) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo TO out (`baz` STRING) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt(r#"CREATE VIEW foo (`baz` STRING) AS SELECT bar AS baz FROM in"#);
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo (`baz` STRING) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo TO out (`baz` STRING) AS SELECT bar AS baz FROM in"#,
    );
    clickhouse().verified_stmt("CREATE VIEW analytics.runs_audit_ingest_daily (`count` UInt64, `ts` DATETIME('UTC')) AS SELECT count(*) AS count, toStartOfDay(ingested_at) AS ts FROM analytics.runs_int_runs GROUP BY ts ORDER BY ts DESC");
}

#[test]
fn parse_alter_table_attach_and_detach_partition() {
    for operation in &["ATTACH", "DETACH"] {
        match clickhouse_and_generic()
            .verified_stmt(format!("ALTER TABLE t0 {operation} PARTITION part").as_str())
        {
            Statement::AlterTable {
                name, operations, ..
            } => {
                assert_eq!("t0", name.to_string());
                assert_eq!(
                    operations[0],
                    if operation == &"ATTACH" {
                        AlterTableOperation::AttachPartition {
                            partition: Partition::Expr(Identifier(Ident::new("part").empty_span())),
                        }
                    } else {
                        AlterTableOperation::DetachPartition {
                            partition: Partition::Expr(Identifier(Ident::new("part").empty_span())),
                        }
                    }
                );
            }
            _ => unreachable!(),
        }

        match clickhouse_and_generic()
            .verified_stmt(format!("ALTER TABLE t1 {operation} PART part").as_str())
        {
            Statement::AlterTable {
                name, operations, ..
            } => {
                assert_eq!("t1", name.to_string());
                assert_eq!(
                    operations[0],
                    if operation == &"ATTACH" {
                        AlterTableOperation::AttachPartition {
                            partition: Partition::Part(Identifier(Ident::new("part").empty_span())),
                        }
                    } else {
                        AlterTableOperation::DetachPartition {
                            partition: Partition::Part(Identifier(Ident::new("part").empty_span())),
                        }
                    }
                );
            }
            _ => unreachable!(),
        }

        // negative cases
        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(format!("ALTER TABLE t0 {operation} PARTITION").as_str())
                .unwrap_err(),
            ParserError::ParserError(format!("Expected an expression:, found: EOF\nNear `ALTER TABLE t0 {operation} PARTITION`").to_string())
        );
        assert_eq!(
            clickhouse_and_generic()
                .parse_sql_statements(format!("ALTER TABLE t0 {operation} PART").as_str())
                .unwrap_err(),
            ParserError::ParserError(
                format!(
                    "Expected an expression:, found: EOF\nNear `ALTER TABLE t0 {operation} PART`"
                )
                .to_string()
            )
        );
    }
}

#[test]
fn parse_alter_table_drop_partition_and_part() {
    // DROP PART 'part_name'
    match clickhouse_and_generic().verified_stmt("ALTER TABLE mt DROP PART 'all_4_4_0'") {
        Statement::AlterTable {
            name, operations, ..
        } => {
            assert_eq!("mt", name.to_string());
            assert_eq!(
                operations[0],
                AlterTableOperation::DropPartition {
                    partition: Partition::Part(Expr::Value(Value::SingleQuotedString(
                        "all_4_4_0".to_string()
                    ))),
                }
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_materialized_view() {
    clickhouse().verified_stmt(
        r#"CREATE MATERIALIZED VIEW foo (`baz` STRING) ENGINE=ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', created_at) ORDER BY (workspace, asset) SETTINGS index_granularity = 8192 AS SELECT bar AS baz FROM in"#,
    );
}

#[test]
fn parse_limit_by() {
    clickhouse().verified_stmt(
        r#"SELECT * FROM default.last_asset_runs_mv ORDER BY created_at DESC LIMIT 1 BY asset"#,
    );
}

#[test]
fn parse_create_table_with_variant_default_expressions() {
    let sql = concat!(
        "CREATE TABLE table (",
        "a DATETIME MATERIALIZED now(),",
        " b DATETIME EPHEMERAL now(),",
        " c DATETIME EPHEMERAL,",
        " d STRING ALIAS toString(c),",
        " x ENUM8('hello' = 1, 'world', 'foo' = -3)",
        ") ENGINE=MergeTree"
    );
    match clickhouse().verified_stmt(sql) {
        Statement::CreateTable { columns, .. } => {
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("a").empty_span(),
                        data_type: DataType::Datetime(None, None),
                        collation: None,
                        codec: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Materialized(Expr::Function(Function {
                                name: ObjectName(vec![Ident::new("now")]),
                                args: vec![],
                                parameters: None,
                                null_treatment: None,
                                over: None,
                                distinct: false,
                                approximate: false,
                                special: false,
                                order_by: vec![],
                                limit: None,
                                within_group: None,
                                on_overflow: None,
                            }))
                        }],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                    ColumnDef {
                        name: Ident::new("b").empty_span(),
                        data_type: DataType::Datetime(None, None),
                        collation: None,
                        codec: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Ephemeral(Some(Expr::Function(Function {
                                name: ObjectName(vec![Ident::new("now")]),
                                args: vec![],
                                parameters: None,
                                null_treatment: None,
                                over: None,
                                distinct: false,
                                approximate: false,
                                special: false,
                                order_by: vec![],
                                limit: None,
                                within_group: None,
                                on_overflow: None,
                            })))
                        }],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                    ColumnDef {
                        name: Ident::new("c").empty_span(),
                        data_type: DataType::Datetime(None, None),
                        collation: None,
                        codec: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Ephemeral(None)
                        }],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                    ColumnDef {
                        name: Ident::new("d").empty_span(),
                        data_type: DataType::String(None),
                        collation: None,
                        codec: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Alias(Expr::Function(Function {
                                name: ObjectName(vec![Ident::new("toString")]),
                                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(
                                    Expr::Identifier(Ident::new("c").empty_span())
                                ))],
                                parameters: None,
                                null_treatment: None,
                                over: None,
                                distinct: false,
                                approximate: false,
                                special: false,
                                order_by: vec![],
                                limit: None,
                                within_group: None,
                                on_overflow: None,
                            }))
                        }],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                    ColumnDef {
                        name: Ident::new("x").empty_span(),
                        data_type: DataType::Enum8(vec![
                            EnumTypeValue::NameWithValue("hello".to_string(), 1),
                            EnumTypeValue::Name("world".to_string()),
                            EnumTypeValue::NameWithValue("foo".to_string(), -3)
                        ]),
                        collation: None,
                        codec: None,
                        options: vec![],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    }
                ]
            )
        }
        _ => unreachable!(),
    }
}

fn column_def(name: Ident, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: name.empty_span(),
        data_type,
        collation: None,
        codec: None,
        options: vec![],
        column_options: vec![],
        mask: None,
        column_location: None,
        column_policy: None,
    }
}

#[test]
fn parse_clickhouse_data_types() {
    let sql = concat!(
        "CREATE TABLE table (",
        "a1 UInt8, a2 UInt16, a3 UInt32, a4 UInt64, a5 UInt128, a6 UInt256,",
        " b1 Int8, b2 Int16, b3 Int32, b4 Int64, b5 Int128, b6 Int256,",
        " c1 Float32, c2 Float64,",
        " d1 Date32, d2 DateTime64(3), d3 DateTime64(3, 'UTC'),",
        " e1 FixedString(255),",
        " f1 LowCardinality(Int32)",
        ") ORDER BY (a1)",
    );
    // ClickHouse has a case-sensitive definition of data type, but canonical representation is not
    let canonical_sql = sql
        .replace(" Int8", " INT8")
        .replace(" Int64", " INT64")
        .replace(" Float64", " FLOAT64");

    match clickhouse_and_generic().one_statement_parses_to(sql, &canonical_sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, ObjectName(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    column_def("a1".into(), DataType::UInt8),
                    column_def("a2".into(), DataType::UInt16),
                    column_def("a3".into(), DataType::UInt32),
                    column_def("a4".into(), DataType::UInt64),
                    column_def("a5".into(), DataType::UInt128),
                    column_def("a6".into(), DataType::UInt256),
                    column_def("b1".into(), DataType::Int8(None)),
                    column_def("b2".into(), DataType::Int16),
                    column_def("b3".into(), DataType::Int32),
                    column_def("b4".into(), DataType::Int64),
                    column_def("b5".into(), DataType::Int128),
                    column_def("b6".into(), DataType::Int256),
                    column_def("c1".into(), DataType::Float32),
                    column_def("c2".into(), DataType::Float64),
                    column_def("d1".into(), DataType::Date32),
                    column_def("d2".into(), DataType::Datetime64(3, None)),
                    column_def("d3".into(), DataType::Datetime64(3, Some("UTC".into()))),
                    column_def("e1".into(), DataType::FixedString(255)),
                    column_def(
                        "f1".into(),
                        DataType::LowCardinality(Box::new(DataType::Int32))
                    ),
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_nullable() {
    let sql = r#"CREATE TABLE table (k UInt8, a Nullable(String), b Nullable(DateTime64(9, 'UTC')), c Nullable(DateTime64(9)), d Date32 NULL) ENGINE=MergeTree ORDER BY (k)"#;
    // ClickHouse has a case-sensitive definition of data type, but canonical representation is not
    let canonical_sql = sql.replace("String", "STRING");

    match clickhouse_and_generic().one_statement_parses_to(sql, &canonical_sql) {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, ObjectName(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    column_def("k".into(), DataType::UInt8),
                    column_def(
                        Ident::new("a"),
                        DataType::Nullable(Box::new(DataType::String(None)))
                    ),
                    column_def(
                        Ident::new("b"),
                        DataType::Nullable(Box::new(DataType::Datetime64(
                            9,
                            Some("UTC".to_string())
                        )))
                    ),
                    column_def(
                        "c".into(),
                        DataType::Nullable(Box::new(DataType::Datetime64(9, None)))
                    ),
                    ColumnDef {
                        name: Ident::new("d").empty_span(),
                        data_type: DataType::Date32,
                        collation: None,
                        codec: None,
                        options: vec![ColumnOptionDef {
                            name: None,
                            option: ColumnOption::Null
                        }],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    }
                ]
            );
        }
        _ => unreachable!(),
    }
}

#[test]
fn parse_create_table_with_nested_data_types() {
    let sql = concat!(
        "CREATE TABLE table (",
        " i Nested(a Array(Int16), b LowCardinality(String)),",
        " k Array(Tuple(FixedString(128), Int128)),",
        " l Tuple(a DateTime64(9), b Array(UUID)),",
        " m Map(String, UInt16)",
        ") ENGINE=MergeTree ORDER BY (k)"
    );

    match clickhouse().one_statement_parses_to(sql, "") {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, ObjectName(vec!["table".into()]));
            assert_eq!(
                columns,
                vec![
                    ColumnDef {
                        name: Ident::new("i").empty_span(),
                        data_type: DataType::Nested(vec![
                            column_def(
                                "a".into(),
                                DataType::Array(ArrayElemTypeDef::Parenthesis(Box::new(
                                    DataType::Int16
                                ),))
                            ),
                            column_def(
                                "b".into(),
                                DataType::LowCardinality(Box::new(DataType::String(None)))
                            )
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
                        name: Ident::new("k").empty_span(),
                        data_type: DataType::Array(ArrayElemTypeDef::Parenthesis(Box::new(
                            DataType::Tuple(vec![
                                StructField {
                                    field_name: None,
                                    field_type: DataType::FixedString(128),
                                    options: vec![],
                                    colon: false,
                                },
                                StructField {
                                    field_name: None,
                                    field_type: DataType::Int128,
                                    options: vec![],
                                    colon: false,
                                }
                            ])
                        ))),
                        collation: None,
                        codec: None,
                        options: vec![],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                    ColumnDef {
                        name: Ident::new("l").empty_span(),
                        data_type: DataType::Tuple(vec![
                            StructField {
                                field_name: Some(Ident::new("a").empty_span()),
                                field_type: DataType::Datetime64(9, None),
                                options: vec![],
                                colon: false,
                            },
                            StructField {
                                field_name: Some(Ident::new("b").empty_span()),
                                field_type: DataType::Array(ArrayElemTypeDef::Parenthesis(
                                    Box::new(DataType::Uuid)
                                )),
                                options: vec![],
                                colon: false,
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
                        name: Ident::new("m").empty_span(),
                        data_type: DataType::Map(
                            Box::new(DataType::String(None)),
                            Box::new(DataType::UInt16)
                        ),
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
fn parse_create_view_with_fields_data_types() {
    match clickhouse().verified_stmt(r#"CREATE VIEW v (i "int", f "String") AS SELECT * FROM t"#) {
        Statement::CreateView {
            name,
            columns_with_types,
            ..
        } => {
            assert_eq!(name, ObjectName(vec!["v".into()]));
            assert_eq!(
                columns_with_types,
                vec![
                    ColumnDef {
                        name: Ident::new("i").empty_span(),
                        data_type: DataType::Custom(
                            ObjectName(vec![Ident {
                                value: "int".into(),
                                quote_style: Some('"')
                            }]),
                            vec![]
                        ),
                        collation: None,
                        codec: None,
                        options: vec![],
                        column_options: vec![],
                        mask: None,
                        column_location: None,
                        column_policy: None,
                    },
                    ColumnDef {
                        name: Ident::new("f").empty_span(),
                        data_type: DataType::Custom(
                            ObjectName(vec![Ident {
                                value: "String".into(),
                                quote_style: Some('"')
                            }]),
                            vec![]
                        ),
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
fn parse_array_accessor() {
    clickhouse().verified_stmt("SELECT baz.1 AS b1 FROM foo AS f");
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
fn parse_select_order_by_with_fill_interpolate() {
    let sql = "SELECT id, fname, lname FROM customer WHERE id < 5 \
        ORDER BY \
            fname ASC NULLS FIRST WITH FILL FROM 10 TO 20 STEP 2, \
            lname DESC NULLS LAST WITH FILL FROM 30 TO 40 STEP 3 \
            INTERPOLATE (col1 AS col1 + 1) \
        LIMIT 2";
    let select = crate::clickhouse().verified_query(sql);
    assert_eq!(
        OrderBy {
            exprs: vec![
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("fname").empty_span()),
                    asc: Some(true),
                    nulls_first: Some(true),
                    with_fill: Some(WithFill {
                        from: Some(Expr::Value(number("10"))),
                        to: Some(Expr::Value(number("20"))),
                        step: Some(Expr::Value(number("2"))),
                    }),
                },
                OrderByExpr {
                    expr: Expr::Identifier(Ident::new("lname").empty_span()),
                    asc: Some(false),
                    nulls_first: Some(false),
                    with_fill: Some(WithFill {
                        from: Some(Expr::Value(number("30"))),
                        to: Some(Expr::Value(number("40"))),
                        step: Some(Expr::Value(number("3"))),
                    }),
                },
            ],
            interpolate: Some(Interpolate {
                exprs: Some(vec![InterpolateExpr {
                    column: Ident::new("col1").empty_span(),
                    expr: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("col1").empty_span())),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("1"))),
                    }),
                }])
            })
        },
        select.order_by.expect("ORDER BY expected")
    );
    assert_eq!(Some(Expr::Value(number("2"))), select.limit);
}

#[test]
fn parse_select_order_by_with_fill_interpolate_multi_interpolates() {
    let sql = "SELECT id, fname, lname FROM customer ORDER BY fname WITH FILL \
        INTERPOLATE (col1 AS col1 + 1) INTERPOLATE (col2 AS col2 + 2)";
    crate::clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("ORDER BY only accepts a single INTERPOLATE clause");
}

#[test]
fn parse_select_order_by_with_fill_interpolate_multi_with_fill_interpolates() {
    let sql = "SELECT id, fname, lname FROM customer \
        ORDER BY \
            fname WITH FILL INTERPOLATE (col1 AS col1 + 1), \
            lname WITH FILL INTERPOLATE (col2 AS col2 + 2)";
    crate::clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("ORDER BY only accepts a single INTERPOLATE clause");
}

#[test]
fn parse_select_order_by_interpolate_not_last() {
    let sql = "SELECT id, fname, lname FROM customer \
        ORDER BY \
            fname INTERPOLATE (col2 AS col2 + 2),
            lname";
    crate::clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("ORDER BY INTERPOLATE must be in the last position");
}

#[test]
fn parse_with_fill() {
    let sql = "SELECT fname FROM customer ORDER BY fname \
        WITH FILL FROM 10 TO 20 STEP 2";
    let select = crate::clickhouse().verified_query(sql);
    assert_eq!(
        Some(WithFill {
            from: Some(Expr::Value(number("10"))),
            to: Some(Expr::Value(number("20"))),
            step: Some(Expr::Value(number("2"))),
        }),
        select.order_by.expect("ORDER BY expected").exprs[0].with_fill
    );
}

#[test]
fn parse_with_fill_missing_single_argument() {
    let sql = "SELECT id, fname, lname FROM customer ORDER BY \
            fname WITH FILL FROM TO 20";
    crate::clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("WITH FILL requires expressions for all arguments");
}

#[test]
fn parse_with_fill_multiple_incomplete_arguments() {
    let sql = "SELECT id, fname, lname FROM customer ORDER BY \
            fname WITH FILL FROM TO 20, lname WITH FILL FROM TO STEP 1";
    crate::clickhouse_and_generic()
        .parse_sql_statements(sql)
        .expect_err("WITH FILL requires expressions for all arguments");
}

#[test]
fn parse_interpolate_body_with_columns() {
    let sql = "SELECT fname FROM customer ORDER BY fname WITH FILL \
        INTERPOLATE (col1 AS col1 + 1, col2 AS col3, col4 AS col4 + 4)";
    let select = crate::clickhouse().verified_query(sql);
    assert_eq!(
        Some(Interpolate {
            exprs: Some(vec![
                InterpolateExpr {
                    column: Ident::new("col1").empty_span(),
                    expr: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("col1").empty_span())),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("1"))),
                    }),
                },
                InterpolateExpr {
                    column: Ident::new("col2").empty_span(),
                    expr: Some(Expr::Identifier(Ident::new("col3").empty_span())),
                },
                InterpolateExpr {
                    column: Ident::new("col4").empty_span(),
                    expr: Some(Expr::BinaryOp {
                        left: Box::new(Expr::Identifier(Ident::new("col4").empty_span())),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(number("4"))),
                    }),
                },
            ])
        }),
        select.order_by.expect("ORDER BY expected").interpolate
    );
}

#[test]
fn parse_interpolate_without_body() {
    let sql = "SELECT fname FROM customer ORDER BY fname WITH FILL INTERPOLATE";
    let select = crate::clickhouse().verified_query(sql);
    assert_eq!(
        Some(Interpolate { exprs: None }),
        select.order_by.expect("ORDER BY expected").interpolate
    );
}

#[test]
fn parse_interpolate_with_empty_body() {
    let sql = "SELECT fname FROM customer ORDER BY fname WITH FILL INTERPOLATE ()";
    let select = crate::clickhouse().verified_query(sql);
    assert_eq!(
        Some(Interpolate {
            exprs: Some(vec![])
        }),
        select.order_by.expect("ORDER BY expected").interpolate
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

#[test]
fn parse_create_table_comment() {
    clickhouse().verified_stmt(
        "CREATE TABLE analytics.f_entity_controls_by_type (`path` STRING) COMMENT 'Information about all entity controls by type.\nThe table contains one row for every entity control by type.\n'",
    );
    clickhouse().one_statement_parses_to(
        "CREATE TABLE analytics.f_entity_controls_by_type
(
    `path` STRING
)
SETTINGS index_granularity = 8192
COMMENT 'Information about all entity controls by type.\nThe table contains one row for every entity control by type.\n'",
        "CREATE TABLE analytics.f_entity_controls_by_type (`path` STRING) COMMENT 'Information about all entity controls by type.\nThe table contains one row for every entity control by type.\n' SETTINGS index_granularity = 8192"
    );
}

#[test]
fn parse_create_view_comment() {
    clickhouse().one_statement_parses_to(
        "CREATE VIEW foo (col STRING) AS (SELECT * FROM bar) COMMENT 'Information about all entity controls by type.\nThe table contains one row for every entity control by type.\n'",
        "CREATE VIEW foo (col STRING) COMMENT='Information about all entity controls by type.\nThe table contains one row for every entity control by type.\n' AS (SELECT * FROM bar)"
    );
}

#[test]
fn parse_in_with_dangling_comma() {
    clickhouse().one_statement_parses_to(
        "SELECT * FROM latest_schemas WHERE workspace IN ('synq-ops', 'test',)",
        "SELECT * FROM latest_schemas WHERE workspace IN ('synq-ops', 'test')",
    );
}

#[test]
fn test_query_with_format_clause() {
    let format_options = vec!["TabSeparated", "JSONCompact", "NULL"];
    for format in &format_options {
        let sql = format!("SELECT * FROM t FORMAT {}", format);
        match clickhouse_and_generic().verified_stmt(&sql) {
            Statement::Query(query) => {
                if *format == "NULL" {
                    assert_eq!(query.format_clause, Some(FormatClause::Null));
                } else {
                    assert_eq!(
                        query.format_clause,
                        Some(FormatClause::Identifier(Ident::new(*format).empty_span()))
                    );
                }
            }
            _ => unreachable!(),
        }
    }

    let invalid_cases = [
        "SELECT * FROM t FORMAT",
        "SELECT * FROM t FORMAT TabSeparated JSONCompact",
        "SELECT * FROM t FORMAT TabSeparated TabSeparated",
    ];
    for sql in &invalid_cases {
        clickhouse_and_generic()
            .parse_sql_statements(sql)
            .expect_err("Expected: FORMAT {identifier}, found: ");
    }
}

#[test]
fn test_create_table_projection() {
    let sql = "CREATE TABLE default.runs (`workspace` LowCardinality(STRING), `id` STRING, `created_at` DateTime64(8, 'UTC'), PROJECTION by_id (SELECT * ORDER BY workspace, id))";
    match clickhouse().verified_stmt(sql) {
        Statement::CreateTable { projections, .. } => {
            assert_eq!(projections.len(), 1);
            assert_eq!(projections[0].name, Ident::new("by_id").empty_span());
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_create_table_codec_with_complex_types() {
    // Map type with CODEC
    clickhouse().one_statement_parses_to(
        "CREATE TABLE t (col Map(LowCardinality(String), String) CODEC(ZSTD(1)))",
        "",
    );

    // Array(Map) with CODEC
    clickhouse().one_statement_parses_to(
        "CREATE TABLE t (col Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)))",
        "",
    );

    // Multi-arg CODEC with Delta
    clickhouse().one_statement_parses_to(
        "CREATE TABLE t (col DateTime64(9) CODEC(Delta(8), ZSTD(1)))",
        "",
    );
}

#[test]
fn test_create_table_order_by_with_functions() {
    // ORDER BY with function calls (ClickHouse expressions in ORDER BY)
    clickhouse().one_statement_parses_to(
        "CREATE TABLE t (col DateTime64(9)) ORDER BY (col, toDateTime(col))",
        "",
    );

    // PRIMARY KEY with function calls
    clickhouse().one_statement_parses_to(
        "CREATE TABLE t (col String) PRIMARY KEY (col, toDateTime(col)) ORDER BY (col, toDateTime(col))",
        "",
    );
}

#[test]
fn test_create_table_index_expression() {
    let sql = "CREATE TABLE default.statuses (INDEX idx_created_at DATE(created_at) TYPE MinMax GRANULARITY 1)";
    match clickhouse().verified_stmt(sql) {
        Statement::CreateTable { constraints, .. } => {
            assert_eq!(constraints.len(), 1);
            match &constraints[0] {
                TableConstraint::ClickhouseIndex { name, .. } => {
                    assert_eq!(*name, Ident::new("idx_created_at").empty_span());
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_parametric_aggregate_functions() {
    // Basic parametric aggregate
    clickhouse().one_statement_parses_to(
        "SELECT groupArrayResample(30, 75, 30)(name, age) FROM people",
        "",
    );

    // Parametric aggregate with string parameter
    clickhouse().one_statement_parses_to(
        "SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t",
        "",
    );

    // Parametric aggregate with single parameter
    clickhouse().one_statement_parses_to(
        "SELECT histogram(5)(number + 1) FROM (SELECT * FROM system.numbers LIMIT 20)",
        "",
    );

    // Parametric aggregate in HAVING
    clickhouse().one_statement_parses_to(
        "SELECT SearchPhrase FROM SearchLog GROUP BY SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5",
        "",
    );

    // Multiple parametric aggregates with aliases
    clickhouse().one_statement_parses_to(
        "SELECT countResample(30, 75, 30)(name, age) AS amount, avgResample(30, 75, 30)(wage, age) AS avg_wage FROM people",
        "",
    );

    // Verify round-trip for simple case
    let sql = "SELECT groupArrayResample(30, 75, 30)(name, age) FROM people";
    clickhouse().verified_only_select(sql);
}

#[test]
fn test_months_interval() {
    let sql = "SELECT dateSub(now(), INTERVAL 3 MONTHS)";
    clickhouse().verified_only_select(sql);
}

#[test]
fn test_aggregate_function_column() {
    let funcs = vec!["AggregateFunction", "SimpleAggregateFunction"];
    for func in &funcs {
        clickhouse().verified_stmt(&format!("CREATE TABLE t (column1 {}(uniq, UInt64))", func));
        clickhouse().verified_stmt(&format!(
            "CREATE TABLE t (column1 {}(uniq, Array(UInt64)))",
            func
        ));
        clickhouse().verified_stmt(&format!(
            "CREATE TABLE t (column1 {}(uniq, Array(Tuple(DateTime64(8, 'UTC'), Int32))))",
            func
        ));
    }
}

#[test]
fn parse_create_table_as_another_table() {
    // ClickHouse supports CREATE TABLE t1 (...) AS t2
    // This is internally represented as SELECT * FROM t2
    clickhouse().one_statement_parses_to(
        "CREATE TABLE t1 (col1 String, col2 UInt64) ENGINE = MergeTree() AS t2",
        "CREATE TABLE t1 (col1 STRING, col2 UInt64) ENGINE=MergeTree() AS SELECT * FROM t2",
    );

    // Also works with qualified table names
    clickhouse().one_statement_parses_to(
        "CREATE TABLE t1 (col1 String) ENGINE = MergeTree() AS db1.t2",
        "CREATE TABLE t1 (col1 STRING) ENGINE=MergeTree() AS SELECT * FROM db1.t2",
    );
}

fn clickhouse() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {})],
        options: None,
    }
}

#[test]
fn parse_create_table_as_function() {
    // CREATE TABLE ... AS function(...) is used for table functions in ClickHouse
    clickhouse().one_statement_parses_to(
        "CREATE TABLE series AS generateSeries(1, 5)",
        "CREATE TABLE series AS SELECT * FROM generateSeries(1, 5)",
    );
}

#[test]
fn parse_select_from_table_final() {
    // ClickHouse: SELECT ... FROM table FINAL - FINAL is skipped (not stored in AST)
    clickhouse().one_statement_parses_to("SELECT * FROM t FINAL", "SELECT * FROM t");
    // FINAL after alias
    clickhouse().one_statement_parses_to("SELECT * FROM t AS t1 FINAL", "SELECT * FROM t AS t1");
}

#[test]
fn parse_explain_with_options() {
    // ClickHouse supports EXPLAIN with key=value options before the statement
    clickhouse().verified_stmt(
        "EXPLAIN distributed = 1 SELECT sum(number) FROM test_table GROUP BY number % 4",
    );

    clickhouse().one_statement_parses_to(
        "EXPLAIN distributed=1 SELECT * FROM remote('127.0.0.{1,2}', numbers(2)) WHERE number = 1",
        "EXPLAIN distributed = 1 SELECT * FROM remote('127.0.0.{1,2}', numbers(2)) WHERE number = 1",
    );

    // EXPLAIN SYNTAX with options
    clickhouse().one_statement_parses_to(
        "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c WHERE a.number = b.number AND b.number = c.number",
        "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM system.numbers AS a, system.numbers AS b, system.numbers AS c WHERE a.number = b.number AND b.number = c.number",
    );
}

#[test]
fn test_clickhouse_trailing_commas() {
    // ClickHouse supports trailing commas in SELECT
    clickhouse().one_statement_parses_to("SELECT 1, 2, FROM t", "SELECT 1, 2 FROM t");
    // Trailing comma with FORMAT clause
    clickhouse().one_statement_parses_to(
        "SELECT (number, toDate('2019-05-20')), dictGetOrNull('range_key_dictionary', 'value', number, toDate('2019-05-20')), FROM system.numbers LIMIT 5 FORMAT TabSeparated",
        "SELECT (number, toDate('2019-05-20')), dictGetOrNull('range_key_dictionary', 'value', number, toDate('2019-05-20')) FROM system.numbers LIMIT 5 FORMAT TabSeparated",
    );
}

#[test]
fn parse_execute_as() {
    // ClickHouse EXECUTE AS <user> (user impersonation)
    clickhouse().verified_stmt("EXECUTE AS u1");
}

#[test]
fn parse_columns_with_apply_transformers() {
    // ClickHouse COLUMNS('pattern') APPLY(func) syntax
    // https://clickhouse.com/docs/en/sql-reference/statements/select#columns-expression
    clickhouse().verified_stmt(
        "SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers",
    );

    // Single APPLY
    clickhouse().verified_stmt("SELECT COLUMNS('[jk]') APPLY(toString) FROM columns_transformers");

    // Verify AST structure
    let sql = "SELECT COLUMNS('[jk]') APPLY(toString) FROM columns_transformers";
    let select = clickhouse().verified_only_select(sql);
    match select.projection[0].clone().unwrap() {
        SelectItem::ColumnsWithTransformers {
            ref columns,
            ref transformers,
        } => {
            assert!(matches!(columns, Expr::Function(_)));
            assert_eq!(transformers.len(), 1);
            assert_eq!(
                transformers[0],
                ColumnTransformer::Apply(Ident::new("toString"))
            );
        }
        _ => panic!("Expected ColumnsWithTransformers"),
    }
}

#[test]
fn test_query_parameters() {
    // ClickHouse query parameters: {name: Type}
    let sql = "SELECT * FROM users WHERE id = {id: UInt32} AND name = {name: String}";
    clickhouse_and_generic().one_statement_parses_to(sql, sql);

    // ClickHouse query parameters in FROM clause: {name: Identifier}
    let sql = "SELECT * FROM {mytablename: Identifier}";
    clickhouse_and_generic().one_statement_parses_to(sql, sql);

    let sql = "SELECT * FROM {table: Identifier}";
    clickhouse_and_generic().one_statement_parses_to(sql, sql);
}

#[test]
fn parse_alter_table_add_projection() {
    clickhouse_and_generic().verified_stmt(
        "ALTER TABLE t ADD PROJECTION p (SELECT x ORDER BY x)",
    );
}

#[test]
fn parse_alter_table_add_projection_with_settings() {
    clickhouse_and_generic().one_statement_parses_to(
        "ALTER TABLE t ADD PROJECTION p (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 4096, index_granularity_bytes = 1048576)",
        "ALTER TABLE t ADD PROJECTION p (SELECT x ORDER BY x) WITH SETTINGS (index_granularity = 4096, index_granularity_bytes = 1048576)",
    );
}

#[test]
fn parse_alter_table_drop_projection() {
    clickhouse_and_generic().verified_stmt(
        "ALTER TABLE t DROP PROJECTION p",
    );
}

#[test]
fn parse_group_by_with_cube() {
    let sql = "SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE";
    clickhouse_and_generic().verified_stmt(sql);
}

#[test]
fn parse_group_by_with_rollup() {
    let sql = "SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP";
    clickhouse_and_generic().verified_stmt(sql);
}

#[test]
fn parse_group_by_with_totals() {
    let sql = "SELECT x, COUNT() FROM y GROUP BY x WITH TOTALS";
    clickhouse_and_generic().verified_stmt(sql);
}

fn clickhouse_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(ClickHouseDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}
