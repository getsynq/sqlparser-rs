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
use sqlparser::dialect::{DuckDbDialect, GenericDialect};

fn duckdb() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DuckDbDialect {})],
        options: None,
    }
}

fn duckdb_and_generic() -> TestedDialects {
    TestedDialects {
        dialects: vec![Box::new(DuckDbDialect {}), Box::new(GenericDialect {})],
        options: None,
    }
}

#[test]
fn test_select_wildcard_with_exclude() {
    let select = duckdb().verified_only_select("SELECT * EXCLUDE (col_a) FROM data");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![Ident::new("col_a")])),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select =
        duckdb().verified_only_select("SELECT name.* EXCLUDE department_id FROM employee_table");
    let expected = SelectItem::QualifiedWildcard(
        ObjectName(vec![Ident::new("name")]),
        WildcardAdditionalOptions {
            opt_exclude: Some(ExcludeSelectItem::Single(Ident::new("department_id"))),
            ..Default::default()
        },
    )
    .empty_span();
    assert_eq!(expected, select.projection[0]);

    let select = duckdb()
        .verified_only_select("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
    let expected = SelectItem::Wildcard(WildcardAdditionalOptions {
        opt_exclude: Some(ExcludeSelectItem::Multiple(vec![
            Ident::new("department_id"),
            Ident::new("employee_id"),
        ])),
        ..Default::default()
    })
    .empty_span();
    assert_eq!(expected, select.projection[0]);
}

#[test]
fn parse_div_infix() {
    duckdb_and_generic().verified_stmt(r#"SELECT 5 // 2"#);
}

#[test]
fn test_create_macro() {
    let macro_ = duckdb().verified_stmt("CREATE MACRO schema.add(a, b) AS a + b");
    let expected = Statement::CreateMacro {
        or_replace: false,
        temporary: false,
        name: ObjectName(vec![Ident::new("schema"), Ident::new("add")]),
        args: Some(vec![MacroArg::new("a"), MacroArg::new("b")]),
        definition: MacroDefinition::Expr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a").empty_span())),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("b").empty_span())),
        }),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_create_macro_default_args() {
    let macro_ = duckdb().verified_stmt("CREATE MACRO add_default(a, b := 5) AS a + b");
    let expected = Statement::CreateMacro {
        or_replace: false,
        temporary: false,
        name: ObjectName(vec![Ident::new("add_default")]),
        args: Some(vec![
            MacroArg::new("a"),
            MacroArg {
                name: Ident::new("b"),
                default_expr: Some(Expr::Value(number("5"))),
            },
        ]),
        definition: MacroDefinition::Expr(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("a").empty_span())),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Identifier(Ident::new("b").empty_span())),
        }),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_create_table_macro() {
    let query = "SELECT col1_value AS column1, col2_value AS column2 UNION ALL SELECT 'Hello' AS col1_value, 456 AS col2_value";
    let macro_ = duckdb().verified_stmt(
        &("CREATE OR REPLACE TEMPORARY MACRO dynamic_table(col1_value, col2_value) AS TABLE "
            .to_string()
            + query),
    );
    let expected = Statement::CreateMacro {
        or_replace: true,
        temporary: true,
        name: ObjectName(vec![Ident::new("dynamic_table")]),
        args: Some(vec![
            MacroArg::new("col1_value"),
            MacroArg::new("col2_value"),
        ]),
        definition: MacroDefinition::Table(duckdb().verified_query(query)),
    };
    assert_eq!(expected, macro_);
}

#[test]
fn test_select_union_by_name() {
    let ast = duckdb().verified_query("SELECT * FROM capitals UNION BY NAME SELECT * FROM weather");
    let expected = Box::<SetExpr>::new(SetExpr::SetOperation {
        op: SetOperator::Union,
        set_quantifier: SetQuantifier::ByName,
        left: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                opt_exclude: None,
                opt_except: None,
                opt_rename: None,
                opt_replace: None,
            })
            .empty_span()],
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: "capitals".to_string(),
                        quote_style: None,
                    }]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            sample: None,
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        }))),
        right: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                opt_exclude: None,
                opt_except: None,
                opt_rename: None,
                opt_replace: None,
            })
            .empty_span()],
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: "weather".to_string(),
                        quote_style: None,
                    }]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            sample: None,
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        }))),
    });

    assert_eq!(ast.body, expected);

    let ast =
        duckdb().verified_query("SELECT * FROM capitals UNION ALL BY NAME SELECT * FROM weather");
    let expected = Box::<SetExpr>::new(SetExpr::SetOperation {
        op: SetOperator::Union,
        set_quantifier: SetQuantifier::AllByName,
        left: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                opt_exclude: None,
                opt_except: None,
                opt_rename: None,
                opt_replace: None,
            })
            .empty_span()],
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: "capitals".to_string(),
                        quote_style: None,
                    }]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            sample: None,
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        }))),
        right: Box::<SetExpr>::new(SetExpr::Select(Box::new(Select {
            distinct: None,
            top: None,
            projection: vec![SelectItem::Wildcard(WildcardAdditionalOptions {
                opt_exclude: None,
                opt_except: None,
                opt_rename: None,
                opt_replace: None,
            })
            .empty_span()],
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(vec![Ident {
                        value: "weather".to_string(),
                        quote_style: None,
                    }]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                    version: None,
                    partitions: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            sample: None,
            selection: None,
            group_by: GroupByExpr::Expressions(vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            value_table_mode: None,
        }))),
    });
    assert_eq!(ast.body, expected);
}

#[test]
fn test_numeric_literal_underscores() {
    // Underscores in numeric literals are stripped during tokenization
    duckdb_and_generic().one_statement_parses_to(
        "SELECT 1_000_000",
        "SELECT 1000000",
    );
    duckdb_and_generic().one_statement_parses_to(
        "SELECT 1_2E+1_0::FLOAT",
        "SELECT CAST(12E+10 AS FLOAT)",
    );
    duckdb_and_generic().one_statement_parses_to(
        "SELECT 1_000.50_0",
        "SELECT 1000.500",
    );
}

#[test]
fn test_prefix_alias_colon_select() {
    // DuckDB prefix alias: `alias: expr` is equivalent to `expr AS alias`
    duckdb().one_statement_parses_to(
        "SELECT e: 1 + 2, f: len('asdf'), s: (SELECT 42)",
        "SELECT 1 + 2 AS e, len('asdf') AS f, (SELECT 42) AS s",
    );
}

#[test]
fn test_prefix_alias_colon_select_aggregation() {
    // DuckDB prefix alias with aggregation functions
    duckdb().one_statement_parses_to(
        "SELECT sum_qty: sum(l_quantity), avg_price: avg(l_extendedprice), count_order: count(*)",
        "SELECT sum(l_quantity) AS sum_qty, avg(l_extendedprice) AS avg_price, count(*) AS count_order",
    );
}

#[test]
fn test_prefix_alias_colon_from() {
    // DuckDB prefix alias in FROM clause: `alias: table` is equivalent to `table AS alias`
    duckdb().one_statement_parses_to(
        "SELECT * FROM foo: c.db.tbl",
        "SELECT * FROM c.db.tbl AS foo",
    );
}

#[test]
fn test_prefix_alias_colon_from_simple() {
    duckdb().one_statement_parses_to(
        "SELECT * FROM foo: bar",
        "SELECT * FROM bar AS foo",
    );
}

#[test]
fn test_prefix_alias_colon_from_function() {
    duckdb().one_statement_parses_to(
        "SELECT * FROM r: range(10)",
        "SELECT * FROM range(10) AS r",
    );
}

#[test]
fn test_prefix_alias_colon_from_multiple() {
    duckdb().one_statement_parses_to(
        "SELECT * FROM r: range(10), v: (VALUES (42))",
        "SELECT * FROM range(10) AS r, (VALUES (42)) AS v",
    );
}

#[test]
fn test_from_first_select() {
    // DuckDB FROM-first syntax: `FROM tbl` is equivalent to `SELECT * FROM tbl`
    duckdb().one_statement_parses_to("FROM tbl", "SELECT * FROM tbl");
}

#[test]
fn test_from_first_select_function() {
    duckdb().one_statement_parses_to("FROM range(10)", "SELECT * FROM range(10)");
}

#[test]
fn test_from_first_subquery() {
    // FROM-first query used as a subquery
    duckdb().one_statement_parses_to(
        "SELECT * FROM r: range(10), v: (VALUES (42)), s: (FROM range(10))",
        "SELECT * FROM range(10) AS r, (VALUES (42)) AS v, (SELECT * FROM range(10)) AS s",
    );
}

#[test]
fn test_from_first_with_where() {
    duckdb().one_statement_parses_to(
        "FROM tbl WHERE x > 1",
        "SELECT * FROM tbl WHERE x > 1",
    );
}

#[test]
fn test_map_literal() {
    duckdb().verified_stmt("SELECT MAP { 'x': 1 }");
    duckdb().verified_stmt("SELECT MAP { 'x': 1, 'y': 2 }");
    duckdb().verified_stmt("SELECT MAP {}");
}

#[test]
fn test_filter_without_where() {
    // DuckDB supports FILTER (expr) without WHERE keyword
    duckdb().one_statement_parses_to(
        "SELECT SUM(x) FILTER (x = 1)",
        "SELECT SUM(x) FILTER (WHERE x = 1)",
    );
    // Standard FILTER (WHERE expr) syntax should still work
    duckdb().verified_stmt("SELECT SUM(x) FILTER (WHERE x = 1)");
}
