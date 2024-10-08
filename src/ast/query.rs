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

#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

use crate::ast::*;

/// The most complete variant of a `SELECT` query expression, optionally
/// including `WITH`, `UNION` / other set operations, and `ORDER BY`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Query {
    /// WITH (common table expressions, or CTEs)
    pub with: Option<With>,
    /// SELECT or UNION / EXCEPT / INTERSECT
    pub body: Box<SetExpr>,
    /// ORDER BY
    pub order_by: Option<OrderBy>,
    /// `LIMIT { <N> | ALL }`
    pub limit: Option<Expr>,

    /// `LIMIT { <N> } BY { <expr>,<expr>,... } }`
    pub limit_by: Vec<Expr>,

    /// `OFFSET <N> [ { ROW | ROWS } ]`
    pub offset: Option<Offset>,
    /// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    pub fetch: Option<Fetch>,
    /// `FOR { UPDATE | SHARE } [ OF table_name ] [ SKIP LOCKED | NOWAIT ]`
    pub locks: Vec<LockClause>,
    /// ClickHouse syntax: `SELECT * FROM t SETTINGS key1 = value1, key2 = value2`
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select#settings-in-select-query)
    pub settings: Option<Vec<Setting>>,
    /// `SELECT * FROM t FORMAT JSONCompact`
    ///
    /// [ClickHouse](https://clickhouse.com/docs/en/sql-reference/statements/select/format)
    /// (ClickHouse-specific)
    pub format_clause: Option<FormatClause>,
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref with) = self.with {
            write!(f, "{with} ")?;
        }
        write!(f, "{}", self.body)?;
        if let Some(ref order_by) = self.order_by {
            write!(f, " ORDER BY")?;
            if !order_by.exprs.is_empty() {
                write!(f, " {}", display_comma_separated(&order_by.exprs))?;
            }
            if let Some(ref interpolate) = order_by.interpolate {
                match &interpolate.exprs {
                    Some(exprs) => write!(f, " INTERPOLATE ({})", display_comma_separated(exprs))?,
                    None => write!(f, " INTERPOLATE")?,
                }
            }
        }
        if let Some(ref limit) = self.limit {
            write!(f, " LIMIT {limit}")?;
        }
        if let Some(ref offset) = self.offset {
            write!(f, " {offset}")?;
        }
        if !self.limit_by.is_empty() {
            write!(f, " BY {}", display_separated(&self.limit_by, ", "))?;
        }
        if let Some(ref fetch) = self.fetch {
            write!(f, " {fetch}")?;
        }
        if !self.locks.is_empty() {
            write!(f, " {}", display_separated(&self.locks, " "))?;
        }
        if let Some(ref format) = self.format_clause {
            write!(f, " {}", format)?;
        }
        Ok(())
    }
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetExpr {
    /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    Select(Box<Select>),
    /// Parenthesized SELECT subquery, which may include more set operations
    /// in its body and an optional ORDER BY / LIMIT.
    Query(Box<Query>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: SetOperator,
        set_quantifier: SetQuantifier,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    Values(Values),
    Insert(Statement),
    Update(Statement),
    Table(Box<Table>),
}

impl SetExpr {
    /// If this `SetExpr` is a `SELECT`, returns the [`Select`].
    pub fn as_select(&self) -> Option<&Select> {
        if let Self::Select(select) = self {
            Some(&**select)
        } else {
            None
        }
    }
}

impl fmt::Display for SetExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetExpr::Select(s) => write!(f, "{s}"),
            SetExpr::Query(q) => write!(f, "({q})"),
            SetExpr::Values(v) => write!(f, "{v}"),
            SetExpr::Insert(v) => write!(f, "{v}"),
            SetExpr::Update(v) => write!(f, "{v}"),
            SetExpr::Table(t) => write!(f, "{t}"),
            SetExpr::SetOperation {
                left,
                right,
                op,
                set_quantifier,
            } => {
                write!(f, "{left} {op}")?;
                match set_quantifier {
                    SetQuantifier::All
                    | SetQuantifier::Distinct
                    | SetQuantifier::ByName
                    | SetQuantifier::AllByName => write!(f, " {set_quantifier}")?,
                    SetQuantifier::None => write!(f, "{set_quantifier}")?,
                }
                write!(f, " {right}")?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetOperator {
    Union,
    Except,
    Intersect,
}

impl fmt::Display for SetOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            SetOperator::Union => "UNION",
            SetOperator::Except => "EXCEPT",
            SetOperator::Intersect => "INTERSECT",
        })
    }
}

/// A quantifier for [SetOperator].
// TODO: Restrict parsing specific SetQuantifier in some specific dialects.
// For example, BigQuery does not support `DISTINCT` for `EXCEPT` and `INTERSECT`
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SetQuantifier {
    All,
    Distinct,
    ByName,
    AllByName,
    None,
}

impl fmt::Display for SetQuantifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetQuantifier::All => write!(f, "ALL"),
            SetQuantifier::Distinct => write!(f, "DISTINCT"),
            SetQuantifier::ByName => write!(f, "BY NAME"),
            SetQuantifier::AllByName => write!(f, "ALL BY NAME"),
            SetQuantifier::None => write!(f, ""),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
/// A [`TABLE` command]( https://www.postgresql.org/docs/current/sql-select.html#SQL-TABLE)
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Table {
    pub table_name: Option<String>,
    pub schema_name: Option<String>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref schema_name) = self.schema_name {
            write!(
                f,
                "TABLE {}.{}",
                schema_name,
                self.table_name.as_ref().unwrap(),
            )?;
        } else {
            write!(f, "TABLE {}", self.table_name.as_ref().unwrap(),)?;
        }
        Ok(())
    }
}

/// A restricted variant of `SELECT` (without CTEs/`ORDER BY`), which may
/// appear either as the only body item of a `Query`, or as an operand
/// to a set operation like `UNION`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Select {
    pub distinct: Option<Distinct>,
    /// MSSQL syntax: `TOP (<N>) [ PERCENT ] [ WITH TIES ]`
    pub top: Option<Top>,
    /// projection expressions
    pub projection: Vec<WithSpan<SelectItem>>,
    /// INTO
    pub into: Option<SelectInto>,
    /// FROM
    pub from: Vec<TableWithJoins>,
    /// LATERAL VIEWs
    pub lateral_views: Vec<LateralView>,
    /// WHERE
    pub selection: Option<WithSpan<Expr>>,
    /// GROUP BY
    pub group_by: GroupByExpr,
    /// CLUSTER BY (Hive)
    pub cluster_by: Vec<Expr>,
    /// DISTRIBUTE BY (Hive)
    pub distribute_by: Vec<Expr>,
    /// SORT BY (Hive)
    pub sort_by: Vec<Expr>,
    /// HAVING
    pub having: Option<Expr>,
    /// WINDOW AS
    pub named_window: Vec<NamedWindowDefinition>,
    /// QUALIFY (Snowflake)
    pub qualify: Option<Expr>,
    /// BigQuery syntax: `SELECT AS VALUE | SELECT AS STRUCT`
    pub value_table_mode: Option<ValueTableMode>,
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SELECT")?;

        if let Some(value_table_mode) = self.value_table_mode {
            write!(f, " {value_table_mode}")?;
        }

        if let Some(ref distinct) = self.distinct {
            write!(f, " {distinct}")?;
        }
        if let Some(ref top) = self.top {
            write!(f, " {top}")?;
        }
        write!(f, " {}", display_comma_separated(&self.projection))?;

        if let Some(ref into) = self.into {
            write!(f, " {into}")?;
        }

        if !self.from.is_empty() {
            write!(f, " FROM {}", display_comma_separated(&self.from))?;
        }
        if !self.lateral_views.is_empty() {
            for lv in &self.lateral_views {
                write!(f, "{lv}")?;
            }
        }
        if let Some(ref selection) = self.selection {
            write!(f, " WHERE {selection}")?;
        }
        match &self.group_by {
            GroupByExpr::All => write!(f, " GROUP BY ALL")?,
            GroupByExpr::Expressions(exprs) => {
                if !exprs.is_empty() {
                    write!(f, " GROUP BY {}", display_comma_separated(exprs))?;
                }
            }
        }
        if !self.cluster_by.is_empty() {
            write!(
                f,
                " CLUSTER BY {}",
                display_comma_separated(&self.cluster_by)
            )?;
        }
        if !self.distribute_by.is_empty() {
            write!(
                f,
                " DISTRIBUTE BY {}",
                display_comma_separated(&self.distribute_by)
            )?;
        }
        if !self.sort_by.is_empty() {
            write!(f, " SORT BY {}", display_comma_separated(&self.sort_by))?;
        }
        if let Some(ref having) = self.having {
            write!(f, " HAVING {having}")?;
        }
        if !self.named_window.is_empty() {
            write!(f, " WINDOW {}", display_comma_separated(&self.named_window))?;
        }
        if let Some(ref qualify) = self.qualify {
            write!(f, " QUALIFY {qualify}")?;
        }
        Ok(())
    }
}

/// A hive LATERAL VIEW with potential column aliases
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LateralView {
    /// LATERAL VIEW
    pub lateral_view: Expr,
    /// LATERAL VIEW table name
    pub lateral_view_name: ObjectName,
    /// LATERAL VIEW optional column aliases
    pub lateral_col_alias: Vec<WithSpan<Ident>>,
    /// LATERAL VIEW OUTER
    pub outer: bool,
}

impl fmt::Display for LateralView {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            " LATERAL VIEW{outer} {} {}",
            self.lateral_view,
            self.lateral_view_name,
            outer = if self.outer { " OUTER" } else { "" }
        )?;
        if !self.lateral_col_alias.is_empty() {
            write!(
                f,
                " AS {}",
                display_comma_separated(&self.lateral_col_alias)
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct NamedWindowDefinition(pub WithSpan<Ident>, pub WindowSpec);

impl fmt::Display for NamedWindowDefinition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS ({})", self.0, self.1)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct With {
    pub recursive: bool,
    pub cte_tables: Vec<Cte>,
}

impl fmt::Display for With {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "WITH {}{}",
            if self.recursive { "RECURSIVE " } else { "" },
            display_comma_separated(&self.cte_tables)
        )
    }
}

/// A single CTE (used after `WITH`): `alias [(col1, col2, ...)] AS ( query )`
/// The names in the column list before `AS`, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Cte {
    pub alias: TableAlias,
    pub query: Box<Query>,
    pub from: Option<Ident>,
}

impl fmt::Display for Cte {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS ({})", self.alias, self.query)?;
        if let Some(ref fr) = self.from {
            write!(f, " FROM {fr}")?;
        }
        Ok(())
    }
}

/// One item of the comma-separated list following `SELECT`
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SelectItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(WithSpan<Expr>),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias {
        expr: WithSpan<Expr>,
        alias: WithSpan<Ident>,
    },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(ObjectName, WildcardAdditionalOptions),
    /// An unqualified `*`
    Wildcard(WildcardAdditionalOptions),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum AggregateItem {
    /// Any expression, not followed by `[ AS ] alias`
    UnnamedExpr(Expr),
    /// An expression, followed by `[ AS ] alias`
    ExprWithAlias { expr: Expr, alias: Ident },
}

/// Single aliased identifier
///
/// # Syntax
/// ```plaintext
/// <ident> AS <alias>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct IdentWithAlias {
    pub ident: Ident,
    pub alias: Ident,
}

impl fmt::Display for IdentWithAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} AS {}", self.ident, self.alias)
    }
}

/// Additional options for wildcards, e.g. Snowflake `EXCLUDE`/`RENAME` and Bigquery `EXCEPT`.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WildcardAdditionalOptions {
    /// `[EXCLUDE...]`.
    pub opt_exclude: Option<ExcludeSelectItem>,
    /// `[EXCEPT...]`.
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#except>
    pub opt_except: Option<ExceptSelectItem>,
    /// `[RENAME ...]`.
    pub opt_rename: Option<RenameSelectItem>,
    /// `[REPLACE]`
    ///  BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_replace>
    ///  Clickhouse syntax: <https://clickhouse.com/docs/en/sql-reference/statements/select#replace>
    pub opt_replace: Option<ReplaceSelectItem>,
}

impl fmt::Display for WildcardAdditionalOptions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(exclude) = &self.opt_exclude {
            write!(f, " {exclude}")?;
        }
        if let Some(except) = &self.opt_except {
            write!(f, " {except}")?;
        }
        if let Some(rename) = &self.opt_rename {
            write!(f, " {rename}")?;
        }
        if let Some(replace) = &self.opt_replace {
            write!(f, " {replace}")?;
        }
        Ok(())
    }
}

/// Snowflake `EXCLUDE` information.
///
/// # Syntax
/// ```plaintext
/// <col_name>
/// | (<col_name>, <col_name>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ExcludeSelectItem {
    /// Single column name without parenthesis.
    ///
    /// # Syntax
    /// ```plaintext
    /// <col_name>
    /// ```
    Single(Ident),
    /// Multiple column names inside parenthesis.
    /// # Syntax
    /// ```plaintext
    /// (<col_name>, <col_name>, ...)
    /// ```
    Multiple(Vec<Ident>),
}

impl fmt::Display for ExcludeSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXCLUDE")?;
        match self {
            Self::Single(column) => {
                write!(f, " {column}")?;
            }
            Self::Multiple(columns) => {
                write!(f, " ({})", display_comma_separated(columns))?;
            }
        }
        Ok(())
    }
}

/// Snowflake `RENAME` information.
///
/// # Syntax
/// ```plaintext
/// <col_name> AS <col_alias>
/// | (<col_name> AS <col_alias>, <col_name> AS <col_alias>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum RenameSelectItem {
    /// Single column name with alias without parenthesis.
    ///
    /// # Syntax
    /// ```plaintext
    /// <col_name> AS <col_alias>
    /// ```
    Single(IdentWithAlias),
    /// Multiple column names with aliases inside parenthesis.
    /// # Syntax
    /// ```plaintext
    /// (<col_name> AS <col_alias>, <col_name> AS <col_alias>, ...)
    /// ```
    Multiple(Vec<IdentWithAlias>),
}

impl fmt::Display for RenameSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RENAME")?;
        match self {
            Self::Single(column) => {
                write!(f, " {column}")?;
            }
            Self::Multiple(columns) => {
                write!(f, " ({})", display_comma_separated(columns))?;
            }
        }
        Ok(())
    }
}

/// Bigquery `EXCEPT` information, with at least one column.
///
/// # Syntax
/// ```plaintext
/// EXCEPT (<col_name> [, ...])
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ExceptSelectItem {
    /// First guaranteed column.
    pub first_element: WithSpan<Ident>,
    /// Additional columns. This list can be empty.
    pub additional_elements: Vec<WithSpan<Ident>>,
}

impl fmt::Display for ExceptSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXCEPT ")?;
        if self.additional_elements.is_empty() {
            write!(f, "({})", self.first_element)?;
        } else {
            write!(
                f,
                "({}, {})",
                self.first_element,
                display_comma_separated(&self.additional_elements)
            )?;
        }
        Ok(())
    }
}

/// Bigquery `REPLACE` information.
///
/// # Syntax
/// ```plaintext
/// REPLACE (<new_expr> [AS] <col_name>)
/// REPLACE (<col_name> [AS] <col_alias>, <col_name> [AS] <col_alias>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReplaceSelectItem {
    pub items: Vec<Box<ReplaceSelectElement>>,
}

impl fmt::Display for ReplaceSelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "REPLACE")?;
        write!(f, " ({})", display_comma_separated(&self.items))?;
        Ok(())
    }
}

/// # Syntax
/// ```plaintext
/// <expr> [AS] <column_name>
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct ReplaceSelectElement {
    pub expr: Expr,
    pub column_name: Ident,
    pub as_keyword: bool,
}

impl fmt::Display for ReplaceSelectElement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.as_keyword {
            write!(f, "{} AS {}", self.expr, self.column_name)
        } else {
            write!(f, "{} {}", self.expr, self.column_name)
        }
    }
}

impl fmt::Display for SelectItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            SelectItem::UnnamedExpr(expr) => write!(f, "{expr}"),
            SelectItem::ExprWithAlias { expr, alias } => write!(f, "{expr} AS {alias}"),
            SelectItem::QualifiedWildcard(prefix, additional_options) => {
                write!(f, "{prefix}.*")?;
                write!(f, "{additional_options}")?;
                Ok(())
            }
            SelectItem::Wildcard(additional_options) => {
                write!(f, "*")?;
                write!(f, "{additional_options}")?;
                Ok(())
            }
        }
    }
}

impl fmt::Display for AggregateItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            AggregateItem::UnnamedExpr(expr) => write!(f, "{expr}"),
            AggregateItem::ExprWithAlias { expr, alias } => write!(f, "{expr} AS {alias}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableWithJoins {
    pub relation: TableFactor,
    pub joins: Vec<Join>,
}

impl fmt::Display for TableWithJoins {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.relation)?;
        for join in &self.joins {
            write!(f, "{join}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Setting {
    pub key: WithSpan<Ident>,
    pub value: Value,
}

impl fmt::Display for Setting {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} = {}", self.key, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SamplingMethod {
    Bernoulli,
    Row,
    System,
    Block,
}

impl Display for SamplingMethod {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SamplingMethod::Bernoulli => write!(f, "BERNOULLI"),
            SamplingMethod::Row => write!(f, "ROW"),
            SamplingMethod::System => write!(f, "SYSTEM"),
            SamplingMethod::Block => write!(f, "BLOCK"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum SelectionCount {
    FractionBased(Value),
    FixedSizeRows(Value),
}

impl Display for SelectionCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SelectionCount::FractionBased(v) => write!(f, "{}", v),
            SelectionCount::FixedSizeRows(v) => write!(f, "{} ROWS", v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableSampleSeed {
    Repeatable(Value),
    Seed(Value),
}

impl Display for TableSampleSeed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableSampleSeed::Repeatable(v) => write!(f, "REPEATABLE ({})", v),
            TableSampleSeed::Seed(v) => write!(f, "SEED ({})", v),
        }
    }
}

/// A table name or a parenthesized subquery with an optional alias
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
#[cfg_attr(feature = "visitor", visit(with = "visit_table_factor"))]
pub enum TableFactor {
    Table {
        #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
        name: ObjectName,
        alias: Option<TableAlias>,
        /// Arguments of a table-valued function, as supported by Postgres
        /// and MSSQL. Note that deprecated MSSQL `FROM foo (NOLOCK)` syntax
        /// will also be parsed as `args`.
        ///
        /// This field's value is `Some(v)`, where `v` is a (possibly empty)
        /// vector of arguments, in the case of a table-valued function call,
        /// whereas it's `None` in the case of a regular table name.
        args: Option<Vec<FunctionArg>>,
        /// MSSQL-specific `WITH (...)` hints such as NOLOCK.
        with_hints: Vec<Expr>,
        /// Optional version qualifier to facilitate table time-travel, as
        /// supported by BigQuery and MSSQL.
        version: Option<TableVersion>,
        /// [Partition selection](https://dev.mysql.com/doc/refman/8.0/en/partitioning-selection.html), supported by MySQL.
        partitions: Vec<Ident>,
    },
    FieldAccessor {
        expr: Expr,
        alias: Option<TableAlias>,
    },
    Derived {
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    /// `TABLE(<expr>)[ AS <alias> ]`
    TableFunction {
        expr: Expr,
        alias: Option<TableAlias>,
    },
    /// `e.g. LATERAL FLATTEN(<args>)[ AS <alias> ]`
    Function {
        lateral: bool,
        name: ObjectName,
        args: Vec<FunctionArg>,
        alias: Option<TableAlias>,
    },
    /// ```sql
    /// SELECT * FROM UNNEST ([10,20,30]) as numbers WITH OFFSET;
    /// +---------+--------+
    /// | numbers | offset |
    /// +---------+--------+
    /// | 10      | 0      |
    /// | 20      | 1      |
    /// | 30      | 2      |
    /// +---------+--------+
    /// ```
    UNNEST {
        alias: Option<TableAlias>,
        array_exprs: Vec<Expr>,
        with_offset: bool,
        with_offset_alias: Option<WithSpan<Ident>>,
    },
    /// ```sql
    /// EXTERNAL_QUERY('connection_id', '''external_database_query'''[, 'options'])
    /// ```
    ExternalQuery {
        connection_id: Value,
        external_database_query: Value,
        options: Option<Value>,
    },
    /// Represents a parenthesized table factor. The SQL spec only allows a
    /// join expression (`(foo <JOIN> bar [ <JOIN> baz ... ])`) to be nested,
    /// possibly several times.
    ///
    /// The parser may also accept non-standard nesting of bare tables for some
    /// dialects, but the information about such nesting is stripped from AST.
    NestedJoin {
        table_with_joins: Box<TableWithJoins>,
        alias: Option<TableAlias>,
    },
    /// Represents PIVOT operation on a table.
    /// For example `FROM monthly_sales PIVOT(sum(amount) FOR MONTH IN ('JAN', 'FEB'))`
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/pivot>
    Pivot {
        #[cfg_attr(feature = "visitor", visit(with = "visit_table_factor"))]
        table: Box<TableFactor>,
        aggregates: Vec<AggregateItem>,
        value_column: Vec<Ident>,
        pivot_values: Vec<Value>,
        alias: Option<TableAlias>,
    },
    /// An UNPIVOT operation on a table.
    ///
    /// Syntax:
    /// ```sql
    /// table UNPIVOT(value FOR name IN (column1, [ column2, ... ])) [ alias ]
    /// ```
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/unpivot>.
    Unpivot {
        #[cfg_attr(feature = "visitor", visit(with = "visit_table_factor"))]
        table: Box<TableFactor>,
        value: WithSpan<Ident>,
        name: WithSpan<Ident>,
        columns: Vec<WithSpan<Ident>>,
        alias: Option<TableAlias>,
    },

    /// An SAMPLE | TABLESAMPLE operation on a TableFactor as allowed in Snowflake.
    ///
    /// Syntax:
    /// ```sql
    /// SELECT ...
    /// FROM ...
    ///   { SAMPLE | TABLESAMPLE } [ samplingMethod ] ( { <probability> | <num> ROWS } ) [ { REPEATABLE | SEED } ( <seed> ) ]
    /// [ ... ]
    /// ```
    ///
    /// See <https://docs.snowflake.com/en/sql-reference/constructs/sample>.
    TableSample {
        #[cfg_attr(feature = "visitor", visit(with = "visit_table_factor"))]
        table: Box<TableFactor>,
        sample: bool,
        sampling_method: Option<SamplingMethod>,
        to_return: SelectionCount,
        seed: Option<TableSampleSeed>,
    },
}

impl fmt::Display for TableFactor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                partitions,
            } => {
                write!(f, "{name}")?;
                if !partitions.is_empty() {
                    write!(f, "PARTITION ({})", display_comma_separated(partitions))?;
                }
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args))?;
                }
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if !with_hints.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_hints))?;
                }
                if let Some(version) = version {
                    write!(f, "{version}")?;
                }
                Ok(())
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "({subquery})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => {
                if *lateral {
                    write!(f, "LATERAL ")?;
                }
                write!(f, "{name}")?;
                write!(f, "({})", display_comma_separated(args))?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::TableFunction { expr, alias } => {
                write!(f, "TABLE({expr})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
            } => {
                write!(f, "UNNEST({})", display_comma_separated(array_exprs))?;

                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                if *with_offset {
                    write!(f, " WITH OFFSET")?;
                }
                if let Some(alias) = with_offset_alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::ExternalQuery {
                connection_id,
                external_database_query,
                options,
            } => {
                write!(
                    f,
                    "EXTERNAL_QUERY({},{}",
                    connection_id, external_database_query
                )?;

                if let Some(options) = options {
                    write!(f, ", {options}")?;
                }
                write!(f, ")")?;
                Ok(())
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                write!(f, "({table_with_joins})")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::FieldAccessor { expr, alias } => {
                write!(f, "{expr}")?;
                if let Some(alias) = alias {
                    write!(f, " AS {alias}")?;
                }
                Ok(())
            }
            TableFactor::Pivot {
                table,
                aggregates: aggregate_projections,
                value_column,
                pivot_values,
                alias,
            } => {
                write!(
                    f,
                    "{} PIVOT({} FOR {} IN ({}))",
                    table,
                    display_comma_separated(aggregate_projections),
                    Expr::CompoundIdentifier(value_column.to_vec().empty_span()),
                    display_comma_separated(pivot_values)
                )?;
                if alias.is_some() {
                    write!(f, " AS {}", alias.as_ref().unwrap())?;
                }
                Ok(())
            }
            TableFactor::Unpivot {
                table,
                value,
                name,
                columns,
                alias,
            } => {
                write!(
                    f,
                    "{} UNPIVOT({} FOR {} IN ({}))",
                    table,
                    value,
                    name,
                    display_comma_separated(columns)
                )?;
                if alias.is_some() {
                    write!(f, " AS {}", alias.as_ref().unwrap())?;
                }
                Ok(())
            }

            TableFactor::TableSample {
                table,
                sample,
                sampling_method,
                to_return,
                seed,
            } => {
                write!(f, "{}", table)?;
                if *sample {
                    write!(f, " SAMPLE")?;
                } else {
                    write!(f, " TABLESAMPLE")?;
                }
                if let Some(sampling_method) = sampling_method {
                    write!(f, " {}", sampling_method)?;
                }
                write!(f, " ({})", to_return)?;
                if let Some(seed) = seed {
                    write!(f, " {}", seed)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct TableAlias {
    pub name: WithSpan<Ident>,
    pub columns: Vec<WithSpan<Ident>>,
}

impl fmt::Display for TableAlias {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum TableVersion {
    ForSystemTimeAsOf(Expr),
}

impl Display for TableVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableVersion::ForSystemTimeAsOf(e) => write!(f, " FOR SYSTEM_TIME AS OF {e}")?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Join {
    pub relation: TableFactor,
    pub join_operator: JoinOperator,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn prefix(constraint: &JoinConstraint) -> &'static str {
            match constraint {
                JoinConstraint::Natural => "NATURAL ",
                _ => "",
            }
        }
        fn suffix(constraint: &'_ JoinConstraint) -> impl fmt::Display + '_ {
            struct Suffix<'a>(&'a JoinConstraint);
            impl<'a> fmt::Display for Suffix<'a> {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    match self.0 {
                        JoinConstraint::On(expr) => write!(f, " ON {expr}"),
                        JoinConstraint::Using(attrs) => {
                            write!(f, " USING({})", display_comma_separated(attrs))
                        }
                        _ => Ok(()),
                    }
                }
            }
            Suffix(constraint)
        }
        match &self.join_operator {
            JoinOperator::Inner(constraint) => write!(
                f,
                " {}JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::LeftOuter(constraint) => write!(
                f,
                " {}LEFT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightOuter(constraint) => write!(
                f,
                " {}RIGHT JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::FullOuter(constraint) => write!(
                f,
                " {}FULL JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::CrossJoin(constraint) => {
                write!(f, " CROSS JOIN {}{}", self.relation, suffix(constraint))
            }
            JoinOperator::LeftSemi(constraint) => write!(
                f,
                " {}LEFT SEMI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightSemi(constraint) => write!(
                f,
                " {}RIGHT SEMI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::LeftAnti(constraint) => write!(
                f,
                " {}LEFT ANTI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::RightAnti(constraint) => write!(
                f,
                " {}RIGHT ANTI JOIN {}{}",
                prefix(constraint),
                self.relation,
                suffix(constraint)
            ),
            JoinOperator::CrossApply => write!(f, " CROSS APPLY {}", self.relation),
            JoinOperator::OuterApply => write!(f, " OUTER APPLY {}", self.relation),
            JoinOperator::Array => write!(f, " ARRAY JOIN {}", self.relation),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JoinOperator {
    Inner(JoinConstraint),
    LeftOuter(JoinConstraint),
    RightOuter(JoinConstraint),
    FullOuter(JoinConstraint),
    CrossJoin(JoinConstraint),
    /// LEFT SEMI (non-standard)
    LeftSemi(JoinConstraint),
    /// RIGHT SEMI (non-standard)
    RightSemi(JoinConstraint),
    /// LEFT ANTI (non-standard)
    LeftAnti(JoinConstraint),
    /// RIGHT ANTI (non-standard)
    RightAnti(JoinConstraint),
    /// CROSS APPLY (non-standard)
    CrossApply,
    /// OUTER APPLY (non-standard)
    OuterApply,
    /// ARRAY JOIN (ClickHouse)
    Array,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<WithSpan<Ident>>),
    Natural,
    None,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OrderBy {
    pub exprs: Vec<OrderByExpr>,
    /// Optional: `INTERPOLATE`
    /// Supported by [ClickHouse syntax]
    ///
    /// [ClickHouse syntax]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
    pub interpolate: Option<Interpolate>,
}

/// An `ORDER BY` expression
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct OrderByExpr {
    pub expr: Expr,
    /// Optional `ASC` or `DESC`
    pub asc: Option<bool>,
    /// Optional `NULLS FIRST` or `NULLS LAST`
    pub nulls_first: Option<bool>,
    /// Optional: `WITH FILL`
    /// Supported by [ClickHouse syntax]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
    pub with_fill: Option<WithFill>,
}

impl fmt::Display for OrderByExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        match self.asc {
            Some(true) => write!(f, " ASC")?,
            Some(false) => write!(f, " DESC")?,
            None => (),
        }
        match self.nulls_first {
            Some(true) => write!(f, " NULLS FIRST")?,
            Some(false) => write!(f, " NULLS LAST")?,
            None => (),
        }
        if let Some(ref with_fill) = self.with_fill {
            write!(f, " {}", with_fill)?
        }
        Ok(())
    }
}

/// ClickHouse `WITH FILL` modifier for `ORDER BY` clause.
/// Supported by [ClickHouse syntax]
///
/// [ClickHouse syntax]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct WithFill {
    pub from: Option<Expr>,
    pub to: Option<Expr>,
    pub step: Option<Expr>,
}

impl fmt::Display for WithFill {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WITH FILL")?;
        if let Some(ref from) = self.from {
            write!(f, " FROM {}", from)?;
        }
        if let Some(ref to) = self.to {
            write!(f, " TO {}", to)?;
        }
        if let Some(ref step) = self.step {
            write!(f, " STEP {}", step)?;
        }
        Ok(())
    }
}

/// ClickHouse `INTERPOLATE` clause for use in `ORDER BY` clause when using `WITH FILL` modifier.
/// Supported by [ClickHouse syntax]
///
/// [ClickHouse syntax]: <https://clickhouse.com/docs/en/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct InterpolateExpr {
    pub column: WithSpan<Ident>,
    pub expr: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Interpolate {
    pub exprs: Option<Vec<InterpolateExpr>>,
}

impl fmt::Display for InterpolateExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.column)?;
        if let Some(ref expr) = self.expr {
            write!(f, " AS {}", expr)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Offset {
    pub value: Expr,
    pub rows: OffsetRows,
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OFFSET {}{}", self.value, self.rows)
    }
}

/// Stores the keyword after `OFFSET <number>`
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum OffsetRows {
    /// Omitting ROW/ROWS is non-standard MySQL quirk.
    None,
    Row,
    Rows,
}

impl fmt::Display for OffsetRows {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OffsetRows::None => Ok(()),
            OffsetRows::Row => write!(f, " ROW"),
            OffsetRows::Rows => write!(f, " ROWS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Fetch {
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl fmt::Display for Fetch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let extension = if self.with_ties { "WITH TIES" } else { "ONLY" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(f, "FETCH FIRST {quantity}{percent} ROWS {extension}")
        } else {
            write!(f, "FETCH FIRST ROWS {extension}")
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct LockClause {
    pub lock_type: LockType,
    pub of: Option<ObjectName>,
    pub nonblock: Option<NonBlock>,
}

impl fmt::Display for LockClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FOR {}", &self.lock_type)?;
        if let Some(ref of) = self.of {
            write!(f, " OF {of}")?;
        }
        if let Some(ref nb) = self.nonblock {
            write!(f, " {nb}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum LockType {
    Share,
    Update,
}

impl fmt::Display for LockType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let select_lock = match self {
            LockType::Share => "SHARE",
            LockType::Update => "UPDATE",
        };
        write!(f, "{select_lock}")
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum NonBlock {
    Nowait,
    SkipLocked,
}

impl fmt::Display for NonBlock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let nonblock = match self {
            NonBlock::Nowait => "NOWAIT",
            NonBlock::SkipLocked => "SKIP LOCKED",
        };
        write!(f, "{nonblock}")
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum Distinct {
    /// DISTINCT
    Distinct,

    /// DISTINCT ON({column names})
    On(Vec<Expr>),
}

impl fmt::Display for Distinct {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Distinct::Distinct => write!(f, "DISTINCT"),
            Distinct::On(col_names) => {
                let col_names = display_comma_separated(col_names);
                write!(f, "DISTINCT ON ({col_names})")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Top {
    /// SQL semantic equivalent of LIMIT but with same structure as FETCH.
    pub with_ties: bool,
    pub percent: bool,
    pub quantity: Option<Expr>,
}

impl fmt::Display for Top {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let extension = if self.with_ties { " WITH TIES" } else { "" };
        if let Some(ref quantity) = self.quantity {
            let percent = if self.percent { " PERCENT" } else { "" };
            write!(f, "TOP ({quantity}){percent}{extension}")
        } else {
            write!(f, "TOP{extension}")
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct Values {
    /// Was there an explicit ROWs keyword (MySQL)?
    /// <https://dev.mysql.com/doc/refman/8.0/en/values.html>
    pub explicit_row: bool,
    pub rows: Vec<Vec<Expr>>,
}

impl fmt::Display for Values {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VALUES ")?;
        let prefix = if self.explicit_row { "ROW" } else { "" };
        let mut delim = "";
        for row in &self.rows {
            write!(f, "{delim}")?;
            delim = ", ";
            write!(f, "{prefix}({})", display_comma_separated(row))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct SelectInto {
    pub temporary: bool,
    pub unlogged: bool,
    pub table: bool,
    pub name: ObjectName,
}

impl fmt::Display for SelectInto {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let temporary = if self.temporary { " TEMPORARY" } else { "" };
        let unlogged = if self.unlogged { " UNLOGGED" } else { "" };
        let table = if self.table { " TABLE" } else { "" };

        write!(f, "INTO{}{}{} {}", temporary, unlogged, table, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum GroupByExpr {
    /// ALL syntax of [Snowflake], and [DuckDB]
    ///
    /// [Snowflake]: <https://docs.snowflake.com/en/sql-reference/constructs/group-by#label-group-by-all-columns>
    /// [DuckDB]:  <https://duckdb.org/docs/sql/query_syntax/groupby.html>
    All,

    /// Expressions
    Expressions(Vec<Expr>),
}

impl fmt::Display for GroupByExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GroupByExpr::All => write!(f, "GROUP BY ALL"),
            GroupByExpr::Expressions(col_names) => {
                let col_names = display_comma_separated(col_names);
                write!(f, "GROUP BY ({col_names})")
            }
        }
    }
}

/// FORMAT identifier or FORMAT NULL clause, specific to ClickHouse.
///
/// [ClickHouse]: <https://clickhouse.com/docs/en/sql-reference/statements/select/format>
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum FormatClause {
    Identifier(WithSpan<Ident>),
    Null,
}

impl fmt::Display for FormatClause {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FormatClause::Identifier(ident) => write!(f, "FORMAT {}", ident),
            FormatClause::Null => write!(f, "FORMAT NULL"),
        }
    }
}

/// BigQuery supports ValueTables which have 2 modes:
/// `SELECT AS STRUCT`
/// `SELECT AS VALUE`
/// <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#value_tables>
#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub enum ValueTableMode {
    AsStruct,
    AsValue,
}

impl fmt::Display for ValueTableMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ValueTableMode::AsStruct => write!(f, "AS STRUCT"),
            ValueTableMode::AsValue => write!(f, "AS VALUE"),
        }
    }
}
