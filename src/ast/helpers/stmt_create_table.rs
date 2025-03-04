#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, format, string::String, vec, vec::Vec};

use crate::ast::{SortKey, WithSpan};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::ast::{
    ColumnDef, DistributionStyle, EngineSpec, Expr, FileFormat, HiveDistributionStyle, HiveFormat,
    Ident, ObjectName, OnCommit, Query, SqlOption, Statement, TableConstraint,
};
use crate::parser::ParserError;
use sqlparser::ast::TableProjection;
#[cfg(feature = "visitor")]
use sqlparser_derive::{Visit, VisitMut};

/// Builder for create table statement variant ([1]).
///
/// This structure helps building and accessing a create table with more ease, without needing to:
/// - Match the enum itself a lot of times; or
/// - Moving a lot of variables around the code.
///
/// # Example
/// ```rust
/// use sqlparser::ast::helpers::stmt_create_table::CreateTableBuilder;
/// use sqlparser::ast::{ColumnDef, DataType, Ident, ObjectName};
/// let builder = CreateTableBuilder::new(ObjectName(vec![Ident::new("table_name")]))
///    .if_not_exists(true)
///    .columns(vec![ColumnDef {
///        name: Ident::new("c1"),
///        data_type: DataType::Int(None),
///        collation: None,
///        options: vec![],
/// }]);
/// // You can access internal elements with ease
/// assert!(builder.if_not_exists);
/// // Convert to a statement
/// assert_eq!(
///    builder.build().to_string(),
///    "CREATE TABLE IF NOT EXISTS table_name (c1 INT)"
/// )
/// ```
///
/// [1]: crate::ast::Statement::CreateTable
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateTableBuilder {
    pub or_replace: bool,
    pub temporary: bool,
    pub external: bool,
    pub global: Option<bool>,
    pub if_not_exists: bool,
    pub transient: bool,
    pub name: ObjectName,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub hive_distribution: HiveDistributionStyle,
    pub hive_formats: Option<HiveFormat>,
    pub dist_style: Option<DistributionStyle>,
    pub dist_key: Option<WithSpan<Ident>>,
    pub sort_key: Option<SortKey>,
    pub table_options: Vec<SqlOption>,
    pub projections: Vec<TableProjection>,
    pub table_properties: Vec<SqlOption>,
    pub with_options: Vec<SqlOption>,
    pub file_format: Option<FileFormat>,
    pub location: Option<String>,
    pub query: Option<Box<Query>>,
    pub without_rowid: bool,
    pub like: Option<ObjectName>,
    pub clone: Option<ObjectName>,
    pub engine: Option<EngineSpec>,
    pub comment: Option<String>,
    pub auto_increment_offset: Option<u32>,
    pub default_charset: Option<String>,
    pub collation: Option<String>,
    pub on_commit: Option<OnCommit>,
    pub partitioned_by: Option<Expr>,
    pub on_cluster: Option<String>,
    pub primary_key: Option<Vec<WithSpan<Ident>>>,
    pub order_by: Option<Vec<WithSpan<Ident>>>,
    pub cluster_by: Option<Vec<Expr>>,
    pub strict: bool,
    pub table_ttl: Option<Expr>,
    pub clickhouse_settings: Option<Vec<SqlOption>>,
    pub using: Option<ObjectName>,
    pub copy_grants: bool,
}

impl CreateTableBuilder {
    pub fn new(name: ObjectName) -> Self {
        Self {
            or_replace: false,
            temporary: false,
            external: false,
            global: None,
            if_not_exists: false,
            transient: false,
            name,
            columns: vec![],
            constraints: vec![],
            hive_distribution: HiveDistributionStyle::NONE,
            hive_formats: None,
            dist_style: None,
            dist_key: None,
            sort_key: None,
            table_options: vec![],
            projections: vec![],
            table_properties: vec![],
            with_options: vec![],
            file_format: None,
            location: None,
            query: None,
            without_rowid: false,
            like: None,
            clone: None,
            engine: None,
            comment: None,
            auto_increment_offset: None,
            default_charset: None,
            collation: None,
            on_commit: None,
            on_cluster: None,
            primary_key: None,
            order_by: None,
            partitioned_by: None,
            cluster_by: None,
            strict: false,
            table_ttl: None,
            clickhouse_settings: None,
            using: None,
            copy_grants: false,
        }
    }
    pub fn or_replace(mut self, or_replace: bool) -> Self {
        self.or_replace = or_replace;
        self
    }

    pub fn temporary(mut self, temporary: bool) -> Self {
        self.temporary = temporary;
        self
    }

    pub fn external(mut self, external: bool) -> Self {
        self.external = external;
        self
    }

    pub fn global(mut self, global: Option<bool>) -> Self {
        self.global = global;
        self
    }

    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    pub fn transient(mut self, transient: bool) -> Self {
        self.transient = transient;
        self
    }

    pub fn columns(mut self, columns: Vec<ColumnDef>) -> Self {
        self.columns = columns;
        self
    }

    pub fn constraints(mut self, constraints: Vec<TableConstraint>) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn hive_distribution(mut self, hive_distribution: HiveDistributionStyle) -> Self {
        self.hive_distribution = hive_distribution;
        self
    }

    pub fn hive_formats(mut self, hive_formats: Option<HiveFormat>) -> Self {
        self.hive_formats = hive_formats;
        self
    }

    pub fn dist_style(mut self, dist_style: Option<DistributionStyle>) -> Self {
        self.dist_style = dist_style;
        self
    }

    pub fn dist_key(mut self, dist_key: Option<WithSpan<Ident>>) -> Self {
        self.dist_key = dist_key;
        self
    }

    pub fn sort_key(mut self, sort_key: Option<SortKey>) -> Self {
        self.sort_key = sort_key;
        self
    }

    pub fn table_options(mut self, table_options: Vec<SqlOption>) -> Self {
        self.table_options = table_options;
        self
    }

    pub fn projections(mut self, projections: Vec<TableProjection>) -> Self {
        self.projections = projections;
        self
    }

    pub fn table_properties(mut self, table_properties: Vec<SqlOption>) -> Self {
        self.table_properties = table_properties;
        self
    }

    pub fn with_options(mut self, with_options: Vec<SqlOption>) -> Self {
        self.with_options = with_options;
        self
    }
    pub fn file_format(mut self, file_format: Option<FileFormat>) -> Self {
        self.file_format = file_format;
        self
    }
    pub fn location(mut self, location: Option<String>) -> Self {
        self.location = location;
        self
    }

    pub fn query(mut self, query: Option<Box<Query>>) -> Self {
        self.query = query;
        self
    }
    pub fn without_rowid(mut self, without_rowid: bool) -> Self {
        self.without_rowid = without_rowid;
        self
    }

    pub fn like(mut self, like: Option<ObjectName>) -> Self {
        self.like = like;
        self
    }

    // Different name to allow the object to be cloned
    pub fn clone_clause(mut self, clone: Option<ObjectName>) -> Self {
        self.clone = clone;
        self
    }

    pub fn engine(mut self, engine: Option<EngineSpec>) -> Self {
        self.engine = engine;
        self
    }

    pub fn comment(mut self, comment: Option<String>) -> Self {
        self.comment = comment;
        self
    }

    pub fn auto_increment_offset(mut self, offset: Option<u32>) -> Self {
        self.auto_increment_offset = offset;
        self
    }

    pub fn default_charset(mut self, default_charset: Option<String>) -> Self {
        self.default_charset = default_charset;
        self
    }

    pub fn collation(mut self, collation: Option<String>) -> Self {
        self.collation = collation;
        self
    }

    pub fn on_commit(mut self, on_commit: Option<OnCommit>) -> Self {
        self.on_commit = on_commit;
        self
    }

    pub fn partitioned_by(mut self, partitioned_by: Option<Expr>) -> Self {
        self.partitioned_by = partitioned_by;
        self
    }

    pub fn on_cluster(mut self, on_cluster: Option<String>) -> Self {
        self.on_cluster = on_cluster;
        self
    }

    pub fn primary_key(mut self, primary_key: Option<Vec<WithSpan<Ident>>>) -> Self {
        self.primary_key = primary_key;
        self
    }

    pub fn order_by(mut self, order_by: Option<Vec<WithSpan<Ident>>>) -> Self {
        self.order_by = order_by;
        self
    }

    pub fn cluster_by(mut self, cluster_by: Option<Vec<Expr>>) -> Self {
        self.cluster_by = cluster_by;
        self
    }

    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn table_ttl(mut self, table_ttl: Option<Expr>) -> Self {
        self.table_ttl = table_ttl;
        self
    }

    pub fn clickhouse_settings(mut self, clickhouse_settings: Option<Vec<SqlOption>>) -> Self {
        self.clickhouse_settings = clickhouse_settings;
        self
    }

    pub fn using(mut self, using: Option<ObjectName>) -> Self {
        self.using = using;
        self
    }

    pub fn copy_grants(mut self, copy_grants: bool) -> Self {
        self.copy_grants = copy_grants;
        self
    }

    pub fn build(self) -> Statement {
        Statement::CreateTable {
            or_replace: self.or_replace,
            temporary: self.temporary,
            external: self.external,
            global: self.global,
            if_not_exists: self.if_not_exists,
            transient: self.transient,
            name: self.name,
            columns: self.columns,
            constraints: self.constraints,
            hive_distribution: self.hive_distribution,
            hive_formats: self.hive_formats,
            dist_style: self.dist_style,
            dist_key: self.dist_key,
            sort_key: self.sort_key,
            table_options: self.table_options,
            projections: self.projections,
            table_properties: self.table_properties,
            with_options: self.with_options,
            file_format: self.file_format,
            location: self.location,
            query: self.query,
            without_rowid: self.without_rowid,
            like: self.like,
            clone: self.clone,
            engine: self.engine,
            comment: self.comment,
            auto_increment_offset: self.auto_increment_offset,
            default_charset: self.default_charset,
            collation: self.collation,
            on_commit: self.on_commit,
            on_cluster: self.on_cluster,
            primary_key: self.primary_key,
            order_by: self.order_by,
            partitioned_by: self.partitioned_by,
            cluster_by: self.cluster_by,
            strict: self.strict,
            table_ttl: self.table_ttl,
            clickhouse_settings: self.clickhouse_settings,
            using: self.using,
            copy_grants: self.copy_grants,
        }
    }
}

impl TryFrom<Statement> for CreateTableBuilder {
    type Error = ParserError;

    // As the builder can be transformed back to a statement, it shouldn't be a problem to take the
    // ownership.
    fn try_from(stmt: Statement) -> Result<Self, Self::Error> {
        match stmt {
            Statement::CreateTable {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                dist_style,
                dist_key,
                sort_key,
                table_options,
                projections,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                comment,
                auto_increment_offset,
                default_charset,
                collation,
                on_commit,
                partitioned_by,
                on_cluster,
                primary_key,
                order_by,
                cluster_by,
                strict,
                table_ttl,
                clickhouse_settings,
                using,
                copy_grants,
            } => Ok(Self {
                or_replace,
                temporary,
                external,
                global,
                if_not_exists,
                transient,
                name,
                columns,
                constraints,
                hive_distribution,
                hive_formats,
                dist_style,
                dist_key,
                sort_key,
                table_options,
                projections,
                table_properties,
                with_options,
                file_format,
                location,
                query,
                without_rowid,
                like,
                clone,
                engine,
                comment,
                auto_increment_offset,
                default_charset,
                collation,
                on_commit,
                partitioned_by,
                on_cluster,
                primary_key,
                order_by,
                cluster_by,
                strict,
                table_ttl,
                clickhouse_settings,
                using,
                copy_grants,
            }),
            _ => Err(ParserError::ParserError(format!(
                "Expected create table statement, but received: {stmt}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::helpers::stmt_create_table::CreateTableBuilder;
    use crate::ast::{Ident, ObjectName, Statement};
    use crate::parser::ParserError;

    #[test]
    pub fn test_from_valid_statement() {
        let builder = CreateTableBuilder::new(ObjectName(vec![Ident::new("table_name")]));

        let stmt = builder.clone().build();

        assert_eq!(builder, CreateTableBuilder::try_from(stmt).unwrap());
    }

    #[test]
    pub fn test_from_invalid_statement() {
        let stmt = Statement::Commit { chain: false };

        assert_eq!(
            CreateTableBuilder::try_from(stmt).unwrap_err(),
            ParserError::ParserError(
                "Expected create table statement, but received: COMMIT".to_owned()
            )
        );
    }
}
