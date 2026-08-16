use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dbflux_core::{
    CodeGenCapabilities, CodeGenScope, CodeGenerator, CodeGeneratorInfo, ColumnInfo, ColumnMeta,
    Connection, ConnectionExt, ConstraintInfo, ConstraintKind, CrudResult, DbError, DbKind,
    DbSchemaInfo, DescribeRequest, DocumentConnection, ExplainRequest, ForeignKeyInfo, IndexData,
    IndexInfo, KeyValueConnection, PlannedQuery, QueryHandle, QueryLanguage, QueryRequest,
    QueryResult, RelationalConnection, RelationalSchema, Row, RowDelete, RowInsert, RowPatch,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaLoadingStrategy, SchemaSnapshot, SemanticPlan,
    SemanticPlanKind, SemanticRequest, SortDirection, SqlDialect, SqlMutationGenerator,
    SqlQueryBuilder, TableInfo, Value, ViewInfo, generate_delete_template, generate_drop_table,
    generate_insert_template, generate_select_star, generate_update_template,
    render_semantic_filter_sql,
};

use crate::dialect::{
    TURSO_DIALECT, collect_filter_values, generate_create_table, kind_from_decltype,
    translate_filter_to_sql, turso_value_to_value,
};
use crate::driver::{METADATA, OpenedDatabase};
use crate::error::format_turso_query_error;
use crate::runtime::runtime;

pub(crate) struct TursoConnection {
    conn: Arc<Mutex<turso::Connection>>,
    cancelled: Arc<AtomicBool>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl TursoConnection {
    pub(crate) fn new(database: OpenedDatabase, path: PathBuf) -> Result<Self, DbError> {
        let conn = database.connect()?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            cancelled: Arc::new(AtomicBool::new(false)),
            path,
        })
    }
}

fn lock_conn(
    conn: &Mutex<turso::Connection>,
) -> Result<std::sync::MutexGuard<'_, turso::Connection>, DbError> {
    conn.lock()
        .map_err(|error| DbError::query_failed(format!("Lock error: {error}")))
}

impl Connection for TursoConnection {
    fn metadata(&self) -> &dbflux_core::DriverMetadata {
        &METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        let conn = lock_conn(&self.conn)?;
        let mut rows = runtime()
            .block_on(conn.query("SELECT 1", ()))
            .map_err(|error| format_turso_query_error(&error))?;
        runtime()
            .block_on(rows.next())
            .map_err(|error| format_turso_query_error(&error))?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn set_referential_integrity(&self, enabled: bool) -> Result<(), DbError> {
        let value = if enabled { "ON" } else { "OFF" };
        let conn = lock_conn(&self.conn)?;
        runtime()
            .block_on(conn.execute(&format!("PRAGMA foreign_keys = {value}"), ()))
            .map_err(|error| format_turso_query_error(&error))?;
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        self.cancelled.store(false, Ordering::SeqCst);
        let start = Instant::now();
        let conn = lock_conn(&self.conn)?;

        let statements = QueryLanguage::Sql.split_statements(&req.sql);
        if statements.len() > 1 {
            let mut result_sets: Vec<QueryResult> = Vec::with_capacity(statements.len());
            for statement in &statements {
                if self.cancelled.load(Ordering::SeqCst) {
                    return Err(DbError::Cancelled);
                }
                result_sets.push(execute_one_statement(
                    &conn,
                    statement,
                    req.limit,
                    start,
                    &self.cancelled,
                )?);
            }
            let mut primary = result_sets.remove(0);
            for extra in result_sets {
                primary.push_additional_result(extra);
            }
            return Ok(primary);
        }

        execute_one_statement(&conn, &req.sql, req.limit, start, &self.cancelled)
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        self.cancel_active()
    }

    fn cancel_active(&self) -> Result<(), DbError> {
        self.cancelled.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let conn = lock_conn(&self.conn)?;
        let tables = get_tables(&conn)?;
        let views = get_views(&conn)?;

        Ok(SchemaSnapshot::relational(RelationalSchema {
            databases: Vec::new(),
            current_database: None,
            schemas: vec![DbSchemaInfo {
                name: "main".to_string(),
                tables,
                views,
                custom_types: None,
            }],
            tables: Vec::new(),
            views: Vec::new(),
        }))
    }

    fn kind(&self) -> DbKind {
        DbKind::Turso
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn table_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        let conn = lock_conn(&self.conn)?;
        let columns = get_columns(&conn, table)?;
        let indexes = get_indexes(&conn, table)?;
        let foreign_keys = get_foreign_keys(&conn, table)?;
        let constraints = get_constraints(&conn, table)?;

        Ok(TableInfo {
            name: table.to_string(),
            schema: None,
            columns: Some(columns),
            indexes: Some(IndexData::Relational(indexes)),
            foreign_keys: Some(foreign_keys),
            constraints: Some(constraints),
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
        })
    }

    fn schema_indexes(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaIndexInfo>, DbError> {
        let conn = lock_conn(&self.conn)?;
        get_all_indexes(&conn)
    }

    fn schema_foreign_keys(
        &self,
        _database: &str,
        _schema: Option<&str>,
    ) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
        let conn = lock_conn(&self.conn)?;
        get_all_foreign_keys(&conn)
    }

    fn fetch_row_by_pk(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
        pk_column: &str,
        pk_value: &Value,
    ) -> Result<Option<HashMap<String, Value>>, DbError> {
        let sql = format!(
            "SELECT * FROM {} WHERE {} = {} LIMIT 1",
            TURSO_DIALECT.quote_identifier(table),
            TURSO_DIALECT.quote_identifier(pk_column),
            TURSO_DIALECT.value_to_literal(pk_value),
        );
        let result = self.execute(&QueryRequest::new(sql))?;
        let columns = result.columns;
        let Some(row) = result.rows.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(
            columns
                .into_iter()
                .zip(row)
                .map(|(col, val)| (col.name, val))
                .collect(),
        ))
    }

    fn referenced_tables(&self, query: &str) -> Option<Vec<dbflux_core::QueryTableRef>> {
        Some(dbflux_core::extract_referenced_tables(query))
    }

    fn code_generators(&self) -> Vec<CodeGeneratorInfo> {
        vec![
            CodeGeneratorInfo {
                id: "create_table".into(),
                label: "CREATE TABLE".into(),
                scope: CodeGenScope::Table,
                order: 10,
                destructive: false,
            },
            CodeGeneratorInfo {
                id: "drop_table".into(),
                label: "DROP TABLE".into(),
                scope: CodeGenScope::Table,
                order: 20,
                destructive: true,
            },
        ]
    }

    fn generate_code(&self, generator_id: &str, table: &TableInfo) -> Result<String, DbError> {
        match generator_id {
            "select_star" => Ok(generate_select_star(&TURSO_DIALECT, table, 100)),
            "insert" => Ok(generate_insert_template(&TURSO_DIALECT, table)),
            "update" => Ok(generate_update_template(&TURSO_DIALECT, table)),
            "delete" => Ok(generate_delete_template(&TURSO_DIALECT, table)),
            "create_table" => Ok(generate_create_table(table)),
            "drop_table" => Ok(generate_drop_table(&TURSO_DIALECT, table)),
            _ => Err(DbError::NotSupported(format!(
                "Code generator '{generator_id}' not supported"
            ))),
        }
    }

    fn update_row(&self, patch: &RowPatch) -> Result<CrudResult, DbError> {
        if !patch.identity.is_valid() {
            return Err(DbError::query_failed(
                "Cannot update row: invalid row identity (missing primary key)".to_string(),
            ));
        }
        if !patch.has_changes() {
            return Err(DbError::query_failed("No changes to save".to_string()));
        }

        let builder = SqlQueryBuilder::new(&TURSO_DIALECT);
        let update_sql = builder
            .build_update(patch, false)
            .ok_or_else(|| DbError::query_failed("Failed to build UPDATE query".to_string()))?;
        self.execute(&QueryRequest::new(update_sql))?;

        let select_sql = builder
            .build_select_by_identity(patch.schema.as_deref(), &patch.table, &patch.identity)
            .ok_or_else(|| DbError::query_failed("Failed to build SELECT query".to_string()))?;
        let result = self.execute(&QueryRequest::new(select_sql))?;
        if let Some(row) = result.rows.into_iter().next() {
            Ok(CrudResult::success(row))
        } else {
            Ok(CrudResult::empty())
        }
    }

    fn insert_row(&self, insert: &RowInsert) -> Result<CrudResult, DbError> {
        if !insert.is_valid() {
            return Err(DbError::query_failed(
                "Cannot insert row: no columns specified".to_string(),
            ));
        }

        let builder = SqlQueryBuilder::new(&TURSO_DIALECT);
        let insert_sql = builder
            .build_insert(insert, false)
            .ok_or_else(|| DbError::query_failed("Failed to build INSERT query".to_string()))?;
        self.execute(&QueryRequest::new(insert_sql))?;

        let table_name = TURSO_DIALECT.qualified_table(insert.schema.as_deref(), &insert.table);
        let result = self.execute(&QueryRequest::new(format!(
            "SELECT * FROM {table_name} WHERE rowid = last_insert_rowid() LIMIT 1"
        )))?;
        if let Some(row) = result.rows.into_iter().next() {
            Ok(CrudResult::success(row))
        } else {
            Ok(CrudResult::new(1, None))
        }
    }

    fn delete_row(&self, delete: &RowDelete) -> Result<CrudResult, DbError> {
        if !delete.is_valid() {
            return Err(DbError::query_failed(
                "Cannot delete row: invalid row identity (missing primary key)".to_string(),
            ));
        }

        let builder = SqlQueryBuilder::new(&TURSO_DIALECT);
        let select_sql = builder
            .build_select_by_identity(delete.schema.as_deref(), &delete.table, &delete.identity)
            .ok_or_else(|| DbError::query_failed("Failed to build SELECT query".to_string()))?;
        let fetched = self.execute(&QueryRequest::new(select_sql))?;
        let returning_row = fetched.rows.into_iter().next();

        let delete_sql = builder
            .build_delete(delete, false)
            .ok_or_else(|| DbError::query_failed("Failed to build DELETE query".to_string()))?;
        self.execute(&QueryRequest::new(delete_sql))?;
        Ok(CrudResult::new(
            if returning_row.is_some() { 1 } else { 0 },
            returning_row,
        ))
    }

    fn explain(&self, request: &ExplainRequest) -> Result<QueryResult, DbError> {
        let query = match &request.query {
            Some(query) => query.clone(),
            None => format!(
                "SELECT * FROM {} LIMIT 100",
                request.table.quoted_with(self.dialect())
            ),
        };
        self.execute(&QueryRequest::new(format!("EXPLAIN QUERY PLAN {query}")))
    }

    fn describe_table(&self, request: &DescribeRequest) -> Result<QueryResult, DbError> {
        let sql = format!(
            "PRAGMA table_info({})",
            self.dialect().quote_identifier(&request.table.name)
        );
        self.execute(&QueryRequest::new(sql))
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &TURSO_DIALECT
    }

    fn code_generator(&self) -> &dyn CodeGenerator {
        &TURSO_CODE_GENERATOR
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        static GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&TURSO_DIALECT);
        Some(&GENERATOR)
    }

    fn plan_semantic_request(&self, request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
        plan_turso_semantic_request(request)
    }

    fn build_select_sql(
        &self,
        table: &str,
        columns: &[String],
        filter: Option<&Value>,
        order_by: &[dbflux_core::OrderByColumn],
        limit: u32,
        offset: u32,
    ) -> String {
        let quoted_table = TURSO_DIALECT.quote_identifier(table);
        let cols = if columns.is_empty() {
            "*".to_string()
        } else {
            columns
                .iter()
                .map(|col| TURSO_DIALECT.quote_identifier(col))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut sql = format!("SELECT {cols} FROM {quoted_table}");
        if let Some(filter) = filter {
            let where_clause = translate_filter_to_sql(filter);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
        }
        if !order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_parts = order_by
                .iter()
                .map(|col| {
                    let direction = match col.direction {
                        SortDirection::Ascending => "ASC",
                        SortDirection::Descending => "DESC",
                    };
                    format!("{} {direction}", col.column.quoted_with(&TURSO_DIALECT))
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&order_parts);
        }
        sql.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));
        sql
    }

    fn build_insert_sql(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
    ) -> (String, Vec<Value>) {
        let quoted_table = TURSO_DIALECT.quote_identifier(table);
        let cols = columns
            .iter()
            .map(|col| TURSO_DIALECT.quote_identifier(col))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = values
            .iter()
            .map(|_| "?".to_string())
            .collect::<Vec<_>>()
            .join(", ");
        (
            format!("INSERT INTO {quoted_table} ({cols}) VALUES ({placeholders})"),
            values.to_vec(),
        )
    }

    fn build_update_sql(
        &self,
        table: &str,
        set: &[(String, Value)],
        filter: Option<&Value>,
    ) -> (String, Vec<Value>) {
        let quoted_table = TURSO_DIALECT.quote_identifier(table);
        let set_str = set
            .iter()
            .map(|(col, _)| format!("{} = ?", TURSO_DIALECT.quote_identifier(col)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("UPDATE {quoted_table} SET {set_str}");
        if let Some(filter) = filter {
            let where_clause = translate_filter_to_sql(filter);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
        }
        let mut params: Vec<Value> = set.iter().map(|(_, value)| value.clone()).collect();
        if let Some(filter) = filter {
            collect_filter_values(filter, &mut params);
        }
        (sql, params)
    }

    fn build_delete_sql(&self, table: &str, filter: Option<&Value>) -> (String, Vec<Value>) {
        let quoted_table = TURSO_DIALECT.quote_identifier(table);
        let mut sql = format!("DELETE FROM {quoted_table}");
        let mut params = Vec::new();
        if let Some(filter) = filter {
            let where_clause = translate_filter_to_sql(filter);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
            collect_filter_values(filter, &mut params);
        }
        (sql, params)
    }

    fn build_count_sql(&self, table: &str, filter: Option<&Value>) -> String {
        let quoted_table = TURSO_DIALECT.quote_identifier(table);
        let mut sql = format!("SELECT COUNT(*) FROM {quoted_table}");
        if let Some(filter) = filter {
            let where_clause = translate_filter_to_sql(filter);
            if !where_clause.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clause);
            }
        }
        sql
    }

    fn build_truncate_sql(&self, table: &str) -> String {
        format!("DELETE FROM {}", TURSO_DIALECT.quote_identifier(table))
    }

    fn build_drop_index_sql(
        &self,
        index_name: &str,
        _table_name: Option<&str>,
        if_exists: bool,
    ) -> String {
        let quoted_index = TURSO_DIALECT.quote_identifier(index_name);
        if if_exists {
            format!("DROP INDEX IF EXISTS {quoted_index}")
        } else {
            format!("DROP INDEX {quoted_index}")
        }
    }

    fn version_query(&self) -> &'static str {
        "SELECT sqlite_version()"
    }

    fn supports_transactional_ddl(&self) -> bool {
        true
    }

    fn translate_filter(&self, filter: &Value) -> Result<String, DbError> {
        Ok(translate_filter_to_sql(filter))
    }
}

impl RelationalConnection for TursoConnection {}

impl ConnectionExt for TursoConnection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        Some(self)
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        None
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        None
    }
}

struct TursoCodeGenerator;

static TURSO_CODE_GENERATOR: TursoCodeGenerator = TursoCodeGenerator;

impl CodeGenerator for TursoCodeGenerator {
    fn capabilities(&self) -> CodeGenCapabilities {
        CodeGenCapabilities::CRUD
            | CodeGenCapabilities::INDEXES
            | CodeGenCapabilities::REINDEX
            | CodeGenCapabilities::CREATE_TABLE
            | CodeGenCapabilities::DROP_TABLE
            | CodeGenCapabilities::ADD_COLUMN
    }
}

fn execute_one_statement(
    conn: &turso::Connection,
    sql: &str,
    limit: Option<u32>,
    start: Instant,
    cancelled: &AtomicBool,
) -> Result<QueryResult, DbError> {
    if cancelled.load(Ordering::SeqCst) {
        return Err(DbError::Cancelled);
    }

    let sql_trimmed = sql.trim().to_uppercase();
    let is_query = sql_trimmed.starts_with("SELECT")
        || sql_trimmed.starts_with("PRAGMA")
        || sql_trimmed.starts_with("EXPLAIN");

    if is_query {
        let mut rows = runtime()
            .block_on(conn.query(sql, ()))
            .map_err(|error| format_turso_query_error(&error))?;

        let columns = column_meta_from_rows(&rows);
        let mut collected: Vec<Row> = Vec::new();

        loop {
            if cancelled.load(Ordering::SeqCst) {
                return Err(DbError::Cancelled);
            }
            let next = runtime()
                .block_on(rows.next())
                .map_err(|error| format_turso_query_error(&error))?;
            let Some(row) = next else {
                break;
            };

            let mut values = Vec::with_capacity(columns.len());
            for index in 0..columns.len() {
                match row.get_value(index) {
                    Ok(value) => values.push(turso_value_to_value(value)),
                    Err(_) => values.push(Value::Null),
                }
            }
            collected.push(values);

            if let Some(row_limit) = limit
                && collected.len() >= row_limit as usize
            {
                break;
            }
        }

        Ok(QueryResult::table(
            columns,
            collected,
            None,
            start.elapsed(),
        ))
    } else {
        let affected = runtime()
            .block_on(conn.execute(sql, ()))
            .map_err(|error| format_turso_query_error(&error))?;
        Ok(QueryResult::table(
            vec![],
            vec![],
            Some(affected),
            start.elapsed(),
        ))
    }
}

fn column_meta_from_rows(rows: &turso::Rows) -> Vec<ColumnMeta> {
    rows.columns()
        .into_iter()
        .map(|column| {
            let type_name = column.decl_type().unwrap_or("TEXT").to_uppercase();
            let kind = kind_from_decltype(column.decl_type());
            ColumnMeta {
                name: column.name().to_string(),
                type_name,
                kind,
                nullable: true,
                is_primary_key: false,
            }
        })
        .collect()
}

fn query_text_column(conn: &turso::Connection, sql: &str) -> Result<Vec<String>, DbError> {
    let result = execute_one_statement(conn, sql, None, Instant::now(), &AtomicBool::new(false))?;
    Ok(result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .map(|value| match value {
            Value::Text(text) => text,
            other => other.to_string(),
        })
        .collect())
}

fn get_tables(conn: &turso::Connection) -> Result<Vec<TableInfo>, DbError> {
    let names = query_text_column(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    Ok(names
        .into_iter()
        .map(|name| TableInfo {
            name,
            schema: None,
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: dbflux_core::CollectionPresentation::DataGrid,
            child_items: None,
            storage_hints: None,
        })
        .collect())
}

fn get_views(conn: &turso::Connection) -> Result<Vec<ViewInfo>, DbError> {
    let names = query_text_column(
        conn,
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name",
    )?;
    Ok(names
        .into_iter()
        .map(|name| ViewInfo { name, schema: None })
        .collect())
}

fn get_columns(conn: &turso::Connection, table: &str) -> Result<Vec<ColumnInfo>, DbError> {
    let exists = query_text_column(
        conn,
        &format!(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = {}",
            TURSO_DIALECT.value_to_literal(&Value::Text(table.to_string()))
        ),
    )?;
    if exists.is_empty() {
        return Err(DbError::ObjectNotFound(
            format!("Table '{table}' not found").into(),
        ));
    }

    let result = execute_one_statement(
        conn,
        &format!(
            "PRAGMA table_info({})",
            TURSO_DIALECT.quote_identifier(table)
        ),
        None,
        Instant::now(),
        &AtomicBool::new(false),
    )?;

    Ok(result
        .rows
        .into_iter()
        .filter_map(|row| {
            // cid, name, type, notnull, dflt_value, pk
            let name = row.get(1).and_then(value_as_text)?;
            let type_name = row.get(2).and_then(value_as_text).unwrap_or_default();
            let notnull = int_value(row.get(3)) != 0;
            let default_value = row.get(4).and_then(value_as_text);
            let pk = int_value(row.get(5));
            Some(ColumnInfo {
                name,
                type_name,
                nullable: !notnull && pk == 0,
                is_primary_key: pk > 0,
                default_value,
                enum_values: None,
            })
        })
        .collect())
}

fn get_indexes(conn: &turso::Connection, table: &str) -> Result<Vec<IndexInfo>, DbError> {
    let list = execute_one_statement(
        conn,
        &format!(
            "PRAGMA index_list({})",
            TURSO_DIALECT.quote_identifier(table)
        ),
        None,
        Instant::now(),
        &AtomicBool::new(false),
    )?;

    let mut indexes = Vec::new();
    for row in list.rows {
        let Some(name) = row.get(1).and_then(value_as_text) else {
            continue;
        };
        let is_unique = int_value(row.get(2)) == 1;
        let columns = index_columns(conn, &name)?;
        indexes.push(IndexInfo {
            name,
            columns,
            is_unique,
            is_primary: false,
        });
    }
    Ok(indexes)
}

fn index_columns(conn: &turso::Connection, index_name: &str) -> Result<Vec<String>, DbError> {
    let result = execute_one_statement(
        conn,
        &format!(
            "PRAGMA index_info({})",
            TURSO_DIALECT.quote_identifier(index_name)
        ),
        None,
        Instant::now(),
        &AtomicBool::new(false),
    )?;
    Ok(result
        .rows
        .into_iter()
        .filter_map(|row| row.get(2).and_then(value_as_text))
        .collect())
}

fn get_foreign_keys(conn: &turso::Connection, table: &str) -> Result<Vec<ForeignKeyInfo>, DbError> {
    let result = execute_one_statement(
        conn,
        &format!(
            "PRAGMA foreign_key_list({})",
            TURSO_DIALECT.quote_identifier(table)
        ),
        None,
        Instant::now(),
        &AtomicBool::new(false),
    )?;

    let mut fk_map: HashMap<i64, ForeignKeyInfo> = HashMap::new();
    for row in result.rows {
        let id = int_value(row.first());
        let ref_table = row.get(2).and_then(value_as_text).unwrap_or_default();
        let from_col = row.get(3).and_then(value_as_text).unwrap_or_default();
        let to_col = row.get(4).and_then(value_as_text).unwrap_or_default();
        let on_update = row.get(5).and_then(value_as_text).unwrap_or_default();
        let on_delete = row.get(6).and_then(value_as_text).unwrap_or_default();
        let entry = fk_map.entry(id).or_insert_with(|| ForeignKeyInfo {
            name: format!("fk_{id}"),
            columns: Vec::new(),
            referenced_table: ref_table,
            referenced_schema: None,
            referenced_columns: Vec::new(),
            on_update: if on_update == "NO ACTION" {
                None
            } else {
                Some(on_update)
            },
            on_delete: if on_delete == "NO ACTION" {
                None
            } else {
                Some(on_delete)
            },
        });
        entry.columns.push(from_col);
        entry.referenced_columns.push(to_col);
    }
    Ok(fk_map.into_values().collect())
}

fn get_constraints(conn: &turso::Connection, table: &str) -> Result<Vec<ConstraintInfo>, DbError> {
    let mut constraints = Vec::new();
    let create_sql = query_text_column(
        conn,
        &format!(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name = {}",
            TURSO_DIALECT.value_to_literal(&Value::Text(table.to_string()))
        ),
    )?
    .into_iter()
    .next();

    if let Some(create_sql) = create_sql
        && create_sql.to_uppercase().contains("CHECK")
    {
        for (index, part) in create_sql.split("CHECK").skip(1).enumerate() {
            if let Some(paren_start) = part.find('(') {
                let mut depth = 1;
                let mut end = paren_start + 1;
                for ch in part[paren_start + 1..].chars() {
                    if ch == '(' {
                        depth += 1;
                    } else if ch == ')' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    end += ch.len_utf8();
                }
                let check_expr = part[paren_start + 1..end].trim().to_string();
                constraints.push(ConstraintInfo {
                    name: format!("check_{index}"),
                    kind: ConstraintKind::Check,
                    columns: Vec::new(),
                    check_clause: Some(check_expr),
                });
            }
        }
    }

    let list = execute_one_statement(
        conn,
        &format!(
            "PRAGMA index_list({})",
            TURSO_DIALECT.quote_identifier(table)
        ),
        None,
        Instant::now(),
        &AtomicBool::new(false),
    )?;
    for row in list.rows {
        let origin = row.get(3).and_then(value_as_text).unwrap_or_default();
        if origin != "u" {
            continue;
        }
        let name = row.get(1).and_then(value_as_text).unwrap_or_default();
        let columns = index_columns(conn, &name)?;
        constraints.push(ConstraintInfo {
            name,
            kind: ConstraintKind::Unique,
            columns,
            check_clause: None,
        });
    }

    Ok(constraints)
}

fn get_all_indexes(conn: &turso::Connection) -> Result<Vec<SchemaIndexInfo>, DbError> {
    let tables = get_tables(conn)?;
    let mut all = Vec::new();
    for table in tables {
        let list = execute_one_statement(
            conn,
            &format!(
                "PRAGMA index_list({})",
                TURSO_DIALECT.quote_identifier(&table.name)
            ),
            None,
            Instant::now(),
            &AtomicBool::new(false),
        )?;
        for row in list.rows {
            let name = row.get(1).and_then(value_as_text).unwrap_or_default();
            let is_unique = int_value(row.get(2)) == 1;
            let origin = row.get(3).and_then(value_as_text).unwrap_or_default();
            let columns = index_columns(conn, &name)?;
            all.push(SchemaIndexInfo {
                name,
                table_name: table.name.clone(),
                columns,
                is_unique,
                is_primary: origin == "pk",
            });
        }
    }
    Ok(all)
}

fn get_all_foreign_keys(conn: &turso::Connection) -> Result<Vec<SchemaForeignKeyInfo>, DbError> {
    let tables = get_tables(conn)?;
    let mut all = Vec::new();
    for table in tables {
        for fk in get_foreign_keys(conn, &table.name)? {
            all.push(SchemaForeignKeyInfo {
                name: format!("{}_{}", table.name, fk.name),
                table_name: table.name.clone(),
                columns: fk.columns,
                referenced_schema: None,
                referenced_table: fk.referenced_table,
                referenced_columns: fk.referenced_columns,
                on_update: fk.on_update,
                on_delete: fk.on_delete,
            });
        }
    }
    Ok(all)
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Int(value) => Some(value.to_string()),
        Value::Float(value) => Some(value.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn int_value(value: Option<&Value>) -> i64 {
    match value {
        Some(Value::Int(value)) => *value,
        Some(Value::Text(text)) => text.parse().unwrap_or(0),
        Some(Value::Float(value)) => *value as i64,
        _ => 0,
    }
}

fn plan_turso_semantic_request(request: &SemanticRequest) -> Result<SemanticPlan, DbError> {
    match request {
        SemanticRequest::TableBrowse(browse) => {
            let mut sql = format!("SELECT * FROM {}", browse.table.quoted_with(&TURSO_DIALECT));
            if let Some(filter) = browse.semantic_filter.as_ref() {
                sql.push_str(" WHERE ");
                sql.push_str(&render_semantic_filter_sql(filter, &TURSO_DIALECT)?);
            }
            if !browse.order_by.is_empty() {
                sql.push_str(" ORDER BY ");
                let order_by = browse
                    .order_by
                    .iter()
                    .map(|column| {
                        let direction = match column.direction {
                            SortDirection::Ascending => "ASC",
                            SortDirection::Descending => "DESC",
                        };
                        format!("{} {direction}", column.column.quoted_with(&TURSO_DIALECT))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                sql.push_str(&order_by);
            }
            Ok(SemanticPlan::single_query(
                SemanticPlanKind::Query,
                PlannedQuery::new(QueryLanguage::Sql, sql),
            ))
        }
        SemanticRequest::Explain(explain) => {
            let query = explain.query.clone().unwrap_or_else(|| {
                format!(
                    "SELECT * FROM {} LIMIT 100",
                    explain.table.quoted_with(&TURSO_DIALECT)
                )
            });
            Ok(SemanticPlan::single_query(
                SemanticPlanKind::Query,
                PlannedQuery::new(QueryLanguage::Sql, format!("EXPLAIN QUERY PLAN {query}")),
            ))
        }
        SemanticRequest::Describe(describe) => Ok(SemanticPlan::single_query(
            SemanticPlanKind::Query,
            PlannedQuery::new(
                QueryLanguage::Sql,
                format!(
                    "PRAGMA table_info({})",
                    TURSO_DIALECT.quote_identifier(&describe.table.name)
                ),
            ),
        )),
        _ => Err(DbError::NotSupported(
            "Unsupported semantic request for Turso".to_string(),
        )),
    }
}
