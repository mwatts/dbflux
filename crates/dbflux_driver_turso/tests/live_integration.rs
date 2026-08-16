#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::result_large_err,
    clippy::unwrap_in_result
)]

use dbflux_core::{
    ConnectionProfile, DbConfig, DbDriver, DbError, QueryRequest, SchemaLoadingStrategy, Value,
};
use dbflux_driver_turso::TursoDriver;

fn connect_memory() -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let driver = TursoDriver::new();
    let profile = ConnectionProfile::new(
        "live-turso",
        DbConfig::Turso {
            mode: "memory".to_string(),
            path: ":memory:".into(),
            url: None,
            connection_id: None,
            experimental_custom_types: false,
            experimental_materialized_views: false,
            experimental_encryption: false,
        },
    );
    let connection = driver.connect(&profile)?;
    connection.ping()?;
    Ok(connection)
}

fn connect_file() -> Result<Box<dyn dbflux_core::Connection>, DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test.db");
    let driver = TursoDriver::new();
    let profile = ConnectionProfile::new(
        "live-turso-file",
        DbConfig::Turso {
            mode: "local".to_string(),
            path: db_path,
            url: None,
            connection_id: None,
            experimental_custom_types: false,
            experimental_materialized_views: false,
            experimental_encryption: false,
        },
    );
    let connection = driver.connect(&profile)?;
    connection.ping()?;
    std::mem::forget(temp_dir);
    Ok(connection)
}

#[test]
fn turso_memory_connect_ping_query_and_schema() -> Result<(), DbError> {
    let connection = connect_memory()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO users (name) VALUES ('alice')",
    ))?;

    let result = connection.execute(&QueryRequest::new("SELECT id, name FROM users"))?;
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        connection.schema_loading_strategy(),
        SchemaLoadingStrategy::SingleDatabase
    );

    let schema = connection.schema()?;
    assert!(schema.is_relational());
    Ok(())
}

#[test]
fn turso_file_connect_and_round_trip() -> Result<(), DbError> {
    let connection = connect_file()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)",
    ))?;
    connection.execute(&QueryRequest::new(
        "INSERT INTO items (label) VALUES ('alpha')",
    ))?;
    let result = connection.execute(&QueryRequest::new("SELECT label FROM items"))?;
    assert_eq!(result.rows.len(), 1);
    match result.rows[0][0] {
        Value::Text(ref text) => assert_eq!(text, "alpha"),
        ref other => panic!("expected text, got {other:?}"),
    }
    Ok(())
}

#[test]
fn turso_schema_introspection() -> Result<(), DbError> {
    let connection = connect_memory()?;

    connection.execute(&QueryRequest::new(
        "CREATE TABLE test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER DEFAULT 0
        )",
    ))?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE test_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES test_users(id),
            amount REAL NOT NULL
        )",
    ))?;
    connection.execute(&QueryRequest::new(
        "CREATE INDEX idx_orders_user_id ON test_orders(user_id)",
    ))?;

    let table = connection.table_details("main", None, "test_users")?;
    let columns = table.columns.as_ref().expect("columns");
    assert!(columns.len() >= 4);
    let id_col = columns.iter().find(|col| col.name == "id").expect("id");
    assert!(id_col.is_primary_key);
    let name_col = columns.iter().find(|col| col.name == "name").expect("name");
    assert!(!name_col.nullable);
    Ok(())
}

#[test]
fn turso_multi_statement_script() -> Result<(), DbError> {
    let connection = connect_memory()?;
    connection.execute(&QueryRequest::new(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER);
         INSERT INTO t (n) VALUES (1);
         INSERT INTO t (n) VALUES (2);",
    ))?;
    let result = connection.execute(&QueryRequest::new("SELECT n FROM t ORDER BY n"))?;
    assert_eq!(result.rows.len(), 2);
    Ok(())
}

#[test]
fn turso_reopen_database_with_fts_index() -> Result<(), DbError> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("fts.db");
    let driver = TursoDriver::new();
    let profile = ConnectionProfile::new(
        "live-turso-fts",
        DbConfig::Turso {
            mode: "local".to_string(),
            path: db_path,
            url: None,
            connection_id: None,
            experimental_custom_types: false,
            experimental_materialized_views: false,
            experimental_encryption: false,
        },
    );

    {
        let connection = driver.connect(&profile)?;
        connection.execute(&QueryRequest::new(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)",
        ))?;
        connection.execute(&QueryRequest::new(
            "CREATE INDEX docs_fts ON docs USING fts(body)",
        ))?;
    }

    let connection = driver.connect(&profile)?;
    connection.ping()?;
    let result = connection.execute(&QueryRequest::new("SELECT name FROM sqlite_master"))?;
    assert!(!result.rows.is_empty());
    std::mem::forget(temp_dir);
    Ok(())
}

#[test]
fn turso_wrong_config_is_rejected() {
    let driver = TursoDriver::new();
    let profile = ConnectionProfile::new("wrong", DbConfig::default_sqlite());
    match driver.connect(&profile) {
        Ok(_) => panic!("wrong config should be rejected"),
        Err(error) => assert!(error.to_string().contains("Expected Turso")),
    }
}
