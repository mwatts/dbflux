use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;

use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    Connection, ConnectionProfile, DatabaseCategory, DbConfig, DbDriver, DbError, DbKind,
    DdlCapabilities, DeploymentClass, DriverCapabilities, DriverFormDef, DriverLimits,
    DriverMetadata, FormFieldKind, FormSection, FormTab, FormValues, Icon, IsolationLevel,
    MutationCapabilities, PaginationStyle, PlaceholderStyle, QueryCapabilities, QueryLanguage,
    SelectOption, SyntaxInfo, TransactionCapabilities, TransferFamily, WhereOperator, field,
    field_file_path, field_password, with_default, with_help,
};

use crate::connection::TursoConnection;
use crate::error::format_turso_connection_error;
use crate::runtime::runtime;

pub static TURSO_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![
        FormTab {
            id: "main".into(),
            label: "Main".into(),
            sections: vec![FormSection {
                title: "Database".into(),
                fields: vec![
                    with_default(
                        field(
                            "mode",
                            "Mode",
                            FormFieldKind::Select {
                                options: vec![
                                    SelectOption::new("local", "Local file"),
                                    SelectOption::new("memory", "In-memory"),
                                    SelectOption::new("remote", "Remote URL"),
                                    SelectOption::new("sync", "Embedded replica (sync)"),
                                ],
                            },
                            "",
                        ),
                        "local",
                    ),
                    with_help(
                        field_file_path(),
                        "Local database file, or the replica path for sync mode. Ignored for in-memory.",
                    ),
                    with_help(
                        field(
                            "url",
                            "Remote URL",
                            FormFieldKind::Text,
                            "libsql://your-db.turso.io",
                        ),
                        "Required for remote and sync modes.",
                    ),
                    with_help(
                        {
                            let mut password = field_password();
                            password.label = "Auth Token".into();
                            password.placeholder = "Turso / libSQL auth token".into();
                            password
                        },
                        "Stored in the OS keyring. Required for remote and sync modes.",
                    ),
                ],
            }],
        },
        FormTab {
            id: "advanced".into(),
            label: "Advanced".into(),
            sections: vec![FormSection {
                title: "Experimental flags".into(),
                fields: vec![
                    with_help(
                        field(
                            "experimental_custom_types",
                            "Enable experimental custom types",
                            FormFieldKind::Checkbox,
                            "",
                        ),
                        "Passes Turso's experimental_custom_types builder flag (json, jsonb, uuid, …). Pre-1.0; not advertised as a capability.",
                    ),
                    with_help(
                        field(
                            "experimental_materialized_views",
                            "Enable experimental views",
                            FormFieldKind::Checkbox,
                            "",
                        ),
                        "Passes Turso's experimental_materialized_views builder flag. Views stay off the capability set until this surface is stable.",
                    ),
                ],
            }],
        },
    ],
});

/// Turso driver metadata. Capabilities are conservative: flags are set only
/// for behavior this crate implements against the stable SQLite frontend.
pub static METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "turso".into(),
    display_name: "Turso".into(),
    description: "Embedded SQLite-compatible engine (Turso Database 0.7)".into(),
    category: DatabaseCategory::Relational,
    transfer_family: TransferFamily::Sql,
    deployment_class: Some(DeploymentClass::Embedded),
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::INDEXES.bits()
            | DriverCapabilities::FOREIGN_KEYS.bits()
            | DriverCapabilities::CHECK_CONSTRAINTS.bits()
            | DriverCapabilities::UNIQUE_CONSTRAINTS.bits()
            | DriverCapabilities::PREPARED_STATEMENTS.bits()
            | DriverCapabilities::INSERT.bits()
            | DriverCapabilities::UPDATE.bits()
            | DriverCapabilities::DELETE.bits()
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::SORTING.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::EXPORT_CSV.bits()
            | DriverCapabilities::EXPORT_JSON.bits()
            | DriverCapabilities::TRANSACTIONAL_DDL.bits()
            | DriverCapabilities::MULTI_STATEMENT.bits()
            | DriverCapabilities::DISABLE_FK_CHECKS.bits(),
    ),
    default_port: None,
    uri_scheme: "turso".into(),
    icon: Icon::Database,
    syntax: Some(SyntaxInfo {
        identifier_quote: '"',
        string_quote: '\'',
        placeholder_style: PlaceholderStyle::QuestionMark,
        supports_schemas: false,
        default_schema: None,
        case_sensitive_identifiers: true,
    }),
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::Offset],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::Like,
            WhereOperator::Null,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        order_by_mode: dbflux_core::OrderByMode::AnyColumns,
        supports_group_by: true,
        supports_having: true,
        supports_distinct: true,
        supports_limit: true,
        supports_offset: true,
        supports_joins: true,
        supports_subqueries: true,
        supports_union: true,
        supports_intersect: true,
        supports_except: true,
        supports_case_expressions: true,
        supports_window_functions: true,
        supports_ctes: true,
        supports_explain: true,
        max_query_parameters: 32766,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: Some(MutationCapabilities {
        supports_insert: true,
        supports_update: true,
        supports_delete: true,
        supports_upsert: true,
        supports_returning: true,
        supports_batch: true,
        supports_bulk_update: true,
        supports_bulk_delete: true,
        max_insert_values: 0,
    }),
    ddl: Some(DdlCapabilities {
        supports_create_database: false,
        supports_drop_database: false,
        supports_create_table: true,
        supports_drop_table: true,
        supports_alter_table: false,
        supports_create_index: true,
        supports_drop_index: true,
        // Views require Turso's experimental builder flag and are not advertised.
        supports_create_view: false,
        supports_drop_view: false,
        supports_create_trigger: false,
        supports_drop_trigger: false,
        transactional_ddl: true,
        supports_add_column: true,
        supports_drop_column: false,
        supports_rename_column: true,
        supports_alter_column: false,
        supports_add_constraint: false,
        supports_drop_constraint: false,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: true,
        supported_isolation_levels: vec![IsolationLevel::ReadCommitted],
        default_isolation_level: Some(IsolationLevel::ReadCommitted),
        supports_savepoints: true,
        supports_nested_transactions: false,
        supports_read_only: true,
        supports_deferrable: true,
    }),
    limits: Some(DriverLimits {
        max_query_length: 1_000_000_000,
        max_parameters: 32766,
        max_result_rows: 0,
        max_connections: 0,
        max_nested_subqueries: 16,
        max_identifier_length: 100_000,
        max_columns: 32766,
        max_indexes_per_table: 64,
        max_bulk_insert_rows: 0,
    }),
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

pub struct TursoDriver;

impl TursoDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TursoDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for TursoDriver {
    fn kind(&self) -> DbKind {
        DbKind::Turso
    }

    fn metadata(&self) -> &DriverMetadata {
        &METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:turso".into()
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let settings = TursoConnectSettings::from_profile(profile)?;
        let token = password.map(|secret| secret.expose_secret().to_string());
        let inner = open_database(&settings, token.as_deref())?;
        Ok(Box::new(TursoConnection::new(inner, settings.path)?))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let settings = TursoConnectSettings::from_profile(profile)?;
        let inner = open_database(&settings, None)?;
        let connection = TursoConnection::new(inner, settings.path)?;
        connection.ping()
    }

    fn form_definition(&self) -> &DriverFormDef {
        &TURSO_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let mode = values
            .get("mode")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("local")
            .to_string();

        if !matches!(mode.as_str(), "local" | "memory" | "remote" | "sync") {
            return Err(DbError::InvalidProfile(format!(
                "Unknown Turso mode '{mode}' (expected local, memory, remote, or sync)"
            )));
        }

        let path = match mode.as_str() {
            "memory" => PathBuf::from(":memory:"),
            _ => {
                let raw = values
                    .get("path")
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty());
                match raw {
                    Some(path) => PathBuf::from(path),
                    None if mode == "local" || mode == "sync" => {
                        return Err(DbError::InvalidProfile(
                            "File path is required for local and sync modes".to_string(),
                        ));
                    }
                    None => PathBuf::new(),
                }
            }
        };

        let url = values
            .get("url")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        if matches!(mode.as_str(), "remote" | "sync") && url.is_none() {
            return Err(DbError::InvalidProfile(
                "Remote URL is required for remote and sync modes".to_string(),
            ));
        }

        Ok(DbConfig::Turso {
            mode,
            path,
            url,
            connection_id: None,
            experimental_custom_types: is_checked(values, "experimental_custom_types"),
            experimental_materialized_views: is_checked(values, "experimental_materialized_views"),
            experimental_encryption: false,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let mut values = HashMap::new();

        if let DbConfig::Turso {
            mode,
            path,
            url,
            experimental_custom_types,
            experimental_materialized_views,
            ..
        } = config
        {
            values.insert("mode".to_string(), mode.clone());
            values.insert("path".to_string(), path.to_string_lossy().to_string());
            if let Some(url) = url {
                values.insert("url".to_string(), url.clone());
            }
            if *experimental_custom_types {
                values.insert("experimental_custom_types".to_string(), "true".to_string());
            }
            if *experimental_materialized_views {
                values.insert(
                    "experimental_materialized_views".to_string(),
                    "true".to_string(),
                );
            }
        }

        values
    }
}

fn is_checked(values: &FormValues, key: &str) -> bool {
    values
        .get(key)
        .map(|value| matches!(value.trim(), "1" | "true" | "on" | "yes"))
        .unwrap_or(false)
}

pub(crate) struct TursoConnectSettings {
    pub mode: String,
    pub path: PathBuf,
    pub url: Option<String>,
    pub experimental_custom_types: bool,
    pub experimental_materialized_views: bool,
}

impl TursoConnectSettings {
    fn from_profile(profile: &ConnectionProfile) -> Result<Self, DbError> {
        match &profile.config {
            DbConfig::Turso {
                mode,
                path,
                url,
                experimental_custom_types,
                experimental_materialized_views,
                ..
            } => Ok(Self {
                mode: mode.clone(),
                path: path.clone(),
                url: url.clone(),
                experimental_custom_types: *experimental_custom_types,
                experimental_materialized_views: *experimental_materialized_views,
            }),
            _ => Err(DbError::InvalidProfile(
                "Expected Turso configuration".to_string(),
            )),
        }
    }
}

pub(crate) enum OpenedDatabase {
    Local(turso::Database),
    #[cfg(feature = "remote")]
    Remote(turso::sync::Database),
}

impl OpenedDatabase {
    pub(crate) fn connect(&self) -> Result<turso::Connection, DbError> {
        match self {
            Self::Local(database) => database
                .connect()
                .map_err(|error| format_turso_connection_error(&error)),
            #[cfg(feature = "remote")]
            Self::Remote(database) => runtime()
                .block_on(database.connect())
                .map_err(|error| format_turso_connection_error(&error)),
        }
    }
}

pub(crate) fn open_database(
    settings: &TursoConnectSettings,
    auth_token: Option<&str>,
) -> Result<OpenedDatabase, DbError> {
    match settings.mode.as_str() {
        "remote" | "sync" => open_remote(settings, auth_token),
        _ => open_local(settings),
    }
}

fn open_local(settings: &TursoConnectSettings) -> Result<OpenedDatabase, DbError> {
    let path = if settings.mode == "memory" || settings.path.as_os_str() == ":memory:" {
        ":memory:".to_string()
    } else {
        settings.path.to_string_lossy().to_string()
    };

    runtime()
        .block_on(async {
            let mut builder = turso::Builder::new_local(&path)
                // Required to load schemas that already contain `USING fts`
                // (and to create such indexes). Not advertised as a capability.
                .experimental_index_method(true);
            if settings.experimental_custom_types {
                builder = builder.experimental_custom_types(true);
            }
            if settings.experimental_materialized_views {
                builder = builder.experimental_materialized_views(true);
            }
            builder.build().await
        })
        .map(OpenedDatabase::Local)
        .map_err(|error| format_turso_connection_error(&error))
}

fn open_remote(
    settings: &TursoConnectSettings,
    auth_token: Option<&str>,
) -> Result<OpenedDatabase, DbError> {
    #[cfg(not(feature = "remote"))]
    {
        let _ = (settings, auth_token);
        return Err(DbError::NotSupported(
            "This build of the Turso driver was compiled without remote/sync support".to_string(),
        ));
    }

    #[cfg(feature = "remote")]
    {
        let url = settings.url.as_deref().ok_or_else(|| {
            DbError::InvalidProfile("Remote URL is required for remote and sync modes".to_string())
        })?;
        let token = auth_token.ok_or_else(|| {
            DbError::InvalidProfile("Auth token is required for remote and sync modes".to_string())
        })?;
        let replica_path = if settings.path.as_os_str().is_empty() {
            ":memory:".to_string()
        } else {
            settings.path.to_string_lossy().to_string()
        };

        runtime()
            .block_on(async {
                let mut builder = turso::sync::Builder::new_remote(&replica_path)
                    .with_remote_url(url)
                    .with_auth_token(token)
                    .bootstrap_if_empty(true)
                    .experimental_index_method(true);
                if settings.experimental_custom_types {
                    builder = builder.experimental_custom_types(true);
                }
                if settings.experimental_materialized_views {
                    builder = builder.experimental_materialized_views(true);
                }
                builder.build().await
            })
            .map(OpenedDatabase::Remote)
            .map_err(|error| format_turso_connection_error(&error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_identity_is_turso_sql_relational() {
        assert_eq!(METADATA.id, "turso");
        assert_eq!(METADATA.uri_scheme, "turso");
        assert_eq!(METADATA.category, DatabaseCategory::Relational);
        assert_eq!(METADATA.query_language, QueryLanguage::Sql);
        assert_eq!(TursoDriver::new().driver_key().as_str(), "builtin:turso");
    }

    #[test]
    fn capabilities_do_not_advertise_experimental_views() {
        assert!(!METADATA.capabilities.contains(DriverCapabilities::VIEWS));
        assert!(!METADATA.ddl.as_ref().expect("ddl").supports_create_view);
    }

    #[test]
    fn capabilities_do_not_advertise_query_cancellation() {
        assert!(
            !METADATA
                .capabilities
                .contains(DriverCapabilities::QUERY_CANCELLATION)
        );
    }

    #[test]
    fn form_round_trip_local_path() {
        let driver = TursoDriver::new();
        let mut values = HashMap::new();
        values.insert("mode".to_string(), "local".to_string());
        values.insert("path".to_string(), "/tmp/app.db".to_string());

        let config = driver.build_config(&values).expect("config");
        let extracted = driver.extract_values(&config);

        assert_eq!(extracted.get("mode").map(String::as_str), Some("local"));
        assert_eq!(
            extracted.get("path").map(String::as_str),
            Some("/tmp/app.db")
        );
    }

    #[test]
    fn form_memory_mode_does_not_require_path() {
        let driver = TursoDriver::new();
        let mut values = HashMap::new();
        values.insert("mode".to_string(), "memory".to_string());

        let config = driver.build_config(&values).expect("config");
        match config {
            DbConfig::Turso { mode, path, .. } => {
                assert_eq!(mode, "memory");
                assert_eq!(path.as_os_str(), ":memory:");
            }
            other => panic!("expected Turso config, got {other:?}"),
        }
    }

    #[test]
    fn form_remote_requires_url() {
        let driver = TursoDriver::new();
        let mut values = HashMap::new();
        values.insert("mode".to_string(), "remote".to_string());
        values.insert("path".to_string(), "/tmp/replica.db".to_string());

        let error = driver.build_config(&values).expect_err("url required");
        assert!(error.to_string().contains("Remote URL"));
    }

    #[test]
    fn form_rejects_unknown_mode() {
        let driver = TursoDriver::new();
        let mut values = HashMap::new();
        values.insert("mode".to_string(), "postgres".to_string());

        let error = driver.build_config(&values).expect_err("mode rejected");
        assert!(error.to_string().contains("Unknown Turso mode"));
    }

    #[test]
    fn form_includes_experimental_advanced_section() {
        let field_ids: Vec<&str> = TURSO_FORM
            .tabs
            .iter()
            .flat_map(|tab| tab.sections.iter())
            .flat_map(|section| section.fields.iter())
            .map(|field| field.id.as_str())
            .collect();

        assert!(field_ids.contains(&"mode"));
        assert!(field_ids.contains(&"path"));
        assert!(field_ids.contains(&"url"));
        assert!(field_ids.contains(&"password"));
        assert!(field_ids.contains(&"experimental_custom_types"));
        assert!(field_ids.contains(&"experimental_materialized_views"));
    }
}
