use dbflux_core::{ColumnInfo, ColumnKind, PlaceholderStyle, SqlDialect, TableInfo, Value};

pub(crate) struct TursoDialect;

pub(crate) static TURSO_DIALECT: TursoDialect = TursoDialect;

impl SqlDialect for TursoDialect {
    fn quote_identifier(&self, name: &str) -> String {
        quote_ident(name)
    }

    fn qualified_table(&self, _schema: Option<&str>, table: &str) -> String {
        quote_ident(table)
    }

    fn value_to_literal(&self, value: &Value) -> String {
        value_to_literal(value)
    }

    fn escape_string(&self, s: &str) -> String {
        escape_string(s)
    }

    fn placeholder_style(&self) -> PlaceholderStyle {
        PlaceholderStyle::QuestionMark
    }

    fn build_upsert_statement(
        &self,
        schema: Option<&str>,
        table: &str,
        assignments: &[dbflux_core::ColumnAssignment],
        conflict_columns: &[String],
        update_assignments: &[dbflux_core::ColumnAssignment],
    ) -> Option<String> {
        if assignments.is_empty() || conflict_columns.is_empty() {
            return None;
        }

        let table = self.qualified_table(schema, table);
        let columns = assignments
            .iter()
            .map(|assignment| self.quote_identifier(&assignment.name))
            .collect::<Vec<_>>()
            .join(", ");
        let values = assignments
            .iter()
            .map(|assignment| {
                self.value_to_literal_typed(&assignment.value, assignment.type_name.as_deref())
            })
            .collect::<Vec<_>>()
            .join(", ");
        let conflict_columns = conflict_columns
            .iter()
            .map(|column| self.quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");

        if update_assignments.is_empty() {
            return Some(format!(
                "INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_columns}) DO NOTHING"
            ));
        }

        let update_clause = update_assignments
            .iter()
            .map(|assignment| {
                format!(
                    "{} = {}",
                    self.quote_identifier(&assignment.name),
                    self.value_to_literal_typed(&assignment.value, assignment.type_name.as_deref())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_clause}"
        ))
    }
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub(crate) fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

pub(crate) fn value_to_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(flag) => if *flag { "1" } else { "0" }.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => {
            if value.is_nan() || value.is_infinite() {
                "NULL".to_string()
            } else {
                value.to_string()
            }
        }
        Value::Decimal(s) => format!("'{}'", escape_string(s)),
        Value::Text(s) => format!("'{}'", escape_string(s)),
        Value::Json(s) => format!("'{}'", escape_string(s)),
        Value::Bytes(bytes) => format!("X'{}'", hex::encode(bytes)),
        Value::DateTime(dt) => format!("'{}'", dt.to_rfc3339()),
        Value::Date(date) => format!("'{}'", date.format("%Y-%m-%d")),
        Value::Time(time) => format!("'{}'", time.format("%H:%M:%S%.f")),
        Value::ObjectId(id) => format!("'{}'", escape_string(id)),
        Value::Unsupported(_) => "NULL".to_string(),
        Value::Array(arr) => {
            let json = serde_json::to_string(arr).unwrap_or_else(|_| "[]".to_string());
            format!("'{}'", escape_string(&json))
        }
        Value::Document(doc) => {
            let json = serde_json::to_string(doc).unwrap_or_else(|_| "{}".to_string());
            format!("'{}'", escape_string(&json))
        }
    }
}

pub(crate) fn generate_create_table(table: &TableInfo) -> String {
    let mut sql = format!("CREATE TABLE {} (\n", quote_ident(&table.name));
    let cols = table.columns.as_deref().unwrap_or(&[]);
    let pk_columns: Vec<&ColumnInfo> = cols.iter().filter(|col| col.is_primary_key).collect();

    #[allow(clippy::indexing_slicing)]
    let single_integer_pk =
        pk_columns.len() == 1 && pk_columns[0].type_name.eq_ignore_ascii_case("INTEGER");

    for (index, col) in cols.iter().enumerate() {
        let mut line = if col.type_name.is_empty() {
            format!("    {}", quote_ident(&col.name))
        } else {
            format!("    {} {}", quote_ident(&col.name), col.type_name)
        };

        if !col.nullable {
            line.push_str(" NOT NULL");
        }

        if single_integer_pk && col.is_primary_key {
            line.push_str(" PRIMARY KEY");
        }

        if let Some(ref default) = col.default_value {
            line.push_str(&format!(" DEFAULT {default}"));
        }

        let is_last_column = index == cols.len() - 1;
        let needs_pk_constraint = !pk_columns.is_empty() && !single_integer_pk;
        if !is_last_column || needs_pk_constraint {
            line.push(',');
        }

        sql.push_str(&line);
        sql.push('\n');
    }

    if !pk_columns.is_empty() && !single_integer_pk {
        let pk_quoted: Vec<String> = pk_columns
            .iter()
            .map(|col| quote_ident(&col.name))
            .collect();
        sql.push_str(&format!("    PRIMARY KEY ({})\n", pk_quoted.join(", ")));
    }

    sql.push_str(");");
    sql
}

pub(crate) fn translate_filter_to_sql(filter: &Value) -> String {
    match filter {
        Value::Document(doc) => {
            let mut parts = Vec::new();
            for (key, value) in doc {
                let quoted_col = quote_ident(key);
                let expr = match value {
                    Value::Null => format!("{quoted_col} IS NULL"),
                    Value::Text(s) => format!("{quoted_col} = '{}'", escape_string(s)),
                    Value::Int(i) => format!("{quoted_col} = {i}"),
                    Value::Bool(flag) => {
                        format!("{quoted_col} = {}", if *flag { "1" } else { "0" })
                    }
                    Value::Float(f) => format!("{quoted_col} = {f}"),
                    _ => format!("{quoted_col} = {}", value_to_literal(value)),
                };
                parts.push(expr);
            }
            if parts.is_empty() {
                String::new()
            } else {
                parts.join(" AND ")
            }
        }
        Value::Text(s) => s.clone(),
        _ => String::new(),
    }
}

pub(crate) fn collect_filter_values(filter: &Value, params: &mut Vec<Value>) {
    if let Value::Document(doc) = filter {
        for value in doc.values() {
            if !matches!(value, Value::Null) {
                params.push(value.clone());
            }
        }
    }
}

pub(crate) fn kind_from_decltype(decl: Option<&str>) -> ColumnKind {
    let decl = match decl {
        Some(value) if !value.is_empty() => value.to_uppercase(),
        _ => return ColumnKind::Unknown,
    };

    if decl.contains("INT") {
        return ColumnKind::Integer;
    }

    if decl.contains("REAL")
        || decl.contains("FLOA")
        || decl.contains("DOUB")
        || decl.contains("NUMERIC")
        || decl.contains("DECIMAL")
    {
        return ColumnKind::Float;
    }

    if decl.contains("DATE") || decl.contains("TIME") || decl.contains("STAMP") {
        return ColumnKind::Timestamp;
    }

    if decl.contains("CHAR") || decl.contains("TEXT") || decl.contains("CLOB") {
        return ColumnKind::Text;
    }

    ColumnKind::Unknown
}

pub(crate) fn turso_value_to_value(value: turso::Value) -> Value {
    if let Some(integer) = value.as_integer() {
        return Value::Int(*integer);
    }
    if let Some(real) = value.as_real() {
        return Value::Float(*real);
    }
    if let Some(text) = value.as_text() {
        return Value::Text(text.clone());
    }
    if let Some(blob) = value.as_blob() {
        return Value::Bytes(blob.clone());
    }
    Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_from_decltype_integer_variants() {
        assert_eq!(kind_from_decltype(Some("INTEGER")), ColumnKind::Integer);
        assert_eq!(kind_from_decltype(Some("INT")), ColumnKind::Integer);
        assert_eq!(kind_from_decltype(Some("BIGINT")), ColumnKind::Integer);
    }

    #[test]
    fn kind_from_decltype_float_variants() {
        assert_eq!(kind_from_decltype(Some("REAL")), ColumnKind::Float);
        assert_eq!(kind_from_decltype(Some("FLOAT")), ColumnKind::Float);
        assert_eq!(kind_from_decltype(Some("DOUBLE")), ColumnKind::Float);
    }

    #[test]
    fn kind_from_decltype_timestamp_variants() {
        assert_eq!(kind_from_decltype(Some("DATETIME")), ColumnKind::Timestamp);
        assert_eq!(kind_from_decltype(Some("TIMESTAMP")), ColumnKind::Timestamp);
    }

    #[test]
    fn kind_from_decltype_text_variants() {
        assert_eq!(kind_from_decltype(Some("TEXT")), ColumnKind::Text);
        assert_eq!(kind_from_decltype(Some("VARCHAR")), ColumnKind::Text);
    }

    #[test]
    fn kind_from_decltype_unknown_cases() {
        assert_eq!(kind_from_decltype(None), ColumnKind::Unknown);
        assert_eq!(kind_from_decltype(Some("")), ColumnKind::Unknown);
        assert_eq!(kind_from_decltype(Some("BLOB")), ColumnKind::Unknown);
    }

    #[test]
    fn quote_ident_escapes_double_quotes() {
        assert_eq!(quote_ident("name"), "\"name\"");
        assert_eq!(quote_ident("na\"me"), "\"na\"\"me\"");
    }
}
