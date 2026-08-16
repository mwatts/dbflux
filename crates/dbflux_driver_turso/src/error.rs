use dbflux_core::{DbError, FormattedError, QueryErrorFormatter};

pub(crate) struct TursoErrorFormatter;

impl TursoErrorFormatter {
    pub(crate) fn format_turso_error(error: &turso::Error) -> FormattedError {
        match error {
            turso::Error::Busy(message) | turso::Error::BusySnapshot(message) => {
                FormattedError::new(message.clone()).with_code("BUSY")
            }
            turso::Error::Constraint(message) => {
                FormattedError::new(message.clone()).with_code("CONSTRAINT")
            }
            turso::Error::Corrupt(message) => {
                FormattedError::new(message.clone()).with_code("CORRUPT")
            }
            turso::Error::Misuse(message) => {
                FormattedError::new(message.clone()).with_code("MISUSE")
            }
            turso::Error::Interrupt(message) => {
                FormattedError::new(message.clone()).with_code("INTERRUPT")
            }
            turso::Error::IoError(kind, op) => {
                FormattedError::new(format!("I/O error during {op}: {kind:?}")).with_code("IO")
            }
            other => FormattedError::new(other.to_string()),
        }
    }
}

impl QueryErrorFormatter for TursoErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        if let Some(turso_error) = error.downcast_ref::<turso::Error>() {
            Self::format_turso_error(turso_error)
        } else {
            FormattedError::new(error.to_string())
        }
    }
}

pub(crate) fn format_turso_query_error(error: &turso::Error) -> DbError {
    if matches!(error, turso::Error::Interrupt(_)) {
        return DbError::Cancelled;
    }
    let formatted = TursoErrorFormatter::format_turso_error(error);
    log::error!("Turso query failed: {}", formatted.to_display_string());
    formatted.into_query_error()
}

pub(crate) fn format_turso_connection_error(error: &turso::Error) -> DbError {
    let formatted = TursoErrorFormatter::format_turso_error(error);
    DbError::connection_failed(formatted.to_display_string())
}
