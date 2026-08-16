use std::sync::LazyLock;

/// Process-wide tokio runtime shared across every Turso call.
///
/// Turso's public API is async. DBFlux's `Connection` trait is synchronous,
/// so the driver bridges with `block_on`. A single `LazyLock<Runtime>` lives
/// for the process lifetime — constructing a runtime per call is expensive
/// and can panic if invoked from an existing async context.
#[allow(clippy::expect_used)]
static TURSO_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Runtime::new().expect("Turso driver failed to construct tokio runtime")
});

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    &TURSO_RUNTIME
}
