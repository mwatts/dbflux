#![allow(clippy::result_large_err)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing,
    )
)]

mod connection;
mod dialect;
mod driver;
mod error;
mod runtime;

pub use driver::{METADATA, TURSO_FORM, TursoDriver};
