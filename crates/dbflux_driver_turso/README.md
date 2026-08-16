# dbflux_driver_turso

Built-in driver for [Turso Database](https://github.com/tursodatabase/turso),
the Rust SQLite-compatible engine. Pinned to the `turso` crate **0.7**.

Turso 0.7 is a released, out-of-beta engine (see the [0.7 announcement](https://turso.tech/blog/turso-0.7.0)). It runs in production at multiple organizations. It is not yet 1.0: some surfaces stay behind experimental flags, and Turso still recommends independent backups until they reach their SQLite reliability bar.

## Features

- Local file and in-memory databases through `turso::Builder::new_local`.
- Optional remote URL and embedded-replica (sync) modes when the crate is
  built with the `remote` feature (on by default). Auth tokens use the
  existing connection secret path.
- SQL execution, schema discovery via `sqlite_master` / `PRAGMA`, indexes,
  foreign keys, check and unique constraints.
- Multi-statement scripts are split and executed statement by statement.
- Transactional DDL, `PRAGMA foreign_keys` toggle, and SQLite-style SQL /
  code generation for CRUD, `CREATE TABLE`, and `DROP TABLE`.
- Conservative capability flags: only implemented, non-experimental
  behavior is advertised to the UI.

## Limitations

- Not yet 1.0. Compatibility with SQLite is high but not complete; see
  upstream `COMPAT.md`. Keep independent backups of durable files.
- No query cancellation interrupt yet. A cancel flag is stored, but the
  `QUERY_CANCELLATION` capability is not set.
- Views are not advertised. Turso views / live materialized views require
  an experimental builder flag; the Advanced form can pass that flag, but
  `DriverCapabilities::VIEWS` stays unset.
- Postgres dialect / `tursopg` is a separate experimental frontend and is
  not part of this driver.
- Graph / Cypher frontend is not part of this driver.
- Default access is single-process. Multi-process WAL and MVCC concurrent
  writes are experimental and are not exposed.
- Encryption at rest is not wired in the form (needs a cipher and hex
  key, not a boolean).
- Tantivy FTS (`USING fts`) is compiled in so existing databases that
  use it can open. It is not advertised as a first-class capability.
- No `TRUNCATE TABLE` (`DriverCapabilities::TRUNCATE_TABLE` is unset).
- No multi-schema namespace; the catalog is the SQLite `main` database.

## Experimental flags

The Advanced tab can pass these Turso builder flags. They do **not**
change advertised capabilities:

| Form field | Builder flag | Status |
| --- | --- | --- |
| Enable experimental custom types | `experimental_custom_types` | Experimental |
| Enable experimental views | `experimental_materialized_views` | Experimental |

Deliberately omitted: encryption, multiprocess WAL, generated columns,
`WITHOUT ROWID`, Postgres mode.

Pinned crate: `turso = "0.7"` with `default-features = false` and
`features = ["fts"]`. `mimalloc` stays off so the driver does not
install a process-wide allocator. `fts` stays on so databases that
already use `CREATE INDEX ... USING fts` can open.
