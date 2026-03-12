# idempotencypostgres

`idempotencypostgres` is the durable Postgres adapter for `idempotencykit`.

It provides:
- a `Store` implementation backed by Postgres
- a `kernelkit.Module` for lifecycle and bootstrap
- automatic creation of the durable idempotency table

Defaults:
- database url from `configkit.Database.URL`
- table name `platform.idempotency_records`

Optional env:
- `IDEMPOTENCY_TABLE_NAME`
