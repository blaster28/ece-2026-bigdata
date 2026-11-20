# Lab 10 part 2: Silver to gold with Trino

Johan LIN, ING4 DATA & AI, GRP03

Lab: [10.trino/lab-2-trino-gold.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-2-trino-gold.md)
Prerequisites: Trino deployed in part 1 (`../README.md`), catalog `lakehouse` connected to Polaris as `dev`.
Client: `trino` and `pandas` added to the uv project of the repository (`pyproject.toml`, `uv.lock`).

## How to run

```bash
cd 10-trino/lab-2
export TRINO_HOST=trino TRINO_PORT=8080 TRINO_USER=$KUBERNETES_NAMESPACE
set -a; . ~/work/.polaris-credentials.env; set +a   # outside Git
bash 00_register_users.sh                            # only if silver.users is missing in Polaris
uv run python
```

In the Python shell: `exec(open("00_connect.py").read())`, then the other files in order. The shell is driven with
`exec()` on files, since the Python 3.13 REPL mangles long multi-line pastes.

| File | Step |
|---|---|
| `00_register_users.sh` | registers again `silver.users` (lab 07, `v1.metadata.json`) through the Iceberg REST API |
| `00_connect.py` | `trino.dbapi.connect()` and the `sql()` helper |
| `01_catalogs.py` | catalogs, schemas, tables, types |
| `02_silver_queries.py` | counts, address check, orphan orders |
| `03_metadata.py` | `$snapshots`, `$files`, `$history`, `$manifests` |
| `04_plan.py` | `EXPLAIN (TYPE DISTRIBUTED)`, `EXPLAIN ANALYZE`, `system.runtime.queries` |
| `05_gold.py` | schema `gold` and the 3 gold tables (CTAS) |
| `06_validation.py` | asserts gold vs silver |
| `07_refresh.py` | `DELETE` + `INSERT`, snapshots, time travel |

`silver.users` had been dropped from Polaris by the cleanup of lab 09 part 2; its files were still in the bucket,
so it was registered again with `POST /api/catalog/v1/<catalog>/namespaces/silver/register` as `dev`.

## Results

| Step | Result |
|---|---|
| Version | Trino 480 |
| Catalogs | `lakehouse`, `system`, `tpcds`, `tpch` |
| Schemas of `lakehouse` | `information_schema`, `silver`, `system`, then `gold` |
| Types | `order_id varchar`, `ordered_at timestamp(6) with time zone`, `quantity integer`, `birthdate date` |
| Counts | 2829 orders, 50 users, 0 orphan order |
| Missing cities | `AE` 3, `AP` 2 in the top 10 (plus `AA` 1): the 6 military addresses of lab 07 |
| `orders$snapshots` | 2 snapshots written by Spark: `overwrite`, `append` (the append is no longer a current ancestor after the rollback) |
| `orders$files` | 1 file, 2829 rows, 84,055 B; the bucket holds 2 data files |
| Plan | 3 fragments, `ScanFilter` on orders with a dynamic filter on `user_id`, `REPLICATED` join (users broadcast) |
| `EXPLAIN ANALYZE` | planning 0.45 s, execution 2.9 s, 2879 input rows (2829 + 50), 37 output rows, peak 128 kB |
| `orders_by_state_month` | 50 rows |
| `top_products` | 6 rows: croissant 1523 units, drink 1493, brioche 1424, bread 1420, donut 1390, cookie 1365 |
| `user_activity` | 50 rows, 0 user without order |
| Validation | `units: 8615`, `users: 50`, `orders: 2829`, all asserts pass |
| Refresh | 3 snapshots: `append` (CTAS), `delete`, `append`; time travel: 6, 0, 6 products |

## Answers

- **`catalog=` of `connect()`**: it sets the default catalog of the session. `SHOW SCHEMAS` without `FROM` lists
  the schemas of `lakehouse`, and a table can be named `silver.orders` instead of `lakehouse.silver.orders`.
- **Types**: `ordered_at` is `timestamp(6) with time zone`. Iceberg stores an instant in UTC (`timestamptz`), not a
  zone per row: Trino displays it with `+00:00`. `order_id` stays `varchar`, as written by Spark; Trino does not
  turn it into `uuid` (dbt had a `uuid` column).
- **Output of the silver queries**: same counts as Spark (2829 orders, 50 users); the users without city are the
  military addresses (`AA`, `AE`, `AP`), as found in lab 07; no order points to an unknown user.
- **dbt test**: the orphan query is the `relationships` test (`orders.user_id` to `users.user_id`). To run it on
  every refresh: a dbt model and test with the `dbt-trino` adapter, or an `assert` in the refresh script that stops
  before the gold tables are rebuilt.
- **No job, no session**: Trino is a long-running service, a query is an HTTP request answered in seconds. It fits
  interactive and BI queries; Spark fits heavy batch jobs and transformations written in Python.
- **Snapshots of `silver.orders`**: 2, both written by Spark in lab 07. The rollback made the first one current again.
- **`aws s3 ls` vs `$files`**: `aws s3 ls` lists every object (2 Parquet files, including the one of the rolled back
  append) and needs the S3 keys of the user. `$files` only lists the files of the current snapshot, read from the
  manifests: the user only needs Trino; Trino gets the table from Polaris and reads the manifests with its own keys.
- **Plan**: fragments 1 and 2 are `SOURCE` stages (one `TableScan` per table), fragment 0 merges the partial
  aggregates. `RemoteSource` nodes are the `REMOTE` exchanges between fragments. The join is `REPLICATED`: users
  (50 rows) is broadcast to the task reading orders, and its keys feed a dynamic filter on the scan of orders.
- **`LEFT JOIN` vs inner join**: `user_activity` must keep every user, even without order. `orders_by_state_month`
  starts from the orders and drops those whose user is unknown: it depends on the orphan query (0 here).
- **`current_date`**: `age` is only right on the day of the refresh. The table must be rebuilt every day, or the age
  computed at read time (view) from `birthdate`. Some ages look wrong (9 years old with 100 orders): they come from
  the invalid birthdates found in lab 07.
- **Location of the gold files**: `s3://user-j-lin-ece/warehouse/gold/top_products-<uuid>/`. The namespace location
  comes from Polaris (base location of the catalog + `gold`); the random suffix comes from Trino, whose Iceberg
  connector creates unique table locations by default (`iceberg.unique-table-location`).
- **Refresh**: 3 snapshots, `append` (CTAS), `delete`, `append`. Between the `DELETE` and the `INSERT`, a dashboard
  reads the second snapshot: an empty table (0 products, checked with time travel). A `MERGE`, or a
  `CREATE OR REPLACE TABLE ... AS`, commits the new content in one snapshot: readers see the old or the new data,
  never an empty table.

## Trino vs Spark

| | Trino | Spark |
|---|---|---|
| Client side | `pip` package `trino`, a connection (host, port, user) | `SparkSession`, `iceberg-spark-runtime` package, Iceberg extensions, catalog settings |
| Credentials | held by the server (Polaris `dev` credential and S3 keys in Kubernetes secrets) | held by the client (Polaris credential and S3 keys in the session) |
| Execution | query sent to a running cluster | job run by the driver of the session |

- Dashboard refreshed every minute on the gold layer: **Trino** (always on, low latency).
- Nightly transformation of 10 TB of bronze files: **Spark** (scales out, retries failed tasks).
- Data scientist exploring the silver layer: **Trino** for SQL exploration; Spark if the work goes on in Python (ML).
- **Fault tolerance**: without fault-tolerant execution, losing a worker fails the whole query. The queries of this
  lab last seconds and are simply run again; a 10 TB job would lose hours of work.

Note on governance: Trino has no authentication here and always talks to Polaris as `dev`. Every Trino user gets the
rights of `dev` in Polaris: the per-principal RBAC of module 09 only holds for clients that use their own credential.
