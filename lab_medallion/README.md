# Lab: Bronze to Silver transformation with dbt and DuckDB

Johan LIN, ING4 DATA & AI, GRP03

Lab: [05.etl-elt-and-medallion-architecture/lab-dbt.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/05.etl-elt-and-medallion-architecture/lab-dbt.md)
Platform: Onyxia `vscode-pyspark`, dbt Core 1.12.6, dbt-duckdb 1.11.0, S3 backend SeaweedFS, bucket `user-j-lin-ece`.

## Project layout

```text


lab_medallion/
├── dbt_project.yml                 silver models: views in the main_silver schema
├── profiles.yml                    DuckDB connection (no credential)
├── models/
│   ├── bronze/sources.yml          S3 CSV files declared as sources (read_csv via external_location)
│   └── silver/
│       ├── stg_users.sql           typed users, address split, normalized username, validated birthdate
│       ├── stg_orders.sql          typed and deduplicated orders
│       └── silver.yml              documentation and generic tests
├── seeds/
│   ├── states.csv                  US states, DC, territories and military codes (62 codes)
│   └── zip_prefixes.csv            USPS 3-digit ZIP prefixes per state (exercise 4)
└── tests/
    ├── generic/test_positive.sql   custom generic test (exercise 2)
    └── assert_*.sql                singular tests
```

Run:

```bash
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
cd lab_medallion
uv run dbt build
uv run dbt docs generate --static
```

## Issues met during the lab

- **Expired S3 credentials**: the Onyxia credentials last 12 h. The AWS profile was recreated and the persistent DuckDB secret `s3_onyxia_connection` was recreated from it.
- **Missing `states` model**: the `relationships` test on `stg_users.state` references `ref('states')`, which the lab never creates. It was added as a seed (50 states, DC, 8 territories, AA/AE/AP military codes).
- **Seeds ignored by Git**: the `*.csv` rule of the root `.gitignore` (added for the generated datasets) also ignored the seeds. Exception added: `!lab_medallion/seeds/*.csv`.
- **`dbt build --select silver` does not build the seeds**: after removing the database, the `relationships` test to `states` would fail. `dbt build` without selection builds the seeds, models and tests in the order of the graph.

## Results

| Step | Result |
|---|---|
| Bronze sources | `dbt show` reads the CSV files directly on S3 |
| `dbt run --select silver` | 2 views in `main_silver`: `stg_orders`, `stg_users` |
| Address split | `blairamanda`: military address `DPO AP 09617`, city `NULL`, state `AP` |
| Tests before the exercises | 15 tests, `assert_orders_after_user_birth` FAIL 288, then WARN 288 with `severity='warn'` |
| Full pipeline (`dbt build`) | `PASS=17 WARN=1 ERROR=0` (1 seed, 2 views, 15 tests) |
| Parquet export | `silver/dbt/users.parquet` (30 F, 20 M), `silver/dbt/orders.parquet`: 2829 rows, `EXCEPT` in both directions returns 0 row, types preserved (`UUID`, `TIMESTAMPTZ`, `INTEGER`) |
| Final build | `PASS=27 WARN=1 ERROR=0 TOTAL=28` (2 seeds, 2 models, 24 tests) |

## Exercises

1. **`username_normalized`**: `lower(trim(username))`, documented, tested with `unique` and `not_null`.
2. **Order validation**: custom generic test `positive` (`tests/generic/test_positive.sql`) and `not_null` on `quantity`; `accepted_values` and `not_null` on `product`; `relationships` to `stg_users` on `user_id`, completed by the singular test `assert_orders_have_users`.
3. **Invalid birthdates**: `stg_users` references `stg_orders` to compute the first order date of each user. A birthdate later than the first order is replaced with `NULL` and flagged with `birthdate_is_valid = false` (5 users, 288 orders). `assert_orders_after_user_birth` is back to the default `error` severity and passes; `assert_invalid_birthdates_are_null` checks the consistency of both columns.
4. **Invalid ZIP codes**: seed `zip_prefixes.csv` (USPS 3-digit prefixes per state, simplified: a few local exceptions such as `569` for DC are not covered) and singular test `assert_zip_code_matches_state` in `warn`: **49 of 50** users have a ZIP code inconsistent with their state, e.g. `garzaanthony` `KY 01352`, prefix `013` belongs to `MA`.
5. **Materialization**:

| | Views | Tables |
|---|---|---|
| Object type | `VIEW` | `BASE TABLE` |
| `lab_medallion.duckdb` size | 1.3 MB | 1.8 MB |
| S3 read per query | 323.7 KiB, 1 GET | none |
| Query time (`sum(quantity)` per product) | 0.329 s | 0.0047 s |

## Questions

**Bronze files left untouched: benefit when a bug is found six months later?**
The model is fixed and the silver layer is rebuilt from the original raw data with `dbt build`: nothing is lost and the transformation is reproducible. The raw data is also an audit trail. A transformation overwriting the raw files would make the error irreversible.

**Format and layout for orders ingested every hour for years?**
Parquet (columnar, typed, compressed, statistics per row group), partitioned by date with the Hive convention (`orders/date=YYYY-MM-DD/`), with a regular compaction of the small hourly files, ideally in a table format such as Iceberg or Delta. The source reads `read_parquet('.../orders/*/*.parquet', hive_partitioning = true)`, date filters only read the relevant partitions, and incremental models only process the new ones.

**Views: what happens when a view is queried, and when is it preferable to a table?**
A view only stores the query: each `SELECT` reads the CSV on S3 again, parses it and applies the transformations. The data is always fresh and uses no storage, but the computation is paid at every read. It suits light transformations, small data, freshness requirements or rare reads (staging). A table suits models queried often or expensive to compute.

**`UUID` stored in 16 bytes: size of the `VARCHAR` representation?**
36 bytes (32 hexadecimal characters and 4 hyphens), 2.25 times more, plus the string header.

**Invalid birthdates: strategy for a customer age dashboard, and for sales per region?**
Age: replace the invalid birthdates with `NULL` and flag them (exercise 3), the customers stay counted with an unknown age. Sales per region: the birthdate is not used, all the orders are kept and the issue is monitored with a `warn` test; rejecting the 288 orders would underestimate the sales.

**Random state/ZIP combinations: which test, which reference data?**
A singular test joining the ZIP code prefix to a reference table of the USPS 3-digit prefixes per state, stored as a seed (exercise 4).

**Exercise 4: origin, seed, layer, changes of the reference data?**
The reference data comes from an official and dated source (USPS prefix table, or Census Bureau ZCTA for full ZIP codes). A small and stable table (71 rows) belongs in a seed, versioned with the code; a large or frequently updated one is ingested as a source in the bronze layer. The validation happens in the silver layer, as a flag or a `warn` test, without deleting the rows. When the reference changes, the seed is updated (one commit) and `dbt build` recomputes every model from bronze; a dbt snapshot keeps its history when the reference valid at a given date matters.

**Exercise 5: views vs. tables**
- Storage: a view stores only the query, the data stays in the CSV files on S3; a table copies the data into `lab_medallion.duckdb` (1.3 MB to 1.8 MB).
- Execution: a view runs the transformation at every query; a table runs it once, during `dbt run` / `dbt build`.
- When the CSV changes: a view reflects it at the next query; a table stays stale until the next `dbt build`.
- More storage: the table.
- No re-read of the source files: the table (no HTTP request, 0.0047 s instead of 323.7 KiB downloaded and 0.329 s).