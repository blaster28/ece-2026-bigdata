# Lab: SQL analytics with DuckDB

Johan LIN, ING4 DATA & AI, GRP03

Lab: [04.sql-analytics/lab-duckdb.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/04.sql-analytics/lab-duckdb.md)
Platform: Onyxia `vscode-pyspark`, DuckDB CLI v1.5.5, Python `duckdb` 1.5.6, S3 backend SeaweedFS, bucket `user-j-lin-ece`.

## Files

- `analytics.sql`: window functions and pivot queries of the lab
- `exercises.sql`: answers to the 5 exercises
- `parquet.sql`: Parquet export, footer metadata, Hive partitioning, partition pruning
- `scale.sql`: CSV to Parquet conversion of the large dataset
- `../src/bigdata_ece_2026/orders_report.py`: `orders-report` command (DuckDB Python API)

`init.sql` (bucket name), `analytics.duckdb`, CSV and Parquet files are excluded by `.gitignore`.

## Environment issue: expired S3 credentials

The Onyxia credentials are temporary (STS, 12 h). Both the AWS profile and the persistent DuckDB secret `s3_onyxia_connection` (created at service startup) had expired: `InvalidAccessKeyId` / HTTP 403. The AWS profile was refreshed from the Onyxia "storage" page, then the DuckDB secret was recreated from it:

```sql
CREATE OR REPLACE PERSISTENT SECRET s3_onyxia_connection (
  TYPE s3, KEY_ID '...', SECRET '...', SESSION_TOKEN '...',
  REGION 'us-east-1', ENDPOINT 's3.seaweedfs.adm.adaltas.cloud',
  URL_STYLE 'path', USE_SSL true);
```

## Results

| Section | Result |
|---|---|
| Bronze layer | `sniff_csv`: `,` / `"` / header. `date` inferred as `TIMESTAMP WITH TIME ZONE`, `quantity` as `BIGINT` |
| Tables | 50 users, 2829 orders, from 2020-01-01 to 2020-04-27 |
| Quality checks | 0 orphan order, 0 inactive user, 0 duplicated order id |
| Aggregations | croissant first (1523, 17.7 %); 744 / 696 / 744 / 645 orders per month (one order per hour) |
| Top 5 customers | 5th position is a tie at 285 (`wrightjames` vs `stephen00` in the lab): `ORDER BY quantity DESC LIMIT 5` is not deterministic for ties, a tie-breaker column is needed |
| Window / pivot | Same results as the lab |

## Exercises

| # | Question | Result |
|---|---|---|
| 1 | Average orders per user / quantity per order | 56.58 / 3.05 |
| 2 | First and last order per user | Each user orders over 0 to 5 days only: the generator assigns blocks of consecutive hourly orders to the same user (not realistic, to be handled in the silver layer) |
| 3 | Hour with the highest quantity | 06:00 UTC, 118 orders, quantity 385 |
| 4 | Month-over-month variation per product (`lag`) | 24 rows, `NULL` for January; e.g. bread +14.9 %, +13.5 %, -19.2 %; largest drop donut February -28.3 % |
| 5 | Users who ordered every product | 45 of 50; the 5 others have only 2 to 12 orders |

Note on exercise 2: `date_diff('day', a, b)` counts the date boundaries crossed, not 24 h periods (`blairamanda`: 15 h counted as 1 day). An exact duration uses `max(date) - min(date)` (`INTERVAL`).

## Parquet

| Measure | Result |
|---|---|
| `orders.parquet` vs `orders.csv` | 130 526 B vs 331 568 B (2.5x smaller) |
| `uuid` column | 102 191 B compressed for 113 189 B uncompressed |
| `user_uuid`, `product` | about 2 KB each (dictionary encoding) |
| Hive partitioning by product | 6 files, about 24 KB each, 147 KB in total (more than the single file: one footer and dictionary per file) |
| `WHERE product = 'cookie'` | `Scanning Files: 1/6`, 466 rows, 3 GET, 33.7 KiB |

## CSV vs. Parquet at scale

Generated dataset: 252 416 orders, 5000 users, dates from 2020 to 2048 (the lab text says about 250 000 orders, its sample output shows 999 490).

| Measure | Result |
|---|---|
| Size CSV / Parquet | 29.6 MB / 11.5 MB (2.6x smaller) |
| CSV query (`product`, `sum(quantity)`) | 28.2 MiB received, 1 GET, 1.26 s |
| Same query on Parquet | 203.5 KiB, 4 GET, 0.31 s |
| Parquet `date >= '2100-01-01'` | 16 KiB, 1 GET (footer only) |
| Parquet `date >= '2040-01-01'` | 1022 KiB, 3 GET (row group 0 skipped) |
| Same CSV query on the local file | 0.23 s (parsing only) |
| Row groups | 3: 123 573 + 124 276 + 4 567 rows |

## Questions

**Persistent secret stored in the home directory: risks, and why they are limited on Onyxia?**
The secret is stored in plain text in `~/.duckdb/stored_secrets`: any process running as the user can read it, and it can leak through a backup, a copy or a commit. On Onyxia the credentials are temporary (12 h), scoped to the `S3UserRole` (in practice the user's bucket), and the home volume is private to the user's pod and namespace. The expiration was observed during the lab.

**How to restrict the S3 access of a Kubernetes Job?**
Do not mount the home directory nor use a persistent secret. Give the Job a dedicated ServiceAccount with a workload identity (short-lived STS credentials) and a least-privilege policy (for example read-only on `bronze/`). If a Kubernetes Secret is used, mount it only in this pod and restrict it with RBAC. In DuckDB, use a temporary (in-memory) `CREATE SECRET`.

**Why does `approx_unique` return 40 and 3167?**
It is an estimate computed with HyperLogLog: constant memory and fast, but with an error margin. `count(DISTINCT ...)` gives the exact value.

**Type inference on a sample of a large file?**
If the first rows are not representative (empty values, integers then decimals or text, different date formats), the inferred type is wrong and the read fails or values are misinterpreted. Solutions: `sample_size = -1`, explicit `columns`/`types`, or `all_varchar = true` and casting in the silver layer.

**Why is the `uuid` column barely compressed?**
Random UUIDs are all distinct with a high entropy: no repetition for the dictionary encoding nor for Snappy. Only the restricted hexadecimal alphabet gives a small gain. A binary `UUID` type (16 bytes) would halve its size.

**Why is the compressed size of some columns larger than their uncompressed size?**
`product` is already dictionary encoded (bit-packed indices) and `date` contains distinct 64-bit integers: Snappy finds nothing to compress but adds its own framing, a few bytes larger.

**Why is partitioning by `uuid` a bad idea?**
One file per order: thousands then millions of tiny files. On object storage every file costs at least one HTTP request with its latency, LIST becomes huge, there is no atomic rename, and every file carries its own footer (small files problem). A partition column must have a low or moderate cardinality and match the common filters.

**Partition column for orders growing every day?**
The order date (`order_date=YYYY-MM-DD`, or `year=/month=` for small daily volumes): queries filter on time ranges, and each new day is written to a new partition without rewriting the previous ones. The granularity is chosen to keep files of a reasonable size.

**How many row groups, how many skipped by `date >= '2100-01-01'`?**
3 row groups. The dates end on 2048-10-17, so the `stats_max` of every row group is below 2100: all 3 are skipped and only the footer is read (1 GET, 16 KiB). With `date >= '2040-01-01'`, row group 0 (max 2034) is skipped and the 2 others are read.

**What if the orders were shuffled?**
Every row group would contain dates of the whole period, its min/max would cover 2020 to 2048, and no row group could be skipped: the whole `date` column would be read. Filter efficiency relies on sorting or clustering the data on the filtered column (or partitioning).

**CSV vs. Parquet time: network vs. parsing?**
The CSV query takes 1.26 s on S3 and 0.23 s on the local file: about 1.0 s of network (28 MiB) and 0.23 s of parsing (text to typed values, for the 5 columns although only 2 are needed), so about 80 % network and 20 % parsing here. The Parquet query transfers 0.2 MiB of binary, compressed column chunks: its 0.3 s is mostly the latency of 4 HTTP requests. Timing `aws s3 cp` to stdout (2.27 s) is not a reliable network measure: its `user` time shows it is CPU-bound in the CLI.

## Python API

`orders-report` runs DuckDB inside the Python process (no server), loads the same persistent secret as the CLI, and passes the product as a query parameter (`$product`) instead of concatenating it into the SQL string, which prevents SQL injection.

```
$ uv run orders-report -p cookie
2020-01 128 376
2020-02 121 371
2020-03 122 340
2020-04 95  278
```