# Lab 10 part 3: Star schema with Trino

Johan LIN, ING4 DATA & AI, GRP03

Lab: [10.trino/lab-3-trino-star-schema.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/10.trino/lab-3-trino-star-schema.md)
Prerequisites: part 2 (`../lab-2/`), Python shell connected to Trino with `../lab-2/00_connect.py`.

## How to run

In the Trino Python shell (`uv run python`), from this folder: `exec(open("../lab-2/00_connect.py").read())` if the
connection is closed, then `exec(open("<file>").read())` for each file in order. Exercise 5 runs in a PySpark shell
connected to Polaris as `dev`, with the session of module 09 (`../../09-polaris/lab-2/00_session.py`).

| File | Step |
|---|---|
| `01_dimensions.py` | `dim_user`, `dim_product` (surrogate keys), `dim_date` (generated calendar) |
| `02_fact.py` | `fact_orders`, row counts |
| `03_star_queries.py` | star queries, `orders_by_state_month` rebuilt from the star and compared |
| `04_validation.py` | asserts of the statement, plus uniqueness and row by row checks |
| `05_ex1_partitioning.py` | exercise 1, partitioning by month |
| `06_ex2_cohorts.py` | exercise 2, `gold.cohorts` |
| `07_ex3_merge.py`, `07b_ex3_check.py` | exercise 3, one order appended, `MERGE`, time travel |
| `08_ex4_optimize.py` | exercise 4, `optimize` |
| `09_ex5_spark.py` | exercise 5, same aggregation in Spark |
| `gold/` | metadata of the gold tables, synced from the bucket (`data/` excluded) |

## Results

| Step | Result |
|---|---|
| Dimensions | `dim_user` 50, `dim_product` 6, `dim_date` 118 (2020-01-01 to 2020-04-27, keys `YYYYMMDD`) |
| Fact | `fact_orders` 2829 rows, as `silver.orders` |
| Star vs `orders_by_state_month` | 50 rows on both sides, identical (`DataFrame.equals` is `True`) |
| Validation | statement checks OK (2829 orders, 8615 units, 0 unmatched); 0 duplicated key, 0 wrong link |
| Exercise 1 | unpartitioned: 1 file (50 rows); partitioned: 4 files `month_month=2020-01..04` (17, 10, 13, 10 rows); filter on February: scan of 50 rows (80% filtered) vs 10 rows (0% filtered) |
| Exercise 2 | 11 decades, 50 users, 2829 orders, 2.79 to 3.19 units per order |
| Exercise 3 | order `...0003` (3 croissants, 2020-04-17) appended; `MERGE` = snapshot `overwrite`, 1 added record, 1 position delete; total 2829 before, 2830 after (`FOR VERSION AS OF`) |
| Exercise 4 | `top_products`: no change (1 file of 760 B); `orders_by_state_month`: 3 files (data 50 rows, position delete, data 1 row) replaced by 1 file of 50 rows, snapshot `replace` |
| Exercise 5 | Spark and Trino: 50 rows, 2830 orders, 8618 units; `exceptAll` in both directions: 0 row |

## Answers

- **Are the checks sufficient?** No. They compare totals and look for missing keys, so an order linked to the wrong
  user or product, or with a date shifted by a time zone, keeps the same count and the same sum of units. Errors can
  also compensate each other. Added checks (`04_validation.py`): uniqueness of `order_id` in the fact and of the
  natural keys in the dimensions, and a row by row comparison of each fact row with its silver order (user, product,
  quantity, date). Reconciliations per dimension (units per product, per state) would also catch wrong links.
- **Grain**: `fact_orders`, one order; `dim_date`, one calendar day.
- **Same `user_id` twice in `silver.users`**: `dim_user` gets 2 rows and 2 keys for this user, the `LEFT JOIN`
  duplicates its orders, and the first assert (`silver_orders == fact_orders`) fails, as well as the units.
  **Order without product**: `dim_product` gets a row for `NULL`, but `p.product = o.product` never matches `NULL`,
  so the order keeps a `NULL` `product_key`: the count passes, the `unmatched` assert fails.
- **`row_number()` keys**: the keys follow the order of `user_id`. If `dim_user` is rebuilt after a new user is added,
  every user sorted after it gets a new key, and the existing rows of `fact_orders` point to the wrong users unless the
  fact table is rebuilt at the same time. Stable keys need a persistent mapping (new users get `max(key) + 1`) or a
  key derived from the natural key.
- **User who moves**: `dim_user` is overwritten (slowly changing dimension type 1), so the past orders show the new
  state. Keeping the history (type 2) needs one row per version of the user, with its own surrogate key, validity
  dates (`valid_from`, `valid_to`) and a current flag; the fact rows keep the key of the version valid at order time.
- **`orders_by_state_month` vs the star**: the aggregate answers one question, already computed (50 rows to read).
  The star answers any combination of its dimensions (product by weekday, state by quarter...) without a new table,
  at the cost of joins and of a scan of the fact table for each query. Here 2829 rows cost nothing; at scale, the
  frequent questions are still materialized as aggregates.
- **Exercise 1**: Iceberg keeps the bounds of each partition in the manifests. With the filter on February, Trino
  only opens the file of the February partition: 10 rows read instead of 50.
- **Exercise 3**: the `MERGE` is merge-on-read: Trino did not rewrite the 50-row file, it wrote a position delete file
  (the old row of the state and month) and a data file with the new row. The previous snapshot stays readable.
- **Exercise 4**: `optimize` rewrites the small files and applies the delete files. `top_products` had a single file
  and no delete: nothing to do, no snapshot. `orders_by_state_month` got a `replace` snapshot: same data, 1 file
  instead of 3. The old files stay in the bucket until `expire_snapshots` and `remove_orphan_files`.
- **Exercise 5**: the same aggregation gives the same 50 rows in Spark and in Trino, both reading the tables through
  Polaris. Spark needs `spark.sql.session.timeZone=UTC` so that `date_trunc` cuts the months like Trino.

## Side effects

Exercise 3 wrote to `silver.orders` through Polaris (2830 rows). The other gold tables (`fact_orders`,
`top_products`, `user_activity`, `cohorts`) were built before and still count 2829 orders, and the Hadoop catalog
`lab` of the Spark lab still points to the version without this order.
