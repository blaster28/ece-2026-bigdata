# Lab: Bronze to silver with Spark DataFrames

Johan LIN, ING4 DATA & AI, GRP03

Lab: [07.sparksql-and-dataframes/lab-pyspark-silver.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/07.sparksql-and-dataframes/lab-pyspark-silver.md)
Platform: new Onyxia `vscode-pyspark` service (limits `cpu: 4`, `memory: 4Gi`), Spark 4.1.1, Scala 2.13,
Iceberg 1.11.0 (`iceberg-spark-runtime-4.1_2.13`), `local[4]` (the container is limited to 4 CPUs,
while `nproc` shows the 16 cores of the node).

## How to run

```bash
cd 07-spark-silver
pyspark --master "local[4]" \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Then, in the PySpark shell, run the steps in order: `exec(open("00_session.py").read())`, then `01_bronze.py`,
and so on. Each file is a cell of the lab: the variables it defines are reused by the next ones.

| File | Step |
|---|---|
| `00_session.py` | SparkSession, S3A access with the profile credentials, Iceberg catalog `lab` |
| `01_bronze.py` | Bronze CSV files read from S3 |
| `02_stg_users.py` | `stg_users`, DataFrame API |
| `03_stg_orders.py` | `stg_orders`, simulated duplicates, window deduplication |
| `04_spark_sql.py` | Same deduplication in Spark SQL, analytical query per state |
| `05_parquet.py` | Silver layer written to `silver/spark/` as Parquet |
| `06_iceberg.py` | Silver layer written as Iceberg tables `lab.silver.users` and `lab.silver.orders` |
| `07_ex1_birthdate.py` | Exercise 1, `birthdate_is_valid` with the DataFrame API and with SQL |
| `08_ex2_udf.py` | Exercise 2, Python UDF vs `pandas_udf` vs `regexp_extract()` |
| `09_ex3_cache.py` | Exercise 3, cache of the bronze DataFrames |
| `10_ex4_time_travel.py` | Exercise 4, append and time travel, Iceberg vs Parquet |
| `warehouse/` | Metadata of the Iceberg tables (`metadata/`); the data files are excluded by `*.parquet` |

The shell is driven with `exec()` on files rather than pasted code: the Python 3.13 REPL mangles long
multi-line pastes.

## Issue: expired credentials of the pod

The first reads failed with `403 Forbidden` although the AWS profile was valid. The effective Hadoop
configuration showed another access key (`...6713`): when the `SparkContext` starts, Spark copies the
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` variables of the pod into
`fs.s3a.*`. Those were issued when the service started and had expired (`InvalidAccessKeyId` with the
AWS CLI). The `.config("spark.hadoop.fs.s3a...")` options given to the builder of the session already
created by the shell are not copied into the Hadoop configuration. Fix in `00_session.py`: read the current
credentials from `~/.aws/credentials`, set them with `sparkContext._jsc.hadoopConfiguration().set(...)`,
and call `FileSystem.closeAll()` to drop the S3A client cached with the old keys.

## Results

| Step | Result |
|---|---|
| Bronze | 50 users, 2829 orders; `birthdate` inferred as `date`; with `multiLine=True` the 2-line address stays in one field |
| `stg_users` | 50 rows, address split; 6 users without city, all military addresses (`AA`, `AE`, `AP`) |
| `stg_orders` | 2829 orders, 2832 with 3 simulated duplicates, 2829 after deduplication; `ordered_at` timestamp, `quantity` int |
| Spark SQL | Same 2829 rows; plan: `WindowGroupLimit` (partial, before the shuffle), `Exchange hashpartitioning(order_id, 200)`, `Window row_number()`, `Filter (rn = 1)` |
| Parquet | 1 part-file per table (orders 139,556 B, users 9,581 B); `_SUCCESS` is an 11.8 KB JSON summary written by the S3A magic committer |
| Iceberg | `data/` (orders 84,055 B, users 7,436 B, zstd instead of snappy) and `metadata/` (`v1.metadata.json`, manifest list, manifest, `version-hint.text`) |
| dbt vs Spark | Same results: 2829 orders, quantity 8615, 50 users |
| Schemas | `order_id`, `user_id`: `uuid` (dbt) vs `varchar` (Spark); `ordered_at`: `timestamp with time zone` (dbt) vs `timestamp` (Spark, INT96); orders file 72,910 B (dbt) vs 139,556 B (Spark) |

The `WindowGroupLimit` node is an optimization of Catalyst: since only `rn = 1` is kept, each partition drops
the rows which cannot win before the shuffle, like the map-side combiner of `reduceByKey` in the RDD lab.

## Exercises

1. **`birthdate_is_valid`**: `stg_users` joined with the first order date of each user. The DataFrame and SQL
   versions match row for row (`exceptAll` in both directions returns 0 row): 45 valid, 5 invalid
   (`cgrimes`, `charleslandry`, `dayderek`, `nicolejohnson`, `ronaldstephens`), the same 5 users as the dbt
   fix, and 0 order before a valid birthdate.
2. **UDFs**: the 3 versions return the same zip codes.

| | 50 rows | 100,000 rows |
|---|---|---|
| `regexp_extract()` | 0.083 s | 0.136 s |
| `pandas_udf` | 0.133 s | 0.351 s (2.6x) |
| Python UDF | 0.117 s | 0.417 s (3.1x) |

   On 50 rows the fixed cost of a job dominates. On 100,000 rows the native function stays in the JVM and is
   compiled by Catalyst, while both UDFs serialize the values to a Python worker. The `pandas_udf` exchanges
   Arrow batches but `str.split` / `str.extract` still apply the regex per element: it only gains 15%. Native
   functions first, `pandas_udf` for vectorizable computations, Python UDF as a last resort.
3. **Cache** (bytes read from the source per action, from the stages of the Spark UI REST API):

| `count()` | Without cache | With cache, 1st | With cache, 2nd |
|---|---|---|---|
| `stg_users` | 7,351 B (S3) | 16,767 B | 9,416 B (memory) |
| `stg_orders` | 663,136 B = 2 x 331,568 | 775,136 B | 295,712 B = 2 x 147,856 (memory) |

   Without cache `stg_orders` reads the CSV twice per action: `orders_with_duplicates` is the union of
   `orders_typed` and `orders_typed.limit(3)`, and both branches scan the source. With cache the second action
   only reads the in-memory columnar blocks (Storage tab: users 9,416 B, orders 147,856 B) and nothing from S3.
   The time only drops from 0.43 s to 0.34 s: on such small files the fixed cost of the jobs dominates.
4. **Time travel**: the append creates a second snapshot (`append`, parent the `overwrite` one). The table has
   2830 rows, `VERSION AS OF` the first snapshot returns 2829. The Parquet directory also returns 2830 rows,
   with 2 new part-files for 1 row (small files) and no way to read the previous state. Both were restored
   afterwards: `05_parquet.py` again (overwrite) and `CALL lab.system.rollback_to_snapshot`, which writes a
   `v3.metadata.json`; the append snapshot stays in the history. `version-hint.text` holds `3` in the bucket.

## Questions

**The credentials are temporary: what happens to a running job if the service restarts, and why?**
The job is lost: in `local[*]` the driver and executors live in the JVM of the pod, killed with it, with its
cache and shuffle files. The new service gets new credentials, but the SparkSession must be recreated since the
keys were copied into the Hadoop configuration. Even without a restart, a job longer than the token lifetime
(12 h) fails with `403 / ExpiredToken`: in production a provider which renews the credentials is used.

**Why does `SimpleAWSCredentialsProvider` fail here?**
It only reads the access and secret keys and ignores `fs.s3a.session.token`. Temporary STS credentials are only
valid with their token, sent with every signed request. `TemporaryAWSCredentialsProvider` sends all three.

**Explain the output of the query per state.**
Users and missing cities per state code: cities are only missing for the military codes (`AE` 3/3, `AP` 2/2,
`AA`), whose addresses have no city. No civil state has a missing city: the address parsing is correct.
Counts are small and uniform because Faker picks the states at random among 62 codes, territories included.

**DataFrame or SQL, for a one-off exploration and for a model reused by colleagues?**
Both compile to the same Catalyst plan. SQL for exploration: fast to write and readable by everyone. The
DataFrame API for a reused model: reusable and parameterized functions, unit tests, IDE checks, code review.
SQL also fits a shared model when it is versioned and tested, as in dbt.

**Where else could `regexp_extract()` returning an empty string produce a wrong result?**
On `state` and `zip_code`, extracted the same way and not fixed: an empty state would pass a `not_null` check,
form its own group in a `groupBy`, and silently fail a join with a reference table of states. `cast()` behaves
alike: an invalid quantity becomes `NULL` without error.

**How many part-files under `users.parquet/`, and why?**
One per table, one per non-empty output partition: `users` comes from a `multiLine` CSV, not splittable, read
as one partition; `orders` goes through the shuffle of the window (200 partitions) that Adaptive Query
Execution coalesces into one, the data being small.

**What do you lose with `coalesce(1)` on a larger dataset?**
The parallelism: a single task writes everything on one executor, and since `coalesce` is narrow it can also
reduce the upstream computation of the stage to one task. The file cannot be written or rewritten in parallel.
`repartition(1)` keeps the upstream parallelism at the cost of a shuffle; several files of 128 MB-1 GB are fine.

**Compare `silver/spark/orders.parquet/` and `warehouse/silver/orders/`.**
Parquet: the part-files hold the data, `_SUCCESS` only marks the end of the job; nothing describes the table,
which is whatever is listed in the directory. Iceberg: `data/` holds the data; `metadata/` describes the table:
`vN.metadata.json` (schema, partitioning, snapshots, current snapshot), manifest lists `snap-*.avro`, manifests
`*-m0.avro` (data files with row counts and column min/max), and `version-hint.text`, the current version.

**A job overwrites the Parquet directory while a query reads it. What changes with Iceberg?**
Parquet: the overwrite deletes then writes files, not atomically on S3: the reader may hit missing files, a mix
of old and new files, or an empty table. Iceberg writes new files and publishes the new version in one commit:
a running reader keeps its snapshot, a new one sees the complete new version (snapshot isolation).

**The pointer to the current version is a file in the bucket: what if two jobs commit at the same time?**
The `hadoop` catalog relies on an atomic rename that S3 does not provide: two jobs starting from version N may
both write version N+1, one overwrites the other and a write is lost silently. A catalog service (Hive
Metastore, REST catalog such as Polaris or Nessie, Glue) swaps the pointer with an atomic compare-and-swap,
rejects the second commit to be retried, and adds discovery, access control and sharing between engines.

**What differs between the two schemas, and what is the consequence of having no `UUID` type in Spark?**
The identifiers are strings (36 bytes instead of 16, file twice as large) with no guarantee of format: any
string is accepted, a different case or a space creates a different identifier, and joins with `UUID` columns
need a cast. `ordered_at` is written as INT96 without time zone (`timestamp` instead of
`timestamp with time zone`): consumers must know it is UTC, or `spark.sql.parquet.outputTimestampType` must be
set to `TIMESTAMP_MICROS`.

**One object per model with dbt, a directory of part-files with Spark: easier to read, and scalable?**
The single object is easier to consume (one path, no glob, no marker files). The directory scales: tasks write
and read their files in parallel with a bounded size. A table format such as Iceberg gives both: a table read
by name, files managed by the metadata.

**dbt tests vs this lab: what would you need to get the same guarantees?**
This lab only used `assert` on counts and columns and an eye on a `groupBy().count()`. Each rule must become a
query returning 0 row: `groupBy(key).count().filter("count > 1")` (unique), `filter(col.isNull())`
(not_null), `filter(~col.isin(values))` (accepted_values), a `left_anti` join (relationships), run in the
pipeline and failing the job; or a framework such as Great Expectations, Deequ or Soda, or `dbt-spark`.

**What plays the role of `ref()` and `source()` in the notebook?**
Only the Python variable names and the order of execution of the files. Spark knows the lineage of a DataFrame
within a session, but there is no graph of the project: no selection such as `--select +stg_orders`, no rebuild
of the dependents. An orchestrator (Airflow, Dagster) would provide it.

**Why are the build times of dbt and Spark not directly comparable?**
Not the same work (dbt builds views, Spark writes Parquet and Iceberg), not the same fixed costs (JVM, package
download, job scheduling vs an in-process DuckDB), lazy evaluation with repeated actions, and the cache, the
network to S3 and the interactive driving of the shell.

**dbt with DuckDB or Spark?**
A daily batch of 10 GB: dbt with DuckDB, it fits on one machine, simple and cheap, with tests and lineage. A
daily batch of 10 TB: Spark (or a distributed warehouse), the computation must be spread over a cluster. Analysts
who only know SQL: dbt, with `dbt-spark` if the volume requires Spark underneath.
