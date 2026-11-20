# Lab 09 part 2: Govern Iceberg tables with Polaris

Johan LIN, ING4 DATA & AI, GRP03

Lab: [09.polaris/lab-2.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/09.polaris/lab-2.md)
Prerequisite: Polaris deployed in part 1 (`../README.md`), catalog `user-j-lin-ece`, principals `dev` and `analyst`.
Platform: Onyxia `vscode-pyspark` (limits 2000m / 2500Mi), Spark 4.1, Iceberg 1.11.0, `local[2]`.

## How to run

```bash
cd 09-polaris/lab-2
eval "$(aws configure export-credentials --profile default --format env)"
set -a; . ~/work/.polaris-credentials.env; set +a   # principal credentials, outside Git

spark_as() {
  POLARIS_PRINCIPAL="$1" pyspark --master "local[2]" \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
}
spark_as dev        # or analyst, silver_user
```

In the shell: `exec(open("00_session.py").read())`, then the step files. Changing principal means
restarting the shell, because Spark caches the catalog and its OAuth token once it is loaded.

| File | Principal | Step |
|---|---|---|
| `00_session.py` | any | S3A with the profile keys, catalogs `lab` (Hadoop) and `polaris` (REST) |
| `01_namespaces.py` | dev | `SHOW NAMESPACES` in both catalogs |
| `02_register.py` | dev | `register_table` of `silver.orders` from lab 07 |
| `03_dev_tables.py` | dev | snapshots, `silver.users` created through Polaris |
| `04_schema_change.py` | dev | `ADD COLUMN country`, metadata log |
| `05_analyst.py` | analyst | reads, refused `INSERT` and `ALTER` |
| `06_dev_check.py` | dev | no snapshot left by the refused `INSERT` |
| `07_ex1_drop_column.py` | dev | exercise 1 |
| `08_ex2_gold.py` | dev | exercise 2, namespace `gold` |
| `09_ex2_silver_reader.sh` | root | exercise 2, role `silver_reader` and principal `silver_user` |
| `10_ex2_check.py` | silver_user | exercise 2, silver allowed, gold refused |
| `11_cleanup.py` | dev | cleanup, `silver.orders` kept for the Trino lab |
| `warehouse/` | | metadata synced from the bucket (data files excluded by `*.parquet`) |

Adaptations to the statement: the pod still exports expired `AWS_*` variables, so `00_session.py` reads
`~/.aws/credentials` and sets the Hadoop configuration directly (same fix as lab 07). The catalogs are set
with `spark.conf.set` and the Iceberg extensions are passed at launch, since a `SparkSession.builder` on
the session created by the shell ignores static settings such as `spark.sql.extensions`.

## Results

| Step | Result |
|---|---|
| `SHOW NAMESPACES` | `lab`: `silver` (directories of the bucket); `polaris`: empty (answer of the REST API) |
| Register | `v3.metadata.json` (`version-hint.text` = 3), snapshot `5121821282528029599`, 2829 rows, 1 data file |
| Same table, 2 catalogs | `count(*)` = 2829 through `polaris` and through `lab` |
| Bucket after register | 16 objects under `warehouse/silver/`, as before |
| `silver.users` (Polaris) | 50 rows, `warehouse/silver/users/data/00000-5-...parquet`, `metadata/00000-<uuid>.metadata.json` |
| `ADD COLUMN country` | new `00001-<uuid>.metadata.json`, no new snapshot |
| Exercise 1, `DROP COLUMN` | new `00002-<uuid>.metadata.json`, still 1 snapshot and the same data file |
| Gold table | 6 products, 2829 orders, 8615 items |

### Refusals returned by Polaris

| Principal | Statement | Refused operation |
|---|---|---|
| analyst | `INSERT INTO silver.orders` | `ADD_TABLE_SNAPSHOT` |
| analyst | `ADD COLUMN phone` | `ADD_TABLE_SCHEMA` |
| analyst | `DROP COLUMN country` | `SET_TABLE_CURRENT_SCHEMA` |
| analyst | `CREATE NAMESPACE gold` | `CREATE_NAMESPACE` |
| silver_user | `SHOW TABLES IN gold` | `LIST_TABLES` |
| silver_user | `SELECT * FROM gold.orders_by_product` | `LOAD_TABLE` |
| silver_user | `SHOW NAMESPACES` | `LIST_NAMESPACES` |

Message format: `ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]'
and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT`.

## Answers

- **Which component answered `SHOW NAMESPACES`?** For `lab`, the Hadoop catalog inside Spark listed the
  directories under `s3a://user-j-lin-ece/warehouse/`. For `polaris`, the Iceberg REST client sent a request to
  Polaris, which read its PostgreSQL metastore: the bucket had a `silver` directory, but Polaris knew no namespace.
- **Where is the credential used?** In the requests to Polaris: the Spark REST client exchanges `clientId:clientSecret`
  for an OAuth token (`/api/catalog/v1/oauth/tokens`), then sends it with every catalog call. It is never stored in
  the bucket. The S3 keys are a separate credential, used by Spark to read and write the files.
- **What did `register_table` write?** In Polaris, a table entity that points to `v3.metadata.json`. In the bucket,
  nothing: 16 objects before and after.
- **Why stop writing through `lab`?** Each catalog keeps its own pointer to the current metadata file
  (`version-hint.text` for Hadoop, PostgreSQL for Polaris). A commit through `lab` would create `v4` and move only the
  Hadoop pointer; Polaris would still serve `v3`, and the two catalogs would diverge.
- **Where did Polaris store `silver.users`?** Under `warehouse/silver/users/`: base location of the catalog, then
  namespace, then table name. It is the folder of the lab 07 `users` table; both coexist because the file names
  differ (`v1.metadata.json` + `version-hint.text` for Hadoop, `0000N-<uuid>.metadata.json` for Polaris).
- **Registered vs created through Polaris**: `orders` was written by the Hadoop catalog (`s3a://` paths) and Polaris
  only adopted its metadata; `users` was laid out by Polaris (`s3://` paths). In both cases the data files are written
  by Spark with the S3A keys, not by Polaris.
- **Which metadata file records the schema change?** `00001-...metadata.json` of `users`, a new file; `register_table`
  pointed to `v3.metadata.json` of `orders`, which is unchanged.
- **Which privilege does Polaris check?** Polaris authorizes each metadata update of an Iceberg commit:
  `ADD_TABLE_SCHEMA` for `ADD COLUMN`, `SET_TABLE_CURRENT_SCHEMA` for `DROP COLUMN`. `dev` holds them through
  `CATALOG_MANAGE_CONTENT`, which includes `TABLE_FULL_METADATA`. The documentation lists `TABLE_WRITE_PROPERTIES`
  and mentions fine-grained update privileges, but does not map each operation to a privilege, so the exact name is not
  confirmed here.
- **Missing privilege for the `INSERT`**: `TABLE_WRITE_DATA` (operation `ADD_TABLE_SNAPSHOT`). `catalog_reader` only
  provides `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`.
- **Did the failed `INSERT` create a snapshot?** No: as `dev`, `silver.orders` still has its 2 lab 07 snapshots and
  2829 rows.
- **Exercise 1, why a metadata operation?** Iceberg tracks columns by ID. Dropping `country` writes a new schema in a
  new metadata file; the Parquet file is not rewritten, the column is simply no longer projected.
- **Exercise 2**: `silver_reader` holds `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES` and
  `TABLE_READ_DATA` on the namespace `silver` only. The grants are inherited by the tables of the namespace, no
  catalog-level privilege was needed, and `silver_user` cannot list namespaces or see `gold`.

## What Polaris cannot enforce here

The catalog runs with `stsUnavailable: true`: Polaris does not vend short-lived storage credentials, and every
principal reads and writes files with the same S3 keys. During the refused `INSERT`, the Spark logs show
`IcebergBatchWrite ... is aborting` then `aborted`: the write job ran before the commit, so Spark most likely wrote
a Parquet file with the S3 keys, then removed it itself after the `Forbidden` (the bucket still had 2 data files).
Polaris protects the catalog pointers and the
table metadata; it cannot stop a principal holding the S3 keys from reading `warehouse/gold/` directly, or from
writing files into the bucket.

## Cleanup

`gold.orders_by_product`, the namespace `gold` and `silver.users` are dropped from Polaris (without `PURGE`, so
their files stay in the bucket). `silver.orders` stays registered for the Trino lab. The exercise principal
`silver_user` and its roles are kept; its credential is in `~/work/.polaris-credentials.env`, outside the repository.
