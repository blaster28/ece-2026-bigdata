# Exercise 4: append one order, then read the table as it was before (time travel)
from datetime import datetime

new_order = spark.createDataFrame(
    [("00000000-0000-0000-0000-000000000001", "bdd640fb-0667-4ad1-9c80-317fa3b1799d",
      datetime(2020, 5, 1, 12, 0, 0), 1, "bread")],
    schema=stg_orders.schema,
)

# Iceberg: each commit is a new snapshot, the previous ones stay readable
new_order.writeTo("lab.silver.orders").append()
snapshots = spark.sql("""
    SELECT snapshot_id, parent_id, operation, committed_at
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""")
snapshots.show(truncate=False)
first_snapshot = snapshots.first()["snapshot_id"]
print("current table:", spark.table("lab.silver.orders").count(), "rows")
print("VERSION AS OF first snapshot:",
      spark.sql(f"SELECT count(*) AS n FROM lab.silver.orders VERSION AS OF {first_snapshot}").first()["n"], "rows")
spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor FROM lab.silver.orders.history").show(truncate=False)

# Parquet directory: an append only adds a part-file, nothing records the previous state
parquet_path = f"s3a://{BUCKET}/silver/spark/orders.parquet"
new_order.write.mode("append").parquet(parquet_path)
print("Parquet after append:", spark.read.parquet(parquet_path).count(), "rows")
files = spark.read.parquet(parquet_path).inputFiles()
print("Parquet part-files:", len(files))
for f in files:
    print("  ", f.split("/")[-1])
