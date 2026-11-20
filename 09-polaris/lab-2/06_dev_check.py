# Back as dev: did the refused INSERT of the analyst create a snapshot?
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
spark.sql("SELECT count(*) AS data_files FROM polaris.silver.orders.files").show()
