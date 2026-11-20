# As dev: the registered table, then a second table created through Polaris
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)

stg_users = spark.table("lab.silver.users")
stg_users.writeTo("polaris.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()

# Where Polaris put the files of the new table
spark.sql("SELECT file_path FROM polaris.silver.users.files").show(truncate=False)
spark.sql("SELECT file FROM polaris.silver.users.metadata_log_entries").show(truncate=False)
