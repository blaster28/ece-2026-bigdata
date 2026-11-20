# Exercise 1 (as dev): drop the country column, a metadata-only change
spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
spark.sql("DESCRIBE polaris.silver.users").show(30, truncate=False)

# No new snapshot, a new metadata file with a new schema id, the same data file
spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.users.snapshots").show(truncate=False)
spark.sql("""
    SELECT timestamp, file, latest_schema_id
    FROM polaris.silver.users.metadata_log_entries ORDER BY timestamp
""").show(truncate=False)
spark.sql("SELECT file_path FROM polaris.silver.users.files").show(truncate=False)
