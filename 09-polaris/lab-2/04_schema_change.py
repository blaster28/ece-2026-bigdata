# As dev: schema change on a table managed by Polaris
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE polaris.silver.users").show(30, truncate=False)

# Each commit writes a new metadata file; Polaris now points to the latest one
spark.sql("""
    SELECT timestamp, file FROM polaris.silver.users.metadata_log_entries ORDER BY timestamp
""").show(truncate=False)
spark.sql("""
    SELECT timestamp, file FROM polaris.silver.orders.metadata_log_entries ORDER BY timestamp
""").show(truncate=False)
