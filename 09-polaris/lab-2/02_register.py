# Register the lab 07 table in Polaris: only a pointer to its current metadata file, no data copy
# version-hint.text = 3, so the current metadata file is v3.metadata.json
METADATA_FILE = f"s3a://{BUCKET}/warehouse/silver/orders/metadata/v3.metadata.json"

spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
spark.sql(f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => '{METADATA_FILE}'
    )
""").show(truncate=False)

# Same table seen through both catalogs
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
spark.sql("SELECT count(*) AS orders FROM lab.silver.orders").show()
