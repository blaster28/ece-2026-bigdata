# Cleanup (as dev): drop what the lab and the exercises created, keep silver.orders for the Trino lab
spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_by_product")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")

spark.sql("SHOW NAMESPACES IN polaris").show()
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
