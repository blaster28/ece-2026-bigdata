# Exercise 2 (as dev): a gold namespace with an aggregated table, to test a namespace-scoped role
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
spark.sql("""
    CREATE OR REPLACE TABLE polaris.gold.orders_by_product AS
    SELECT product, count(*) AS orders, sum(quantity) AS quantity
    FROM polaris.silver.orders
    GROUP BY product
""")
spark.sql("SELECT * FROM polaris.gold.orders_by_product ORDER BY product").show()
spark.sql("SHOW NAMESPACES IN polaris").show()
