# Exercise 5 (Spark, principal dev): the aggregation of orders_by_state_month, compared with the Trino table
# Run in the pyspark shell after exec(open("../../09-polaris/lab-2/00_session.py").read())
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")  # date_trunc on the same UTC months as Trino

spark_result = spark.sql("""
    SELECT u.state, date_trunc('MONTH', o.ordered_at) AS month,
           count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
    FROM polaris.silver.orders AS o
    JOIN polaris.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")
trino_table = spark.table("polaris.gold.orders_by_state_month").select("state", "month", "orders", "units", "buyers")

for name, df in [("Spark aggregation", spark_result), ("Trino gold table", trino_table)]:
    totals = df.agg(F.count("*").alias("rows"), F.sum("orders").alias("orders"), F.sum("units").alias("units")).first()
    print(f"{name}: rows={totals['rows']} orders={totals['orders']} units={totals['units']}")

print("rows only in Spark:", spark_result.exceptAll(trino_table).count(),
      "| rows only in Trino:", trino_table.exceptAll(spark_result).count())
