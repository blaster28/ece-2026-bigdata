# Write the silver layer as Parquet: one directory of part-files per DataFrame
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")

# Read them back: no row lost or duplicated
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("Parquet OK | users:", users_silver.count(), "| orders:", orders_silver.count())
print("output partitions | users:", stg_users.rdd.getNumPartitions(), "| orders:", stg_orders.rdd.getNumPartitions())
