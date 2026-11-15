# Exercise 1: birthdate_is_valid, False when the birthdate is later than the first order
from pyspark.sql import functions as F

first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))

# DataFrame API
users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        F.col("birthdate").isNotNull()
        & (F.col("first_ordered_at").isNull()
           | (F.col("first_ordered_at") >= F.col("birthdate").cast("timestamp"))),
    )
    .withColumn("birthdate", F.when(F.col("birthdate_is_valid"), F.col("birthdate")))
    .drop("first_ordered_at")
)

# Spark SQL
stg_orders.createOrReplaceTempView("stg_orders")
stg_users.createOrReplaceTempView("stg_users")
users_valid_sql = spark.sql("""
    WITH first_orders AS (
        SELECT user_id, min(ordered_at) AS first_ordered_at
        FROM stg_orders
        GROUP BY user_id
    ),
    validated AS (
        SELECT u.*,
               u.birthdate IS NOT NULL
               AND (f.first_ordered_at IS NULL
                    OR f.first_ordered_at >= CAST(u.birthdate AS TIMESTAMP)) AS birthdate_is_valid
        FROM stg_users u
        LEFT JOIN first_orders f ON u.user_id = f.user_id
    )
    SELECT user_id, username, name, sex, email,
           CASE WHEN birthdate_is_valid THEN birthdate END AS birthdate,
           street, city, state, zip_code, birthdate_is_valid
    FROM validated
""")

# The two versions must agree, row for row
cols = users_valid_sql.columns
only_df = users_valid_df.select(cols).exceptAll(users_valid_sql).count()
only_sql = users_valid_sql.exceptAll(users_valid_df.select(cols)).count()
print("rows only in DataFrame:", only_df, "| rows only in SQL:", only_sql)

users_valid_df.groupBy("birthdate_is_valid").count().show()
invalid = sorted(r["username"] for r in users_valid_df.filter(~F.col("birthdate_is_valid")).collect())
print("invalid birthdates:", invalid)

# The assertion of the dbt lab: no order before a valid birthdate
orders_before_birth = (
    stg_orders.join(users_valid_df, "user_id")
    .filter(F.col("birthdate_is_valid") & (F.col("ordered_at") < F.col("birthdate").cast("timestamp")))
    .count()
)
print("orders before a valid birthdate:", orders_before_birth)
