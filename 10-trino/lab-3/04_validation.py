# Validation of the star against the silver layer (no output from the asserts means they passed)
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]

# No order is lost or duplicated by the lookups of the keys
assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"

# Every order finds its three dimension rows
unmatched = sql("""
    SELECT count(*) AS v
    FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
""").iloc[0, 0]
assert unmatched == 0, f"{unmatched} orders without a complete dimension row"
print("Statement checks OK | orders:", fact_orders, "| units:", fact_units, "| unmatched:", unmatched)

# Additional checks: the counts can match while rows are duplicated or point to the wrong dimension row
duplicates = sql("""
    SELECT (SELECT count(*) - count(DISTINCT order_id) FROM lakehouse.gold.fact_orders)
         + (SELECT count(*) - count(DISTINCT user_id) FROM lakehouse.gold.dim_user)
         + (SELECT count(*) - count(DISTINCT product) FROM lakehouse.gold.dim_product) AS v
""").iloc[0, 0]
assert duplicates == 0, f"{duplicates} duplicated keys"

wrong_rows = sql("""
    SELECT count(*) AS v
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.silver.orders     AS o ON o.order_id = f.order_id
    JOIN lakehouse.gold.dim_user     AS u ON u.user_key = f.user_key
    JOIN lakehouse.gold.dim_product  AS p ON p.product_key = f.product_key
    WHERE u.user_id <> o.user_id
       OR p.product <> o.product
       OR f.quantity <> o.quantity
       OR f.date_key <> CAST(date_format(o.ordered_at, '%Y%m%d') AS integer)
""").iloc[0, 0]
assert wrong_rows == 0, f"{wrong_rows} orders linked to the wrong dimension row"
print("Additional checks OK | duplicated keys:", duplicates, "| wrong links:", wrong_rows)
