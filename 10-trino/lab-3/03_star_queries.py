# Query the star: join the fact table to the dimensions needed by the question, and only those
print(sql("""
    SELECT d.year, u.state, p.product,
           count(*)        AS orders,
           sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product
    ORDER BY units DESC
    LIMIT 10
"""))

# orders_by_state_month rebuilt from the star, next to the table built in the previous lab
star = sql("""
    SELECT u.state, d.year, d.month,
           count(*)                   AS orders,
           sum(f.quantity)            AS units,
           count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month
    ORDER BY d.year, d.month, u.state
""")
gold = sql("""
    SELECT state, year(month) AS year, month(month) AS month, orders, units, buyers
    FROM lakehouse.gold.orders_by_state_month
    ORDER BY year, month, state
""")
print(star.head(10))
print(gold.head(10))
print("rows:", len(star), "vs", len(gold), "| identical:", star.reset_index(drop=True).equals(gold.reset_index(drop=True)))
