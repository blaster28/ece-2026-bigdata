# Gold layer: aggregated Iceberg tables built with CREATE TABLE AS SELECT, in the Polaris namespace "gold"
print(sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold"))
print(sql("SHOW SCHEMAS FROM lakehouse"))

# Orders by state and month (inner join: relies on the orphan check, 0 orphan order)
print(sql("""
    CREATE TABLE lakehouse.gold.orders_by_state_month AS
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*)                          AS orders,
        sum(o.quantity)                   AS units,
        count(DISTINCT o.user_id)         AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
"""))
print(sql("SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10"))

# Best-selling products
print(sql("""
    CREATE TABLE lakehouse.gold.top_products AS
    SELECT
        product,
        count(*)      AS orders,
        sum(quantity) AS units,
        rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
"""))
print(sql("SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10"))

# Activity of each user (left join: keeps the users who never ordered)
print(sql("""
    CREATE TABLE lakehouse.gold.user_activity AS
    SELECT
        u.user_id,
        u.state,
        date_diff('year', u.birthdate, current_date) AS age,
        count(o.order_id)            AS orders,
        coalesce(sum(o.quantity), 0) AS units,
        min(o.ordered_at)            AS first_order_at,
        max(o.ordered_at)            AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
"""))
print(sql("SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10"))
print(sql("SELECT count_if(orders = 0) AS users_without_order FROM lakehouse.gold.user_activity"))

# Tables of the gold schema, and where Polaris put the files of one of them
print(sql("SHOW TABLES FROM lakehouse.gold"))
print(sql("""
    SELECT file_path, record_count, file_size_in_bytes
    FROM lakehouse.gold."top_products$files"
"""))
