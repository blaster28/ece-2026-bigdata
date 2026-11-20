# Fact table: one row per order (grain of silver.orders), with the keys of the dimensions
print(sql("""
    CREATE TABLE lakehouse.gold.fact_orders AS
    SELECT
        o.order_id,
        u.user_key,
        p.product_key,
        CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
        o.ordered_at,
        o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
"""))

print(sql("""
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders
"""))
