# Query the silver layer: counts, address parsing check, orphan orders
print(sql("SELECT count(*) AS orders FROM lakehouse.silver.orders"))
print(sql("SELECT count(*) AS users FROM lakehouse.silver.users"))

print(sql("""
    SELECT state,
           count(*) AS users,
           count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users
    GROUP BY state
    ORDER BY users DESC
    LIMIT 10
"""))

# An order without user would silently disappear from an inner join
print(sql("""
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
"""))
