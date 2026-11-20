# Refresh a gold table: DELETE then INSERT, one snapshot per statement, then time travel
print(sql("DELETE FROM lakehouse.gold.top_products"))
print(sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity),
           rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders
    GROUP BY product
"""))

snapshots = sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
""")
print(snapshots)

first = snapshots.iloc[0, 0]
second = snapshots.iloc[1, 0]
print(sql(f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}"))
print(sql(f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {second}"))
print(sql("SELECT count(*) AS products FROM lakehouse.gold.top_products"))
