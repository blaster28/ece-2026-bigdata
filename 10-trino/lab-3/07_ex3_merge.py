# Exercise 3: append one order to silver.orders, refresh orders_by_state_month with a MERGE, time travel
NEW_ORDER_ID = "00000000-0000-0000-0000-000000000003"

# One more order for the first user, one hour after its last order (inserted only once)
print(sql(f"""
    INSERT INTO lakehouse.silver.orders
    SELECT '{NEW_ORDER_ID}', user_id, max(ordered_at) + INTERVAL '1' HOUR, 3, 'croissant'
    FROM lakehouse.silver.orders
    WHERE user_id = (SELECT min(user_id) FROM lakehouse.silver.orders)
      AND NOT EXISTS (SELECT 1 FROM lakehouse.silver.orders WHERE order_id = '{NEW_ORDER_ID}')
    GROUP BY user_id
"""))
print(sql(f"SELECT * FROM lakehouse.silver.orders WHERE order_id = '{NEW_ORDER_ID}'"))

before = sql("""
    SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots"
    ORDER BY committed_at DESC LIMIT 1
""").iloc[0, 0]

# Upsert: update the (state, month) rows which changed, insert the new ones
print(sql("""
    MERGE INTO lakehouse.gold.orders_by_state_month AS t
    USING (
        SELECT u.state, date_trunc('month', o.ordered_at) AS month,
               count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    ) AS s
    ON t.state = s.state AND t.month = s.month
    WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
        THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
    WHEN NOT MATCHED
        THEN INSERT (state, month, orders, units, buyers) VALUES (s.state, s.month, s.orders, s.units, s.buyers)
"""))

print(sql("""
    SELECT snapshot_id, committed_at, operation, element_at(summary, 'added-records') AS added, element_at(summary, 'deleted-records') AS deleted
    FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at
"""))

# The row of the new order, before and after the MERGE
ROW = f"""
    JOIN (SELECT u.state, date_trunc('month', o.ordered_at) AS month
          FROM lakehouse.silver.orders AS o JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
          WHERE o.order_id = '{NEW_ORDER_ID}') AS k
    ON k.state = g.state AND k.month = g.month
"""
print(sql(f"SELECT g.* FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {before} AS g {ROW}"))
print(sql(f"SELECT g.* FROM lakehouse.gold.orders_by_state_month AS g {ROW}"))
print(sql(f"""
    SELECT (SELECT sum(orders) FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {before}) AS orders_before,
           (SELECT sum(orders) FROM lakehouse.gold.orders_by_state_month) AS orders_after
"""))
