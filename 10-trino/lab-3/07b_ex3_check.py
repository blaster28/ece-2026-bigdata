# Exercise 3, check after the MERGE: snapshots, the row of the new order before and after, totals
NEW_ORDER_ID = "00000000-0000-0000-0000-000000000003"
snapshots = sql("""
    SELECT snapshot_id, committed_at, operation,
           element_at(summary, 'added-records') AS added,
           element_at(summary, 'deleted-records') AS deleted,
           element_at(summary, 'added-position-deletes') AS position_deletes
    FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at
""")
print(snapshots)
before = snapshots.iloc[-2, 0]  # snapshot just before the MERGE

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
