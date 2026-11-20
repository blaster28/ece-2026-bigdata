# Distributed plan and execution statistics of the join between orders and users
QUERY = """
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
"""
plan = sql("EXPLAIN (TYPE DISTRIBUTED) " + QUERY)
print(plan.iloc[0, 0])

stats = sql("EXPLAIN ANALYZE " + QUERY)
print(stats.iloc[0, 0])

# History of the last queries, from the system catalog of the cluster
print(sql("""
    SELECT query_id, state,
           date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms,
           substr(query, 1, 60) AS query
    FROM system.runtime.queries
    ORDER BY created DESC
    LIMIT 5
"""))
