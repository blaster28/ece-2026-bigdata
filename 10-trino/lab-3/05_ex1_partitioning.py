# Exercise 1: same table partitioned by month, files and input rows compared with the unpartitioned one
print(sql("""
    CREATE TABLE lakehouse.gold.orders_by_state_month_part
    WITH (partitioning = ARRAY['month(month)']) AS
    SELECT * FROM lakehouse.gold.orders_by_state_month
"""))

for table in ["orders_by_state_month", "orders_by_state_month_part"]:
    print(table)
    print(sql(f"""
        SELECT element_at(split(file_path, '/'), -2) AS directory, record_count, file_size_in_bytes
        FROM lakehouse.gold."{table}$files"
        ORDER BY directory
    """))

# Same filter on one month: compare the rows read by the scan
for table in ["orders_by_state_month", "orders_by_state_month_part"]:
    text = sql(f"""
        EXPLAIN ANALYZE
        SELECT sum(orders) FROM lakehouse.gold.{table}
        WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'
    """).iloc[0, 0]
    print(table)
    for line in text.splitlines():
        if "ScanFilter" in line or "TableScan" in line or ("Input:" in line and "Splits" in line):
            print("   ", line.strip()[:160])
