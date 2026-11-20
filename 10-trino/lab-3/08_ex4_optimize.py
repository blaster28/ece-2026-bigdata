# Exercise 4: compaction with optimize, snapshots and files before and after
def files_and_snapshots(table):
    print(sql(f"""
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.gold."{table}$snapshots" ORDER BY committed_at
    """))
    print(sql(f"""
        SELECT content, element_at(split(file_path, '/'), -1) AS file, record_count, file_size_in_bytes
        FROM lakehouse.gold."{table}$files"
    """))


for table in ["top_products", "orders_by_state_month"]:
    print("=====", table, "before")
    files_and_snapshots(table)
    print(sql(f"ALTER TABLE lakehouse.gold.{table} EXECUTE optimize"))
    print("=====", table, "after")
    files_and_snapshots(table)
