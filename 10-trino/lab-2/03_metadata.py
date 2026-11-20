# Iceberg metadata tables of silver.orders, read through Polaris (hidden tables with a $ suffix)
print(sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots"
    ORDER BY committed_at
"""))
print(sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lakehouse.silver."orders$files"
"""))
print(sql('SELECT * FROM lakehouse.silver."orders$history"'))
print(sql("""
    SELECT path, added_snapshot_id, added_data_files_count, existing_data_files_count, deleted_data_files_count
    FROM lakehouse.silver."orders$manifests"
"""))
