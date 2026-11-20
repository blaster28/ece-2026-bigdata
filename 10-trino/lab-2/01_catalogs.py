# Catalogs, schemas and tables exposed by Trino, and the types of the silver tables
print(sql("SHOW CATALOGS"))
print(sql("SHOW SCHEMAS"))  # no catalog given: the default one of connect(catalog=...)
print(sql("SHOW SCHEMAS FROM lakehouse"))
print(sql("SHOW TABLES FROM lakehouse.silver"))
print(sql("DESCRIBE lakehouse.silver.orders"))
print(sql("DESCRIBE lakehouse.silver.users"))
