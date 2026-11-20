# As analyst (catalog_reader): reads are allowed, writes and schema changes are refused by Polaris
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT product, count(*) AS orders FROM polaris.silver.orders GROUP BY product ORDER BY product").show()


def try_sql(statement):
    """Run a statement and print whether Polaris allowed it, with the useful part of the error."""
    try:
        spark.sql(statement)
        print("ALLOWED:", statement)
    except Exception as error:
        lines = str(error).splitlines()
        useful = [l for l in lines if "orbidden" in l or "authorized" in l] or lines[:2]
        print("REFUSED:", statement)
        for line in useful[:3]:
            print("   ", line.strip()[:300])


try_sql("INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
try_sql("ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
