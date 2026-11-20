# Exercise 2 (as silver_user, role silver_reader): silver is readable, gold is not


def try_show(statement):
    """Run a query, show its result if Polaris allows it, else the reason of the refusal."""
    try:
        spark.sql(statement).show(truncate=False)
        print("ALLOWED:", statement)
    except Exception as error:
        lines = str(error).splitlines()
        useful = [l for l in lines if "orbidden" in l or "authorized" in l] or lines[:2]
        print("REFUSED:", statement)
        for line in useful[:3]:
            print("   ", line.strip()[:300])


try_show("SHOW TABLES IN polaris.silver")
try_show("SELECT count(*) AS orders FROM polaris.silver.orders")
try_show("SHOW TABLES IN polaris.gold")
try_show("SELECT * FROM polaris.gold.orders_by_product")
try_show("SHOW NAMESPACES IN polaris")
