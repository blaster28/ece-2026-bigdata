# Connection to Trino from Python, default catalog "lakehouse" (Iceberg REST catalog served by Polaris)
# Run inside "uv run python": exec(open("00_connect.py").read())
import os

import pandas as pd
import trino

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 80)

CATALOG = "lakehouse"
conn = trino.dbapi.connect(
    host=os.environ.get("TRINO_HOST", "trino"),
    port=int(os.environ.get("TRINO_PORT", "8080")),
    user=os.environ.get("TRINO_USER", os.environ["KUBERNETES_NAMESPACE"]),
    http_scheme="http",
    catalog=CATALOG,
)


def sql(statement):
    """Run a statement and return its result as a DataFrame (fetchall waits for the end of the query)."""
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


print(sql("SELECT version()"))
