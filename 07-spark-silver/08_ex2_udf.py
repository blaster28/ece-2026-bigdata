# Exercise 2: zip_code extraction with a Python UDF, a pandas_udf and regexp_extract()
import re
import timeit

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf

ZIP_PATTERN = r"([A-Z]{2}) ([0-9]{5})$"
ZIP_RE = re.compile(ZIP_PATTERN)


@udf("string")
def zip_python(address):
    # Row by row: each value is serialized to a Python worker and back
    lines = (address or "").split("\n")
    match = ZIP_RE.search(lines[1]) if len(lines) > 1 else None
    return match.group(2) if match else ""


@pandas_udf("string")
def zip_pandas(addresses: pd.Series) -> pd.Series:
    # By batch: whole columns are exchanged with Arrow and processed with vectorized pandas
    second_line = addresses.str.split("\n").str[1]
    return second_line.str.extract(ZIP_PATTERN)[1].fillna("")


zip_native = F.regexp_extract(F.element_at(F.split("address", "\n"), 2), ZIP_PATTERN, 2)

check = users_bronze.select(
    zip_native.alias("native"),
    zip_python("address").alias("python"),
    zip_pandas("address").alias("pandas"),
)
mismatches = check.filter((F.col("native") != F.col("python")) | (F.col("native") != F.col("pandas"))).count()
print("rows:", check.count(), "| mismatches between the 3 versions:", mismatches)


def bench(df, label, runs=5):
    versions = {
        "regexp_extract": zip_native,
        "python udf": zip_python("address"),
        "pandas_udf": zip_pandas("address"),
    }
    for name, column in versions.items():
        def run():
            df.select(column.alias("zip_code")).write.format("noop").mode("overwrite").save()
        best = min(timeit.repeat(run, number=1, repeat=runs))
        print(f"  {label:<14} {name:<15} best of {runs}: {best:.3f} s")


big = users_bronze.crossJoin(spark.range(2000)).drop("id").repartition(4).cache()
print("big rows:", big.count())
bench(users_bronze, "50 rows")
bench(big, "100 000 rows")
