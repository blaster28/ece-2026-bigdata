"""Lab: Introduction to Spark's RDD API (ECE Big Data, module 06).

Johan LIN, ING4 DATA & AI, GRP03

Run from this directory, after downloading the bronze datasets:
    aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv
    aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv
    spark-submit rdd_lab.py
"""
import json
import re
import time
import urllib.request
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


def ui(path):
    """Read the Spark UI REST API (same data as the Stages and Storage tabs)."""
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def show_stages(last=6):
    for s in sorted(ui("stages"), key=lambda s: (s["stageId"], s["attemptId"]))[-last:]:
        print(f'  stage {s["stageId"]:>3} {s["status"]:<9} tasks={s["numTasks"]:<3} '
              f'input={s.get("inputBytes", 0):>7} shuffleRead={s.get("shuffleReadBytes", 0):>7} '
              f'shuffleWrite={s.get("shuffleWriteBytes", 0):>7}  {s["name"][:45]}')


def storage():
    rdds = ui("storage/rdd")
    print("  Storage tab:", [(r["name"], r["numCachedPartitions"], r["memoryUsed"]) for r in rdds] or "empty")


def timed(label, action):
    t0 = time.perf_counter()
    result = action()
    print(f"  {label:<34} {time.perf_counter() - t0:7.3f} s  -> {result}")


def section(title):
    print(f"\n=== {title} ===")


section("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("  defaultParallelism:", sc.defaultParallelism)
print("  users / orders partitions:", users_lines.getNumPartitions(), orders_lines.getNumPartitions())
print("  after repartition(8):", orders_lines.repartition(8).getNumPartitions())

section("Lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
print("  count 1:", orders_data.count())
print("  count 2:", orders_data.count())
show_stages(3)

section("The cost of no schema: the multiline address")
print("\n".join(users_lines.take(3)))
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("  physical lines:", users_lines.count(), "| user records:", users_records.count())

section("Manual parsing of orders")


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
print("  first order:", orders_rdd.first())

section("Key-value RDDs: reduceByKey vs groupByKey")
pairs = orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))
user_quantity = pairs.reduceByKey(add)
print("  reduceByKey users:", len(user_quantity.collect()))
show_stages(2)
user_quantity_grouped = pairs.groupByKey().mapValues(sum)
print("  groupByKey users:", len(user_quantity_grouped.collect()))
show_stages(2)
print("  same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

section("Manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print("  before / after dedup:", orders_rdd.count(), orders_dedup.count())

section("Joining orders with users")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
print("  first enriched row:", enriched.first())
print("  enriched rows:", enriched.count())
show_stages(3)
left = orders_kv.leftOuterJoin(users_kv)
print("  left rows:", left.count(), "| without user:", left.filter(lambda kv: kv[1][1] is None).count())

section("Exercise 1: aggregateByKey")
user_stats = pairs.aggregateByKey(
    (0, 0),                                   # zero value: (order_count, total_quantity)
    lambda acc, q: (acc[0] + 1, acc[1] + q),  # within a partition
    lambda a, b: (a[0] + b[0], a[1] + b[1]),  # between partitions
)
user_avg = user_stats.mapValues(lambda c: round(c[1] / c[0], 2))
print("  (order_count, total_quantity):", user_stats.take(3))
print("  average quantity per order:", user_avg.take(3))
print("  totals match reduceByKey:",
      sorted(user_stats.mapValues(lambda c: c[1]).collect()) == sorted(user_quantity.collect()))
print("  orders counted:", user_stats.map(lambda kv: kv[1][0]).sum())

section("Exercise 2: order with an unknown user")
fake = {
    "order_id": "00000000-0000-0000-0000-000000000000",
    "user_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
    "date": "2020-05-01 00:00:00+00:00",
    "quantity": 1,
    "product": "bread",
}
orders_with_fake = orders_rdd.union(sc.parallelize([fake]))
fake_kv = orders_with_fake.map(lambda o: (o["user_id"], o))
orphans = fake_kv.leftOuterJoin(users_kv).filter(lambda kv: kv[1][1] is None)
print("  orders:", orders_with_fake.count(), "| orphans:", orphans.count())
print("  orphan:", orphans.collect())
print("  inner join rows:", fake_kv.join(users_kv).count())

section("Exercise 3: the same aggregation with the DataFrame API")
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True).withColumnRenamed("user_uuid", "user_id")
orders_df.printSchema()
user_quantity_df = orders_df.groupBy("user_id").sum("quantity")
user_quantity_df.show(5, truncate=False)
user_quantity_df.explain()
print(user_quantity.toDebugString().decode())
print("  same result:",
      sorted((r["user_id"], r["sum(quantity)"]) for r in user_quantity_df.collect()) == sorted(user_quantity.collect()))

section("Exercise 4: with and without cache")
orders_rdd.unpersist()
orders_cached = orders_data.map(parse_order).cache()
uq_cached = orders_cached.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
timed("cache    orders count (1st)", orders_cached.count)
timed("cache    orders count (2nd)", orders_cached.count)
timed("cache    user_quantity count (1st)", uq_cached.count)
timed("cache    user_quantity count (2nd)", uq_cached.count)
storage()
orders_cached.unpersist()
orders_nocache = orders_data.map(parse_order)
uq_nocache = orders_nocache.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
timed("no cache orders count (1st)", orders_nocache.count)
timed("no cache orders count (2nd)", orders_nocache.count)
timed("no cache user_quantity count (1st)", uq_nocache.count)
timed("no cache user_quantity count (2nd)", uq_nocache.count)
storage()

spark.stop()
