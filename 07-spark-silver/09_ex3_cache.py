# Exercise 3: cache the bronze DataFrames and measure what each action reads from the source
import json
import time
import urllib.request

UI = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}"


def ui(path):
    with urllib.request.urlopen(f"{UI}/{path}") as response:
        return json.load(response)


def measure(label, action):
    last_stage = max((s["stageId"] for s in ui("stages")), default=-1)
    t0 = time.perf_counter()
    action()
    elapsed = time.perf_counter() - t0
    new_stages = [s for s in ui("stages") if s["stageId"] > last_stage]
    read = sum(s.get("inputBytes", 0) for s in new_stages)
    print(f"  {label:<26} {elapsed:6.3f} s | bytes read from the source: {read:>7} | stages: {len(new_stages)}")


def uses_cache(df):
    # A new query (count) is planned now, after the cache() calls
    return "InMemoryTableScan" in df.groupBy().count()._jdf.queryExecution().executedPlan().toString()


def storage_tab():
    rdds = ui("storage/rdd")
    print("  Storage tab:", [(r["name"][:30], r["numCachedPartitions"], r["memoryUsed"]) for r in rdds] or "empty")


big.unpersist()
for label, cached in [("Without cache", False), ("With cache", True)]:
    print(label)
    if cached:
        users_bronze.cache()
        orders_bronze.cache()
    else:
        users_bronze.unpersist()
        orders_bronze.unpersist()
    measure("stg_users.count() (1st)", stg_users.count)
    measure("stg_users.count() (2nd)", stg_users.count)
    measure("stg_orders.count() (1st)", stg_orders.count)
    measure("stg_orders.count() (2nd)", stg_orders.count)
    print("  plan uses the cache:", uses_cache(stg_orders))
    storage_tab()
