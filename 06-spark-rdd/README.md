# Lab: Introduction to Spark's RDD API

Johan LIN, ING4 DATA & AI, GRP03

Lab: [06.spark-introduction/lab-pyspark-rdd.md](https://github.com/adaltas/ece-bigdata-2026-fall/blob/main/06.spark-introduction/lab-pyspark-rdd.md)
Platform: Onyxia `vscode-pyspark`, Spark 4.1.1, Python 3.13, `local[*]` with 2 cores (`defaultParallelism = 2`).

## Files

- `rdd_lab.py`: the whole lab and its exercises, run with `spark-submit rdd_lab.py`
- `rdd_lab_output.txt`: output of a run of the script

`users.csv` and `orders.csv` are local copies of the bronze layer, excluded by `.gitignore`. The Spark UI (`localhost:4040`) is not reachable from the browser on Onyxia: its REST API (`/api/v1/applications/<app>/stages` and `/storage/rdd`) was read from the driver instead, which gives the same figures as the Stages and Storage tabs.

## Results

| Section | Result |
|---|---|
| Partitions | `textFile()`: 2 partitions for each file; 8 after `repartition(8)` |
| Lazy evaluation | `first()`: 1 task, 262 144 B read; each `count()` is a new stage with 2 tasks reading the whole file again (427 928 B, more than the 331 568 B of the file: buffered reads, and the 2nd split reads past its boundary to finish its line) |
| No schema | `users.csv`: 101 physical lines for 50 users (header + 2 lines per user, the address spans 2 lines); 50 records after the UUID filter |
| Manual parsing | `quantity` cast to `int` by hand, `date` left as a string |
| `reduceByKey` vs `groupByKey` | Same result for 50 users; shuffle write 2 513 B vs 6 230 B (2.5x) |
| Deduplication | 2829 orders before and after (no duplicate in the generated data) |
| Join | 2829 enriched rows `(user_id, (order, username))`; the shuffle reads 182 502 B (full order dictionaries); on the second action the map stage is `SKIPPED`: its shuffle files are reused |
| `leftOuterJoin` | 0 order without user |

## Exercises

1. **`aggregateByKey`**: `(order_count, total_quantity)` per user in a single pass, with the zero value `(0, 0)`, a function within a partition and a function between partitions; the totals match `reduceByKey` and 2829 orders are counted. The average order quantity is `total / count` per user.
2. **Unknown user**: one fake order added (2830 orders). `leftOuterJoin` keeps it with `None` as username (1 orphan), while the inner `join` silently drops it (2829 rows).
3. **DataFrame API**: `groupBy("user_id").sum("quantity")` gives the same result in 2 lines instead of about 12 for the RDD version. The column is named `user_uuid` in the raw CSV: the first attempt with `user_id` failed with an `AnalysisException` before any job ran, an RDD would only fail during execution. The physical plan says what each step does: `FileScan csv` reading only 2 columns (`ReadSchema: struct<user_uuid:string,quantity:int>`), `HashAggregate(partial_sum)`, `Exchange hashpartitioning(user_id, 200)`, `HashAggregate(sum)`, within an `AdaptiveSparkPlan`. The RDD lineage only shows `PythonRDD`, `PairwiseRDD` and `ShuffledRDD`, with no notion of column or operation, and every column is parsed in Python.
4. **Cache**:

| Measure | With cache | Without cache |
|---|---|---|
| `orders` count, 1st | 0.525 s (read, parse and store) | 0.197 s |
| `orders` count, 2nd | 0.197 s | 0.290 s |
| `user_quantity` count, 1st | 0.403 s | 0.415 s |
| `user_quantity` count, 2nd | 0.197 s | 0.193 s |
| Storage tab | 1 RDD, 2 partitions, 172 051 B | empty |

The cache works (Storage tab, second count 2.7x faster than the first), but on a 330 KB file its gain is hidden by the fixed cost of a job (about 0.2 s), and the first run is slower because it stores the partitions. It pays off when the lineage is expensive (large or remote file, heavy parsing) and reused by several actions. The second `user_quantity` count is fast in both cases because the shuffle files of `reduceByKey` are reused, not because of the cache.

## Questions

**What decides the default number of partitions of `textFile()` for a file this size?**
`minPartitions` defaults to `min(defaultParallelism, 2)` = 2. The file is split into Hadoop input splits of at most one block (32 MB locally, 128 MB on HDFS / S3A); a small file fits in one block, so `minPartitions` wins: 2 partitions.

**`repartition()` or `coalesce()`: which one shuffles?**
`repartition(n)` always performs a full shuffle (it is `coalesce(n, shuffle=True)`): it can increase or decrease the number of partitions and balances them. `coalesce(n)` merges existing partitions in place without a shuffle (narrow dependency): it can only decrease the count and may leave them unbalanced.

**Does a second `count()` create a new stage? Why?**
Yes: an RDD is only a lineage, every action launches a new job which recomputes it from the source (the same 427 928 B are read again). Nothing is kept unless requested.

**When would `.cache()` change that answer?**
When called on `orders_data` before the first action: the first action computes and stores the partitions, the next ones still launch a job but read the cached blocks instead of the file, and the RDD appears in the Storage tab.

**Why does `spark.read.csv(multiLine=True)` not have the multiline address problem?**
The CSV reader understands the CSV syntax: a line break inside quotes belongs to the field. With `multiLine=True` it reads each file as one stream instead of splitting it at arbitrary byte offsets. `textFile()` splits blindly at every line break.

**What has to be true of a file format for a line-oriented reader to be safe?**
Every record fits on exactly one physical line: the record delimiter never appears inside a record (or it is escaped). The file can then be split at any line end (JSON Lines, logs, CSV without embedded line breaks).

**What if one line had an extra comma inside `product`?**
`split(",")` returns 6 fields for 5 variables: `ValueError: too many values to unpack`. Because `map` is lazy, the error is raised in a task at the first action touching that line, and fails the whole job. `split` also ignores quotes, so even a correctly quoted value would break the parsing.

**Where does the DataFrame API perform the equivalent check, and when?**
In the CSV reader, when the data is read, with a declared or inferred schema: it applies the CSV rules (quotes, escapes) and casts the types. Malformed rows follow the `mode` option: `PERMISSIVE` (nulls and `_corrupt_record`), `DROPMALFORMED` or `FAILFAST`. Column names and types are checked at analysis time, before any job runs (observed in exercise 3).

**What does `groupByKey` shuffle instead of combined values, and why is it more expensive?**
Every `(key, value)` pair (the 2829 quantities) instead of one partial sum per key and partition: more bytes to serialize, write and transfer (2.5x here), and all the values of a key must fit in memory on one executor before the `sum`, a risk with skewed keys.

**For which aggregations is `groupByKey` unavoidable?**
When all the values of a key are needed together and the aggregation cannot be expressed as an associative combination of partial results: exact median or percentiles, mode, ordering the events of a user to build a sequence or a session, or keeping the full list. Averages, counts or bounded top-N fit in `aggregateByKey` / `combineByKey`.

**What plays the role of `PARTITION BY` in the deduplication?**
The shuffle of `reduceByKey`: it hash-partitions the `(order_id, order)` pairs, so every row of an `order_id` lands in the same partition, where `most_recent` combines them (starting on the map side).

**Which version states the intent more directly?**
The window function: "keep row number 1 per `order_id` ordered by date descending" reads directly and the optimizer chooses the execution. The `reduceByKey` version hides the intent in a Python function, and relies on comparing dates as strings, which only works because they share the ISO format and offset.

**What must be true of the partitioning of `orders_kv` and `users_kv` for the join to skip the shuffle?**
Both must already be partitioned by the join key with the same partitioner and number of partitions (e.g. `partitionBy(4)` on both, computed once and cached): the rows of a `user_id` are then in the same partition on both sides and the join becomes a narrow dependency. With a small table such as `users`, broadcasting it and looking it up on the map side avoids the shuffle entirely.

**What changes with `leftOuterJoin()` if a `user_id` has no matching user?**
The order is kept with `None` as username: `(user_id, (order, None))`, while `join()` silently drops it (exercise 2).

**Risk of calling `.collect()` on `enriched`?**
`collect()` brings every row to the driver, in a single process: on a large dataset the driver runs out of memory and the application fails, and everything transits through the network. Filter or aggregate first, or use `take(n)`, write the result to storage, or iterate with `toLocalIterator()`.
