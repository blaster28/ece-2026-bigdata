.timer on
COPY (FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode = false))
TO (getvariable('bucket') || '/large/orders.parquet') (FORMAT parquet);
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet');
