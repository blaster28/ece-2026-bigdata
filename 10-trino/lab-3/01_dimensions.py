# Star schema, dimensions: surrogate keys for users and products, a generated calendar for dates
print(sql("""
    CREATE TABLE lakehouse.gold.dim_user AS
    SELECT
        row_number() OVER (ORDER BY user_id) AS user_key,
        user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
"""))
print(sql("""
    CREATE TABLE lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
"""))

# One row per day between the first and the last order
first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day,
           CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
""").iloc[0]
print("calendar:", first_day, "->", last_day)

print(sql(f"""
    CREATE TABLE lakehouse.gold.dim_date AS
    SELECT
        CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
        d                                       AS full_date,
        year(d)                                 AS year,
        quarter(d)                              AS quarter,
        month(d)                                AS month,
        date_format(CAST(d AS timestamp), '%M') AS month_name,
        day_of_week(d)                          AS day_of_week,
        day_of_week(d) IN (6, 7)                AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
"""))
print(sql("SELECT * FROM lakehouse.gold.dim_date ORDER BY date_key LIMIT 5"))
