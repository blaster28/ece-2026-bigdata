# Exercise 2: users, orders and average units per order for each decade of birth (from the star)
print(sql("""
    CREATE TABLE lakehouse.gold.cohorts AS
    SELECT
        (year(u.birthdate) / 10) * 10 AS birth_decade,
        count(DISTINCT u.user_key)    AS users,
        count(f.order_id)             AS orders,
        round(avg(f.quantity), 2)     AS avg_units_per_order
    FROM lakehouse.gold.dim_user AS u
    LEFT JOIN lakehouse.gold.fact_orders AS f ON f.user_key = u.user_key
    GROUP BY 1
"""))
cohorts = sql("SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade")
print(cohorts)
fact_count = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
assert cohorts["users"].sum() == 50 and cohorts["orders"].sum() == fact_count
print("Cohorts OK | users:", cohorts["users"].sum(), "| orders:", cohorts["orders"].sum())
