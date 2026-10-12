-- 1. Average number of orders per user, and average quantity per order
SELECT
  round(count(*) / (SELECT count(*) FROM users), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- 2. First and last order date per user, and number of days between them
SELECT
  u.username,
  min(o.date) AS first_order,
  max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.uuid, u.username
ORDER BY days_between DESC, u.username;

-- 3. Hour of the day with the highest quantity sold (ties kept with rank)
SELECT hour(date) AS hour, count(*) AS orders, sum(quantity) AS quantity
FROM orders
GROUP BY hour
QUALIFY rank() OVER (ORDER BY sum(quantity) DESC) = 1;

-- 4. Month-over-month variation of the quantity sold per product, in percent
WITH monthly AS (
  SELECT product, date_trunc('month', date)::DATE AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY product, month
)
SELECT
  product,
  strftime(month, '%Y-%m') AS month,
  quantity,
  lag(quantity) OVER w AS previous_quantity,
  round(100 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly
WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

-- 5. Users who ordered every product at least once
SELECT u.username, count(DISTINCT o.product) AS products
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.uuid, u.username
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
