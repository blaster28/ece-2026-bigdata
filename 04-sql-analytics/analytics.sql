-- 4e. Window functions : cumul et moyenne mobile sur 7 jours
WITH daily AS (
  SELECT date::DATE AS day, sum(quantity) AS quantity
  FROM orders
  GROUP BY day
)
SELECT
  day,
  quantity,
  sum(quantity) OVER (ORDER BY day) AS cumulative,
  round(avg(quantity) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily
ORDER BY day
LIMIT 10;

-- 4f. Produit le plus vendu de chaque mois
SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders
GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;

-- 4g. Pivot produits x mois
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product
USING sum(quantity)
ORDER BY month;