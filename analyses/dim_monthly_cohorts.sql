WITH base AS(
SELECT
customer_id,
country,
DATE_TRUNC(first_order_date, MONTH) AS cohort_date,
total_order_value
FROM
{{ref('dim_customers')}} )

SELECT
cohort_date,
country,
COUNT(DISTINCT(customer_id)) AS n_customers,
SUM(total_order_value) AS total_order_value
FROM base
GROUP BY cohort_date, country