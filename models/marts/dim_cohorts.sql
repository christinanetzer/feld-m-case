WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="(select DATE_TRUNC(min(order_date), MONTH) from " + ref('stg_orders') | string + ")",
        end_date="DATE_ADD((select max(order_date) from " + ref('stg_orders') | string + "), INTERVAL 1 MONTH)"
    ) }}
), 

customers AS(
SELECT
    customer_id,
    country,
    DATE_TRUNC(first_order_date, MONTH) AS cohort_month,
    total_order_value
FROM
{{ref('dim_customers')}} ),

all_rows AS (
    SELECT
        CAST(ds.date_month AS DATE) AS cohort_month,
        country AS country
    FROM date_spine ds
    CROSS JOIN (SELECT DISTINCT country FROM customers)
)

SELECT 
    ar.cohort_month,
    ar.country,
    COALESCE(COUNT(DISTINCT(c.customer_id)),0) AS n_customers,
    COALESCE(SUM(c.total_order_value),0) AS total_order_value
FROM all_rows ar
LEFT JOIN customers c 
ON ar.cohort_month = c.cohort_month
AND ar.country = c.country
GROUP BY cohort_month, country
