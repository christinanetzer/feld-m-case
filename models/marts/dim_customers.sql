WITH 
orders AS(
    SELECT
        o.customer_id,
        o.order_date,
        od.order_id,
        od.unit_price,
        od.quantity,
        od.discount,
        -- Note: I interpret the total order value as the gross order value excl. all deductions and thus ignore the discount in this calculation.
        unit_price*quantity AS order_value,
        -- Note: I interpret 'whether it’s one of the top 10 customers (by revenue generated)' as it should be net from discount
        unit_price*quantity*(1-discount) AS revenue
    FROM
    {{ref('stg_order_details')}} od
    LEFT JOIN
    {{ref('stg_orders')}} o
        ON od.order_id = o.order_id
),

customers AS (
    SELECT 
        customer_id,
        country
    FROM
    {{ref('stg_customers')}})

SELECT
    c.customer_id,
    MIN(c.country) AS country,
    MIN(o.order_date) AS first_order_date,
    MAX(order_value) AS most_expensive_order,
    SUM(order_value) AS total_order_value,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT(order_id)) AS n_orders,
    CASE WHEN 
    ROW_NUMBER() OVER (ORDER BY SUM(revenue) DESC) <= 10 THEN TRUE ELSE FALSE 
    END AS is_top_ten_customer
FROM 
customers c

LEFT JOIN 
orders o
    ON c.customer_id = o.customer_id

GROUP BY c.customer_id
