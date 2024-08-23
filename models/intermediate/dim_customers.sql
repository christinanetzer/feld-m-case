WITH 
orders AS(
    SELECT
    o.customerID AS customer_id,
    od.orderID AS order_id,
    od.unitPrice,
    od.quantity,
    od.discount,
    unitPrice*quantity*(1-discount) AS order_value
    FROM
    {{ref('stg_order_details')}} od
    LEFT JOIN
    {{ref('stg_orders')}} o
    ON od.orderID = o.orderID
)

SELECT
customer_id,
MAX(order_value) AS most_expensive_order,
COUNT(DISTINCT(order_id)) AS number_of_orders,
CASE WHEN 
ROW_NUMBER() OVER (ORDER BY SUM(order_value) DESC) < 11 THEN 1 ELSE 0 END AS is_top_ten_customer
FROM orders
GROUP BY customer_id
