WITH base AS(
    SELECT
        od.order_id,
        od.product_id,
        o.customer_id,
        CASE WHEN o.order_date = c.first_order_date THEN TRUE ELSE FALSE 
        END AS is_new_customer,
        o.order_date,
        FIRST_VALUE(o.order_date) OVER(PARTITION BY o.customer_id, od.product_id ORDER BY o.order_date ASC) AS product_first_purchase_date
    FROM
    {{ref('stg_order_details')}} od
    
    LEFT JOIN 
    {{ref('stg_orders')}} o
        ON od.order_id = o.order_id
    
    LEFT JOIN
    {{ref('dim_customers')}} c 
        ON o.customer_id = c.customer_id)

SELECT
    product_id,
    order_id,
    customer_id,
    order_date,
    is_new_customer,
    product_first_purchase_date,
    DATE_DIFF(order_date, product_first_purchase_date, DAY) AS days_since_first_purchase
FROM base
