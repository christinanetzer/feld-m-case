SELECT
    CAST(orderID AS STRING) AS order_id,
    CAST(customerID AS STRING) AS customer_id,
    CAST(orderDate AS DATE) AS order_date
FROM
{{source('feld-m-case','orders')}}