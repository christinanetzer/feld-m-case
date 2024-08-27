SELECT
    CAST(orderID AS STRING) AS order_id,
    CAST(productID AS STRING) AS product_id,
    unitPrice AS unit_price,
    quantity,
    discount
FROM
{{source('feld-m-case','order_details')}}