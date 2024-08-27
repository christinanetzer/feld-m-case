SELECT
    CAST(customerID AS STRING) AS customer_id,
    country
FROM
{{source('feld-m-case','customers')}}