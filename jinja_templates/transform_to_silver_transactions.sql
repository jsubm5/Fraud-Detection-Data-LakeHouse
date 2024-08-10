INSERT INTO {{ destination_name }}
SELECT 
    transaction_id,
    sender_id,
    receiver_id,
    transaction_amount,
    transaction_currency,
    transaction_datetime,
    CAST(transaction_datetime AS DATE) AS transaction_date,
    m.month_name AS transaction_month,
    DATE_FORMAT(transaction_datetime, 'HH:mm:ss') AS transaction_time,
    HOUR(transaction_datetime) AS transaction_hour,
    transaction_type,
    transaction_location,
    device_id
FROM
    {{ batch_view_or_table_name }} v
LEFT JOIN
    {{ months_lookup_table }} as m
ON 
    m.id = MONTH(v.transaction_datetime)