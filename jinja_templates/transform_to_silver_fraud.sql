INSERT INTO {{ destination_name }}
    transaction_id,
    labeled_at,
    sender_id,
    receiver_id, 
    transaction_datetime,
    transaction_type,
    transaction_location,
    device_id,
    ingested_at
SELECT 
    brnz_tr.transaction_id,
    btch.labeled_at,
    brnz_tr.sender_id,
    brnz_tr.receiver_id, 
    brnz_tr.transaction_datetime,
    brnz_tr.transaction_type,
    brnz_tr.transaction_location,
    brnz_tr.device_id,
    btch.ingested_at
FROM
    {{ batch_table_or_view }} btch
LEFT JOIN
    {{ bronz_transactions_table_name }} AS brnz_tr
ON
    brnz_tr.transaction_id = btch.transaction_id