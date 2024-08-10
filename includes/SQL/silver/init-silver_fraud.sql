CREATE TABLE IF NOT EXISTS nessie.silver_fraud_transactions (
    transaction_id STRING,
    labeled_at TIMESTAMP,
    sender_id STRING,
    receiver_id STRING,
    transaction_amount      DOUBLE,
    transaction_datetime TIMESTAMP,
    transaction_type STRING,
    transaction_location STRING,
    device_id STRING,
    ingested_at TIMESTAMP 
)USING ICEBERG PARTITIONED BY (DATE(transaction_datetime))