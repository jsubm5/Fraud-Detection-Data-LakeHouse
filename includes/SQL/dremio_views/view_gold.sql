SELECT
    t.sender_id,
    COUNT(f.transaction_id) AS fraud_transactions_count,
    SUM(t.transaction_amount) AS total_fraud_amount,
    AVG(t.transaction_amount) AS avg_fraud_amount
FROM nessie.silver_fraud_transactions f
JOIN nessie.silver_transactions t ON f.transaction_id = t.transaction_id
GROUP BY t.sender_id;

SELECT
    sender_id,
    COUNT(transaction_id) AS total_transactions,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_transaction_amount,
    MIN(transaction_datetime) AS first_transaction_date,
    MAX(transaction_datetime) AS last_transaction_date
FROM nessie.silver_transactions
GROUP BY sender_id