CREATE OR REPLACE VIEW nessie.gold_transactions_summary AS
SELECT
    sender_id,
    COUNT(transaction_id) AS total_transactions,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_transaction_amount,
    MIN(transaction_datetime) AS first_transaction_date,
    MAX(transaction_datetime) AS last_transaction_date
FROM nessie.silver_transactions
GROUP BY sender_id