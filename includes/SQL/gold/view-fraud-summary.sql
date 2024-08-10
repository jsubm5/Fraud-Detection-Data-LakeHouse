CREATE OR REPLACE VIEW nessie.gold_fraud_analysis AS
SELECT
    t.sender_id,
    COUNT(f.transaction_id) AS fraud_transactions_count,
    SUM(t.transaction_amount) AS total_fraud_amount,
    AVG(t.transaction_amount) AS avg_fraud_amount
FROM nessie.silver_fraud_transactions f
JOIN nessie.silver_transactions t ON f.transaction_id = t.transaction_id
GROUP BY t.sender_id