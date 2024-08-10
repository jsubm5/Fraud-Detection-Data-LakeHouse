CREATE TABLE IF NOT EXISTS nessie.silver_customers(
    customer_id             STRING,
    first_name              STRING,
    last_name               STRING,
    phone_number            STRING,
    email                   STRING,
    customer_address        STRING,
    birth_date              DATE,
    registration_datetime   TIMESTAMP,
    registration_year       INT,
    registration_month      VARCHAR(3),
    ingested_at             TIMESTAMP
)USING ICEBERG
PARTITIONED BY (DATE(registration_datetime))