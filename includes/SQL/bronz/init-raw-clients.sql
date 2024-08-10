CREATE TABLE IF NOT EXISTS nessie.bronz_raw_customers (
    first_name              STRING,
    last_name               STRING,
    phone_number            STRING,
    email                   STRING,
    c_address               STRING,
    birth_date              DATE,
    customer_id             STRING,
    registration_datetime   TIMESTAMP,
    ingested_at             TIMESTAMP
) USING iceberg
PARTITIONED BY (MONTH(registration_datetime))