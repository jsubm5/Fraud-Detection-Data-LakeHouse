WITH max_destination_date AS (
    SELECT 
        COALESCE(MAX({{ destination_ingestion_timestamp }}), DATE '1970-01-01') AS max_date 
    FROM 
        {{ destination_name }}
),
batch AS (
    SELECT 
        *
    FROM 
        {{ source_name }} AS src
    WHERE 
        CAST(src.{{ source_ingestion_timestamp }} AS DATE) > (SELECT max_date FROM max_destination_date)
)
SELECT
    *
FROM
    batch;