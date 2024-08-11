WITH max_destination_date AS (
    SELECT 
        COALESCE(MAX({{ destination_ingestion_timestamp }}), TIMESTAMP '1970-01-01 00:00:00') AS max_date 
    FROM 
        {{ destination_name }}
),
batch AS (
    SELECT 
        *
    FROM 
        {{ source_name }} AS src
    WHERE 
        src.{{ source_ingestion_timestamp }} > (SELECT max_date FROM max_destination_date)
)
SELECT
    *
FROM
    batch;