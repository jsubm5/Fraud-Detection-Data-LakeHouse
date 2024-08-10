INSERT INTO {{ destination_name }}
(
    {{ destination_columns }}
)
SELECT
    {{ source_columns }}
FROM
    {{ source_name }}