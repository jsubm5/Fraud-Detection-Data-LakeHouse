INSERT INTO {{ destination_name }}
(
    customer_id,
    first_name,
    last_name,
    phone_number,
    email,
    customer_address,
    birth_date,
    registration_datetime,
    registration_year,
    registration_month,
    ingested_at             
)
SELECT
    brnz.customer_id,
    brnz.first_name, 
    brnz.last_name,
    brnz.phone_number,
    brnz.email,
    brnz.c_address AS customer_address,
    brnz.birth_date,
    brnz.registration_datetime,
    YEAR(brnz.registration_datetime) AS registration_year,
    mnths.month_name AS registration_month,
    brnz.ingested_at
FROM
    {{ batch_view_or_table_name }} AS brnz
LEFT JOIN
    {{ months_lookup_table }} AS mnths
ON MONTH(brnz.registration_datetime) = mnths.id 