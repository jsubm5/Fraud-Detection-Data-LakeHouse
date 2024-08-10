CREATE OR REPLACE TABLE nessie.months_lookup (
    id INT,
    month_name STRING
) USING iceberg;
INSERT INTO nessie.months_lookup (id, month_name) VALUES
    (1, 'Jan'),
    (2, 'Feb'),
    (3, 'Mar'),
    (4, 'Apr'),
    (5, 'May'),
    (6, 'Jun'),
    (7, 'Jul'),
    (8, 'Aug'),
    (9, 'Sep'),
    (10, 'Oct'),
    (11, 'Nov'),
    (12, 'Dec');
