-- Query external data from S3
SELECT passenger_count,
  avg(toFloat32(total_amount))
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_0.gz',
    'TabSeparatedWithNames'
  )
GROUP BY passenger_count
ORDER BY passenger_count;
-- Moving the data into a ClickHouse table, where is create table though ---
INSERT INTO nyc_taxi
SELECT *
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_0.gz',
    'TabSeparatedWithNames'
  ) SETTINGS input_format_allow_errors_num = 25000;
