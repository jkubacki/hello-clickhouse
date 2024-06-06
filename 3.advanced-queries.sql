-- Create trips table --
CREATE TABLE trips (
  `trip_id` UInt32,
  `vendor_id` Enum8(
    '1' = 1,
    '2' = 2,
    '3' = 3,
    '4' = 4,
    'CMT' = 5,
    'VTS' = 6,
    'DDS' = 7,
    'B02512' = 10,
    'B02598' = 11,
    'B02617' = 12,
    'B02682' = 13,
    'B02764' = 14,
    '' = 15
  ),
  `pickup_date` Date,
  `pickup_datetime` DateTime,
  `dropoff_date` Date,
  `dropoff_datetime` DateTime,
  `store_and_fwd_flag` UInt8,
  `rate_code_id` UInt8,
  `pickup_longitude` Float64,
  `pickup_latitude` Float64,
  `dropoff_longitude` Float64,
  `dropoff_latitude` Float64,
  `passenger_count` UInt8,
  `trip_distance` Float64,
  `fare_amount` Float32,
  `extra` Float32,
  `mta_tax` Float32,
  `tip_amount` Float32,
  `tolls_amount` Float32,
  `ehail_fee` Float32,
  `improvement_surcharge` Float32,
  `total_amount` Float32,
  `payment_type` Enum8(
    'UNK' = 0,
    'CSH' = 1,
    'CRE' = 2,
    'NOC' = 3,
    'DIS' = 4
  ),
  `trip_type` UInt8,
  `pickup` FixedString(25),
  `dropoff` FixedString(25),
  `cab_type` Enum8('yellow' = 1, 'green' = 2, 'uber' = 3),
  `pickup_nyct2010_gid` Int8,
  `pickup_ctlabel` Float32,
  `pickup_borocode` Int8,
  `pickup_ct2010` String,
  `pickup_boroct2010` String,
  `pickup_cdeligibil` String,
  `pickup_ntacode` FixedString(4),
  `pickup_ntaname` String,
  `pickup_puma` UInt16,
  `dropoff_nyct2010_gid` UInt8,
  `dropoff_ctlabel` Float32,
  `dropoff_borocode` UInt8,
  `dropoff_ct2010` String,
  `dropoff_boroct2010` String,
  `dropoff_cdeligibil` String,
  `dropoff_ntacode` FixedString(4),
  `dropoff_ntaname` String,
  `dropoff_puma` UInt16
) ENGINE = MergeTree PARTITION BY toYYYYMM(pickup_date)
ORDER BY pickup_datetime;
--- Insert dataset from s3 ---
INSERT INTO trips
SELECT *
FROM s3(
    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{1..2}.gz',
    'TabSeparatedWithNames',
    "
    `trip_id` UInt32,
    `vendor_id` Enum8('1' = 1, '2' = 2, '3' = 3, '4' = 4, 'CMT' = 5, 'VTS' = 6, 'DDS' = 7, 'B02512' = 10, 'B02598' = 11, 'B02617' = 12, 'B02682' = 13, 'B02764' = 14, '' = 15),
    `pickup_date` Date,
    `pickup_datetime` DateTime,
    `dropoff_date` Date,
    `dropoff_datetime` DateTime,
    `store_and_fwd_flag` UInt8,
    `rate_code_id` UInt8,
    `pickup_longitude` Float64,
    `pickup_latitude` Float64,
    `dropoff_longitude` Float64,
    `dropoff_latitude` Float64,
    `passenger_count` UInt8,
    `trip_distance` Float64,
    `fare_amount` Float32,
    `extra` Float32,
    `mta_tax` Float32,
    `tip_amount` Float32,
    `tolls_amount` Float32,
    `ehail_fee` Float32,
    `improvement_surcharge` Float32,
    `total_amount` Float32,
    `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4),
    `trip_type` UInt8,
    `pickup` FixedString(25),
    `dropoff` FixedString(25),
    `cab_type` Enum8('yellow' = 1, 'green' = 2, 'uber' = 3),
    `pickup_nyct2010_gid` Int8,
    `pickup_ctlabel` Float32,
    `pickup_borocode` Int8,
    `pickup_ct2010` String,
    `pickup_boroct2010` String,
    `pickup_cdeligibil` String,
    `pickup_ntacode` FixedString(4),
    `pickup_ntaname` String,
    `pickup_puma` UInt16,
    `dropoff_nyct2010_gid` UInt8,
    `dropoff_ctlabel` Float32,
    `dropoff_borocode` UInt8,
    `dropoff_ct2010` String,
    `dropoff_boroct2010` String,
    `dropoff_cdeligibil` String,
    `dropoff_ntacode` FixedString(4),
    `dropoff_ntaname` String,
    `dropoff_puma` UInt16
"
  ) SETTINGS input_format_try_infer_datetimes = 0;
-- Count ---
SELECT count()
FROM trips;
-- Distinct neighborhood
SELECT DISTINCT(pickup_ntaname)
FROM trips;
-- Avg tip
SELECT round(avg(tip_amount), 2)
FROM trips;
-- Average cost based on the number of passengers:
SELECT passenger_count,
  ceil(avg(total_amount), 2) AS average_total_amount
FROM trips
GROUP BY passenger_count;
-- Here is a query that calculates the daily number of pickups per neighborhood:
SELECT pickup_date,
  pickup_ntaname,
  SUM(1) AS number_of_trips
FROM trips
GROUP BY pickup_date,
  pickup_ntaname
ORDER BY pickup_date ASC;
-- rides to LaGuardia or JFK airports
SELECT pickup_datetime,
  dropoff_datetime,
  total_amount,
  pickup_nyct2010_gid,
  dropoff_nyct2010_gid,
  CASE
    WHEN dropoff_nyct2010_gid = 138 THEN 'LGA'
    WHEN dropoff_nyct2010_gid = 132 THEN 'JFK'
  END AS airport_code,
  EXTRACT(
    YEAR
    FROM pickup_datetime
  ) AS year,
  EXTRACT(
    DAY
    FROM pickup_datetime
  ) AS day,
  EXTRACT(
    HOUR
    FROM pickup_datetime
  ) AS hour
FROM trips
WHERE dropoff_nyct2010_gid IN (132, 138)
ORDER BY pickup_datetime;
