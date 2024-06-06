-- Create taxi zone dictionary
CREATE DICTIONARY taxi_zone_dictionary (
  `LocationID` UInt16 DEFAULT 0,
  `Borough` String,
  `Zone` String,
  `service_zone` String
) PRIMARY KEY LocationID SOURCE(
  HTTP(
    URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'
  )
) LIFETIME(MIN 0 MAX 0) LAYOUT(HASHED_ARRAY());
-- Select dictionary
SELECT *
FROM taxi_zone_dictionary;
-- fetch value from dictionary
SELECT dictGet('taxi_zone_dictionary', 'Borough', 132);
-- check if dictionary has key
SELECT dictHas('taxi_zone_dictionary', 132);
-- 1
SELECT dictHas('taxi_zone_dictionary', 4567);
-- 0
-- Retrieve from dict in a query
SELECT count(1) AS total,
  dictGetOrDefault(
    'taxi_zone_dictionary',
    'Borough',
    toUInt64(pickup_nyct2010_gid),
    'Unknown'
  ) AS borough_name
FROM trips
WHERE dropoff_nyct2010_gid = 132
  OR dropoff_nyct2010_gid = 138
GROUP BY borough_name
ORDER BY total DESC;
-- Join dictionary
SELECT count(1) AS total,
  Borough
FROM trips
  JOIN taxi_zone_dictionary ON toUInt64(trips.pickup_nyct2010_gid) = taxi_zone_dictionary.LocationID
WHERE dropoff_nyct2010_gid = 132
  OR dropoff_nyct2010_gid = 138
GROUP BY Borough
ORDER BY total DESC;
-- Don't use SELECT *
