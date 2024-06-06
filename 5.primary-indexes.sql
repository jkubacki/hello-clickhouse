-- Create MergeTree engine table
CREATE TABLE hits_NoPrimaryKey (
  `UserID` UInt32,
  `URL` String,
  `EventTime` DateTime
) ENGINE = MergeTree PRIMARY KEY tuple();
-- Insert data - 8.87 million rows
INSERT INTO hits_NoPrimaryKey
SELECT intHash32(UserID) AS UserID,
  URL,
  EventTime
FROM url(
    'https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz',
    'TSV',
    'WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16), URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8, FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8, SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8, HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16), RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16, SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8'
  )
WHERE URL != '';
-- Optimize
OPTIMIZE TABLE hits_NoPrimaryKey FINAL;
-- 10 most clicked urls for user 749927693
SELECT URL,
  count(URL) as Count
FROM hits_NoPrimaryKey
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
-- Elapsed: 0.037 sec. Processed 8.87 million rows, this doesn't scale
-- Create table with primary key
CREATE TABLE hits_UserID_URL (
  `UserID` UInt32,
  `URL` String,
  `EventTime` DateTime
) ENGINE = MergeTree PRIMARY KEY (UserID, URL)
ORDER BY (UserID, URL, EventTime) SETTINGS index_granularity = 8192,
  index_granularity_bytes = 0;
-- Insert data - 8.87 million rows
INSERT INTO hits_UserID_URL
SELECT intHash32(UserID) AS UserID,
  URL,
  EventTime
FROM url(
    'https://datasets.clickhouse.com/hits/tsv/hits_v1.tsv.xz',
    'TSV',
    'WatchID UInt64,  JavaEnable UInt8,  Title String,  GoodEvent Int16,  EventTime DateTime,  EventDate Date,  CounterID UInt32,  ClientIP UInt32,  ClientIP6 FixedString(16),  RegionID UInt32,  UserID UInt64,  CounterClass Int8,  OS UInt8,  UserAgent UInt8,  URL String,  Referer String,  URLDomain String,  RefererDomain String,  Refresh UInt8,  IsRobot UInt8,  RefererCategories Array(UInt16),  URLCategories Array(UInt16), URLRegions Array(UInt32),  RefererRegions Array(UInt32),  ResolutionWidth UInt16,  ResolutionHeight UInt16,  ResolutionDepth UInt8,  FlashMajor UInt8, FlashMinor UInt8,  FlashMinor2 String,  NetMajor UInt8,  NetMinor UInt8, UserAgentMajor UInt16,  UserAgentMinor FixedString(2),  CookieEnable UInt8, JavascriptEnable UInt8,  IsMobile UInt8,  MobilePhone UInt8,  MobilePhoneModel String,  Params String,  IPNetworkID UInt32,  TraficSourceID Int8, SearchEngineID UInt16,  SearchPhrase String,  AdvEngineID UInt8,  IsArtifical UInt8,  WindowClientWidth UInt16,  WindowClientHeight UInt16,  ClientTimeZone Int16,  ClientEventTime DateTime,  SilverlightVersion1 UInt8, SilverlightVersion2 UInt8,  SilverlightVersion3 UInt32,  SilverlightVersion4 UInt16,  PageCharset String,  CodeVersion UInt32,  IsLink UInt8,  IsDownload UInt8,  IsNotBounce UInt8,  FUniqID UInt64,  HID UInt32,  IsOldCounter UInt8, IsEvent UInt8,  IsParameter UInt8,  DontCountHits UInt8,  WithHash UInt8, HitColor FixedString(1),  UTCEventTime DateTime,  Age UInt8,  Sex UInt8,  Income UInt8,  Interests UInt16,  Robotness UInt8,  GeneralInterests Array(UInt16), RemoteIP UInt32,  RemoteIP6 FixedString(16),  WindowName Int32,  OpenerName Int32,  HistoryLength Int16,  BrowserLanguage FixedString(2),  BrowserCountry FixedString(2),  SocialNetwork String,  SocialAction String,  HTTPError UInt16, SendTiming Int32,  DNSTiming Int32,  ConnectTiming Int32,  ResponseStartTiming Int32,  ResponseEndTiming Int32,  FetchTiming Int32,  RedirectTiming Int32, DOMInteractiveTiming Int32,  DOMContentLoadedTiming Int32,  DOMCompleteTiming Int32,  LoadEventStartTiming Int32,  LoadEventEndTiming Int32, NSToDOMContentLoadedTiming Int32,  FirstPaintTiming Int32,  RedirectCount Int8, SocialSourceNetworkID UInt8,  SocialSourcePage String,  ParamPrice Int64, ParamOrderID String,  ParamCurrency FixedString(3),  ParamCurrencyID UInt16, GoalsReached Array(UInt32),  OpenstatServiceName String,  OpenstatCampaignID String,  OpenstatAdID String,  OpenstatSourceID String,  UTMSource String, UTMMedium String,  UTMCampaign String,  UTMContent String,  UTMTerm String, FromTag String,  HasGCLID UInt8,  RefererHash UInt64,  URLHash UInt64,  CLID UInt32,  YCLID UInt64,  ShareService String,  ShareURL String,  ShareTitle String,  ParsedParams Nested(Key1 String,  Key2 String, Key3 String, Key4 String, Key5 String,  ValueDouble Float64),  IslandID FixedString(16),  RequestNum UInt32,  RequestTry UInt8'
  )
WHERE URL != '';
-- Optimize table
OPTIMIZE TABLE hits_UserID_URL FINAL;
-- Query for metadata
SELECT part_type,
  path,
  formatReadableQuantity(rows) AS rows,
  formatReadableSize(data_uncompressed_bytes) AS data_uncompressed_bytes,
  formatReadableSize(data_compressed_bytes) AS data_compressed_bytes,
  formatReadableSize(primary_key_bytes_in_memory) AS primary_key_bytes_in_memory,
  marks,
  formatReadableSize(bytes_on_disk) AS bytes_on_disk
FROM system.parts
WHERE (table = 'hits_UserID_URL')
  AND (active = 1) FORMAT Vertical;
-- marks: 1083
-- Select
SELECT URL,
  count(URL) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
-- Processed 8.19 thousand rows
-- 8870000/1083 = 8192 rows per mark. Checks out.
-- Explain
EXPLAIN indexes = 1
SELECT URL,
  count(URL) AS Count
FROM hits_UserID_URL
WHERE UserID = 749927693
GROUP BY URL
ORDER BY Count DESC
LIMIT 10;
--  1083 granules was selected as possibly containing rows with a UserID column value of 749927693.
-- Using multiple primary indexes
SELECT UserID,
  count(UserID) AS Count
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
-- Processed 8.81 million rows - nope - URL column is not the first key column
-- Secondary table
CREATE TABLE hits_URL_UserID (
  `UserID` UInt32,
  `URL` String,
  `EventTime` DateTime
) ENGINE = MergeTree PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime) SETTINGS index_granularity = 8192,
  index_granularity_bytes = 0;
-- Insert data from hits_UserID_URL
INSERT INTO hits_URL_UserID
SELECT *
from hits_UserID_URL;
-- Optimize
OPTIMIZE TABLE hits_URL_UserID FINAL;
-- Now query
SELECT UserID,
  count(UserID) AS Count
FROM hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
-- Processed 319.49 thousand rows
-- Materialized Views
CREATE MATERIALIZED VIEW mv_hits_URL_UserID ENGINE = MergeTree() PRIMARY KEY (URL, UserID)
ORDER BY (URL, UserID, EventTime) POPULATE AS
SELECT *
FROM hits_UserID_URL;
-- Query mareialized view
SELECT UserID,
  count(UserID) AS Count
FROM mv_hits_URL_UserID
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
-- Progress: 335.87 thousand rows
-- Projections - a hidden table
ALTER TABLE hits_UserID_URL
ADD PROJECTION prj_url_userid (
    SELECT *
    ORDER BY (URL, UserID)
  );
-- Query with projection
SELECT UserID,
  count(UserID) AS Count
FROM hits_UserID_URL
WHERE URL = 'http://public_search'
GROUP BY UserID
ORDER BY Count DESC
LIMIT 10;
-- Processed 319.49 thousand rows
