-- Athena Engine 3 template for the public SWH 2026-06-04 directory_entry ORC.
-- Replace YOUR_DATABASE before running. Athena query results must be written
-- to an S3 bucket owned by the caller. This query produces P4 directory hits,
-- not repository URLs; origin/revision mapping is a later graph join.

CREATE DATABASE IF NOT EXISTS YOUR_DATABASE;

CREATE EXTERNAL TABLE IF NOT EXISTS YOUR_DATABASE.swh_directory_entry_20260604 (
  directory_id string,
  name binary,
  type string,
  target string,
  perms int
)
STORED AS ORC
LOCATION 's3://softwareheritage/graph/2026-06-04/orc/directory_entry/';

SELECT
  directory_id,
  from_utf8(name) AS name,
  type,
  target AS content_id
FROM YOUR_DATABASE.swh_directory_entry_20260604
WHERE type = 'file'
  AND lower(from_utf8(name)) LIKE '%.p4';
