-- Candidate extraction template for a Software Heritage graph export.
-- Replace GRAPH_DATASET with the exported table prefix. The exact table
-- names/column types vary by SWH release; keep the output column names below
-- stable for collect_swh_p4.py.
--
-- This query deliberately selects GitHub origins and main/master/HEAD
-- branches only. If a GHArchive/default-branch table is available, join it in
-- the github_origins CTE and set github_default_branch per origin.
-- For maximum branch recall, remove the main_branches name predicate and
-- export every revision branch; run the collector with --selection-policy
-- latest-any or --selection-policy all.

WITH github_origins AS (
  SELECT
    CAST(id AS STRING) AS origin_id,
    CAST(url AS STRING) AS origin_url,
    CAST(NULL AS STRING) AS github_default_branch
  FROM `GRAPH_DATASET.origin`
  WHERE REGEXP_CONTAINS(
    LOWER(CAST(url AS STRING)),
    r'^https?://(www\.)?github\.com/'
  )
),
latest_visits AS (
  SELECT origin, snapshot, date AS visit_date
  FROM (
    SELECT
      origin,
      snapshot,
      date,
      ROW_NUMBER() OVER (PARTITION BY origin ORDER BY date DESC) AS row_number
    FROM `GRAPH_DATASET.origin_visit_status`
    WHERE snapshot IS NOT NULL
      AND status = 'full'
  )
  WHERE row_number = 1
),
main_branches AS (
  SELECT
    CAST(snapshot AS STRING) AS snapshot_id,
    CAST(name AS STRING) AS branch_name,
    CAST(target AS STRING) AS revision_id
  FROM `GRAPH_DATASET.snapshot_branch`
  WHERE target_type = 'revision'
    AND LOWER(CAST(name AS STRING)) IN (
      'head', 'main', 'master', 'refs/heads/main', 'refs/heads/master'
    )
),
revisions AS (
  SELECT
    CAST(id AS STRING) AS revision_id,
    CAST(directory AS STRING) AS directory_id,
    date AS revision_date
  FROM `GRAPH_DATASET.revision`
  WHERE date IS NULL OR date >= TIMESTAMP('2016-01-01')
)
SELECT
  github_origins.origin_url,
  latest_visits.snapshot AS snapshot_id,
  latest_visits.visit_date,
  main_branches.branch_name,
  main_branches.revision_id,
  revisions.directory_id,
  github_origins.github_default_branch
FROM github_origins
JOIN latest_visits
  ON latest_visits.origin = github_origins.origin_id
JOIN main_branches
  ON main_branches.snapshot_id = latest_visits.snapshot
JOIN revisions
  ON revisions.revision_id = main_branches.revision_id;
