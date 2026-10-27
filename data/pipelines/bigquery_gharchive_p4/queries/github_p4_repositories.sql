SELECT
  repo_name,
  COUNT(*) AS p4_file_count,
  ARRAY_AGG(path IGNORE NULLS LIMIT 100) AS example_paths
FROM `bigquery-public-data.github_repos.files`
WHERE REGEXP_CONTAINS(LOWER(path), r'(^|/)[^/]+\.p4$')
GROUP BY repo_name;
