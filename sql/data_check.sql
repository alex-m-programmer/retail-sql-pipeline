WITH base AS (
  SELECT *
  FROM `{PROJECT_ID}.{DATASET_GOLD}.{TABLE_PERFORMANCE}`
),

metrics AS (
  SELECT
    COUNT(DISTINCT Country) AS country_count,
    SUM(CASE WHEN TotalRevenue < 0 OR Customer_Count <= 0 THEN 1 ELSE 0 END) AS invalid_rows,
    SUM(CASE WHEN AvgOrderValue > 5000 THEN 1 ELSE 0 END) AS outlier_rows
  FROM base
)

SELECT
  ARRAY_LENGTH(issue_types) AS issue_count,
  ARRAY_TO_STRING(issue_types, ', ') AS issue_types
FROM (
  SELECT ARRAY(
    SELECT issue
    FROM UNNEST([
      CASE WHEN invalid_rows > 0 THEN 'Negative Revenue / Invalid Customers' END,
      CASE WHEN country_count < 36 THEN 'Missing Key Markets' END,
      CASE WHEN outlier_rows > 0 THEN 'Extreme Outliers' END
    ]) AS issue
    WHERE issue IS NOT NULL
  ) AS issue_types
  FROM metrics
);