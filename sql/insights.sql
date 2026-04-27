CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.{TABLE_PERFORMANCE}` 
CLUSTER BY Country AS
WITH country_stats AS (
  SELECT Country, CAST(ROUND(SUM(Quantity * UnitPrice), 2) AS NUMERIC) AS TotalRevenue, COUNT(DISTINCT InvoiceNo) AS Transaction_Count, COUNT(DISTINCT CustomerID) AS Customer_Count
  FROM `{PROJECT_ID}.{DATASET_SILVER}.{TABLE_CLEANED}`
  GROUP BY 1
)
SELECT *, CAST(ROUND(TotalRevenue / Transaction_Count, 2) AS NUMERIC) AS AvgOrderValue, CAST(ROUND(TotalRevenue / Customer_Count, 2) AS NUMERIC) AS AvgSpendPerCustomer
FROM country_stats
WHERE Customer_Count > 0