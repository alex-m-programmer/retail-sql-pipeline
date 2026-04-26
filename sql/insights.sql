CREATE OR REPLACE TABLE `retail-analytics-494014.gold.country_sales_performance` 
CLUSTER BY Country AS
WITH country_stats AS (
  SELECT Country, CAST(ROUND(SUM(Quantity * UnitPrice), 2) AS NUMERIC) AS TotalRevenue, COUNT(DISTINCT InvoiceNo) AS Transaction_Count, COUNT(DISTINCT CustomerID) AS Customer_Count
  FROM `retail-analytics-494014.silver.online_retail_cleaned`
  GROUP BY 1
)
SELECT *, CAST(ROUND(TotalRevenue / Transaction_Count, 2) AS NUMERIC) AS AvgOrderValue, CAST(ROUND(TotalRevenue / Customer_Count, 2) AS NUMERIC) AS AvgSpendPerCustomer
FROM country_stats
WHERE Customer_Count > 0