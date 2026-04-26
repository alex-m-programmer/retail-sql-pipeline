CREATE OR REPLACE TABLE `retail-analytics-494014.silver.online_retail_cleaned` 
PARTITION BY DATE(InvoiceDate)
CLUSTER BY Country AS
SELECT *
FROM retail-analytics-494014.bronze.online_retail_raw
WHERE CustomerID IS NOT NULL AND Quantity > 0 AND UnitPrice > 0;