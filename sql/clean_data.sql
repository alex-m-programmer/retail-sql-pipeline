CREATE OR REPLACE TABLE 'retail-analytics-494014.raw_data.online_retail_cleaned' AS
SELECT 
  InvoiceNo,
  StockCode,
  Description,
  Quantity,
  InvoiceDate,
  UnitPrice,
  CustomerID,
  Country
FROM 
  'retail-analytics-494014.raw_data.online_retail_raw'
WHERE 
  CustomerID IS NOT NULL 
  AND Quantity > 0 
  AND UnitPrice > 0;