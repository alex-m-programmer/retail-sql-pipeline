SELECT COUNT(*) as issue_count
FROM `retail-analytics-494014.gold.country_sales_performance`
WHERE TotalRevenue < 0 OR Customer_Count <= 0;