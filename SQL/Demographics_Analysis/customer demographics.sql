SELECT * FROM demographic_data

SELECT * FROM demographic_data LIMIT 15;

SELECT COUNT(*) AS num_null_values
FROM demographic_data
WHERE ip_address IS null;

SELECT COUNT(*) AS num_null_values
FROM demographic_data
WHERE fullname IS null;

SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE ip_address IS NULL AND instore = 1;

SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE fullname IS NULL;
SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE region IS NULL;
SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE instore IS NULL;
SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE age IS NULL;
SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE items IS NULL;
SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE amount IS NULL;

SELECT SUM(amount) AS sales_by_region FROM demographic_data GROUP BY region;

SELECT SUM(amount) AS total_sales FROM demographic_data; 

SELECT SUM(amount) AS sales_by_channel FROM demographic_data GROUP BY instore;

SELECT SUM(amount) AS sales_by_channel FROM demographic_data WHERE instore = 0 / SUM(amount);

SELECT DISTINCT region from demographic_data;

SELECT region, round(AVG(age),2) AS avg_age, ROUND(CAST (SUM(amount) AS numeric),2) AS sales_by_region, SUM(items) AS units_sold , ROUND(CAST (SUM(amount) / SUM(items) as numeric),2) AS avg_selling_price
FROM demographic_data GROUP BY region ORDER BY region;


SELECT ROUND(CAST (SUM(amount) AS numeric),2) AS total_sales, round(AVG(age),2) AS avg_age, SUM(items) AS units_sold, ROUND(CAST (SUM(amount) / SUM(items) as numeric),2) AS avg_selling_price FROM demographic_data;

SELECT instore, ROUND(CAST (SUM(amount) AS numeric),2) AS sales_by_channel, round(AVG(age),2) AS avg_age, SUM(items) AS units_sold, ROUND(CAST (SUM(amount) / SUM(items) as numeric),2) AS avg_selling_price 
FROM demographic_data GROUP BY instore;

SELECT instore, ROUND(CAST (SUM(amount) / SUM(items) as numeric),2) AS avg_selling_price, ROUND(AVG(age),2) AS avg_age, ROUND(CAST(SUM(amount) as numeric),2) AS Total_Spend,
CASE WHEN age < 18 THEN 'Under 18'
	 WHEN age BETWEEN 18 AND 29 THEN '18 - 29'
     WHEN age BETWEEN 30 AND 39 THEN '30 - 39'
     WHEN age BETWEEN 40 AND 49 THEN '40 - 49'
     WHEN age BETWEEN 50 AND 59 THEN '50 - 59'
	 WHEN age >=60 THEN 'Over 60s'
END AS AGE_RANGE
FROM demographic_data
GROUP BY AGE_RANGE,instore ORDER BY AGE_RANGE, instore;

ALTER TABLE demographic_data 
ALTER COLUMN instore TYPE varchar(30) USING instore::varchar;
UPDATE demographic_data SET instore = 'Offline' WHERE instore = '0';
UPDATE demographic_data SET instore = 'Online' WHERE instore = '1';

SELECT region, instore, ROUND(CAST (SUM(amount) / SUM(items) as numeric),2) AS avg_selling_price, ROUND(AVG(age),2) AS avg_age, ROUND(CAST(SUM(amount) as numeric),2) AS Total_Spend,
CASE WHEN age < 18 THEN 'Under 18'
	 WHEN age BETWEEN 18 AND 29 THEN '18 - 29'
     WHEN age BETWEEN 30 AND 39 THEN '30 - 39'
     WHEN age BETWEEN 40 AND 49 THEN '40 - 49'
     WHEN age BETWEEN 50 AND 59 THEN '50 - 59'
	 WHEN age >=60 THEN 'Over 60s'
END AS AGE_RANGE
FROM demographic_data
GROUP BY region, instore, AGE_RANGE ORDER BY region, instore, AGE_RANGE;