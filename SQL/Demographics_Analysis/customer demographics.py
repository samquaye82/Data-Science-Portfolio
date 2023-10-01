import psycopg2

hostname = 'localhost'
database = 'postgres'
username = 'postgres'  
pwd = '$uccessIsmydestiny365!'
port_id = 5432
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host = hostname,
        dbname = database,
        user =username,
        password = pwd,
        port = port_id)

    conn.autocommit = True
    cursor = conn.cursor()

    create_script = ''' CREATE TABLE IF NOT EXISTS demographic_data (
                            index int PRIMARY KEY,
                            fullname varchar(48) NOT NULL,
                            ip_address varchar(30),
                            region int,
                            instore int,
                            age int,
                            items int,
                            amount float)'''
    
    cursor.execute(create_script)

    importCSV = '''COPY demographic_data(index,fullname, ip_address, region, instore, age, items, amount) FROM '/Volumes/LaCie 2/Q15 Brands/Undeniable Developer/SQL/Demographic_Data_Orig.csv'
    DELIMITER ','
    CSV HEADER;'''
    cursor.execute(importCSV)

    first_records = '''SELECT * FROM demographic_data LIMIT 15;'''
    cursor.execute(first_records)

    columns = ['fullname', 'ip_address', 'region', 'instore', 'age', 'items', 'amount']
    for col in columns:
        query = f"SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE {col} IS NULL;"
    
    cursor.execute(query)

    total_sales = '''SELECT SUM(amount) AS sales_by_region FROM demographic_data;'''
    cursor.execute(total_sales)

    sales_by_region = '''SELECT SUM(amount) AS sales_by_region FROM demographic_data GROUP BY region;'''
    cursor.execute(sales_by_region)


except Exception as error:
    print("Error while connecting to PostgreSQL",error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()


##--Exploratory Data Analysis--##

# What does the dataset look like?

'''
Data types of all the columns:

Index
Full Name - String (varchar)
ip_address - String (varchar)
Region - Integer (int)
In Store - Integer (int)
Age - Integer (int)
Items - Integer (int)
Amount - Float (double precision)
'''

# How many empty values are in the dataset?

# Basic query is as follows:

null_values = '''SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE ip_address IS null;'''

# Make it a for loop to do all columns at once

columns = ['fullname', 'ip_address', 'region', 'instore', 'age', 'items', 'amount']
for col in columns:
    query = f"SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE {col} IS NULL;"

''' There are no missing values in the dataset apart from 40,000 null values in the ip_address column. 
    Looking a bit deeper this is probably due to the nature of the purchase, online purchase vs offline'''

# Basic feature engineering
# Update the values in the instore column to be Online or Offline

'''
ALTER TABLE demographic_data 
ALTER COLUMN instore TYPE varchar(30) USING instore::varchar;
UPDATE demographic_data SET instore = 'Offline' WHERE instore = '0';
UPDATE demographic_data SET instore = 'Online' WHERE instore = '1';
'''

# Specifically for ip_address confirm that the missing values are only due to offline purchases.

ip_address_check = '''SELECT COUNT(*) AS num_null_values FROM demographic_data WHERE ip_address IS NULL AND instore = 1;'''

''' No. of missing values in ip_address column is solely due to those sales being taken offline rather than being an online purchase.'''

# How many unique regions are there?
unique_regions= ''' SELECT DISTINCT region from demographic_data;'''

'''Now let's see what the data tells us...'''

# Overall Sales Overview

'''
SELECT ROUND(CAST (SUM(amount) AS numeric),2) AS total_sales, round(AVG(age),2) AS avg_age, SUM(items) AS units_sold, ROUND(CAST (SUM(amount) / SUM(items) as numeric),2) AS avg_selling_price FROM demographic_data;
'''

'''
Total sales = 66,873,573.57
Average price per item sold = 185.55
Average age per customer = 45.76 (46 years old)

'''

# Sales Overview instore vs online

'''
SELECT instore, ROUND(CAST (SUM(amount) AS numeric),2) AS sales_by_channel, round(AVG(age),2) AS avg_age, SUM(items) AS units_sold, ROUND(CAST (SUM(amount) / SUM(items) as numeric),2) AS avg_selling_price 
FROM demographic_data GROUP BY instore;
'''

'''
Offline Sales = 35,902,788.29
Onine Sales =   30,970,785.28
'''

# Sales Overview by region

'''
SELECT region, AVG(age) AS avg_age, SUM(amount) AS sales_by_region, SUM(items) AS units_sold , SUM(amount) / SUM(items) AS avg_selling_price
FROM demographic_data GROUP BY region ORDER BY region;

'''

'''
Region 1 = 11,922,583.85
Region 2 =  5,042,183.92
Region 3 = 16,523,453.47
Region 4 = 33,385,352.32

'''

# AUR by age group in-store vs non in store

'''
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

'''
#--CONCLUSIONS--#

'''
Offline sales drive 54% of overall sales, driven by consumers that are 3 years older than the overall average (48 vs 45), but these customers buy products with an average selling price $13 above the overall average. 
18-29's spend 1.5x more per items offline than they do online.
40-49's are the only age group where online sales outstrip offline sales
Over 60's are the only age group who spend more per item online than they do offline.

Region 4 has the youngest clientele who love to buy expensive things and lots of it. 
They represent 50% of all sales, are 7 years younger than the overall average and spend over $100 more per item.
This is primarily driven by offline sales by 18-29's.
On the other hand region 2 is the opposite. Oldest clientele at 57 years old (11 years above average). They account for < 10% of total sales and spend 3x less per item than the average.
This is mostly due to offline sales by the over 60's.

Using these findings would be a good starting point to decide the overall business direction. 
Depending on unavailable growth information and the directional vision of the business, identifying the psychographic reasons behind the purchase decisions of customers
in both group 2 and 4 to decide which customers are more valuable/important.

'''

# extract above average spending customers and build a demographic profile of them vs. rest of the dataset

