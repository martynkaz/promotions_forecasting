SET DateStyle = 'ISO, DMY';
-- importing revenue 1 data
CREATE TABLE revenue1 (date1 TEXT,
					  Week_ID INT,
					  Month_Number INT,
					  Month_ID INT,
					  Year INT,
					  Day_Name VARCHAR(10),
					  Revenue INT);
--importing data through import button on the table					  
SELECT * FROM revenue1;
--changing the date from text to datetime format
ALTER TABLE revenue1
ADD COLUMN date DATE;
SELECT * FROM revenue1;

UPDATE revenue1
SET date = TO_DATE(date1, 'DD/MM/YYYY');

-- removing the date1 column(text) after transforming the date to datetime format
-- and placing date as the first column (this step will be performed and other datasets as well)
-- Create a new table with the desired column order
CREATE TABLE revenue1_new AS
SELECT date,week_id,month_number,month_id,year,day_name,revenue 
FROM revenue1;

select* from revenue1_new;

-- Drop the original table
DROP TABLE revenue1;

-- Rename the new table to the original table name
ALTER TABLE revenue1_new
RENAME TO revenue1;

SELECT * FROM revenue1;

-- check if the date only appears once to make sure we can use it as a primary key for merging
SELECT date, COUNT(*) as date_unique_count
FROM revenue1
GROUP BY date
HAVING COUNT(*) > 1; -- all the dates are unique
--setting date as primary key to keep uniqness and integirty 
ALTER TABLE revenue1
ADD CONSTRAINT PK_revenue1 PRIMARY KEY (date);

-- importing revenue 2 data
CREATE TABLE revenue2 (date1 TEXT,
					  Week_ID INT,
					  Month_Number INT,
					  Month_ID INT,
					  Year INT,
					  Day_Name VARCHAR(10),
					  Revenue INT);
					  
SELECT * FROM revenue2;

ALTER TABLE revenue2
ADD COLUMN date DATE;
SELECT * FROM revenue2;

--changing the date type
UPDATE revenue2
SET date = TO_DATE(date1, 'DD/MM/YYYY');
-- removing date1 column

CREATE TABLE revenue2_new AS
SELECT date,week_id,month_number,month_id,year,day_name,revenue 
FROM revenue2;

select* from revenue2_new;

-- Drop the original table
DROP TABLE revenue2;
-- Rename the new table to the original table name
ALTER TABLE revenue2_new
RENAME TO revenue2;

SELECT * FROM revenue2;

--checking unique date
SELECT date, COUNT(*) as date_unique_count
FROM revenue2
GROUP BY date
HAVING COUNT(*) > 1;--all the data is unique
ALTER TABLE revenue2
ADD CONSTRAINT PK_revenue2 PRIMARY KEY (date);

--importing marketing data
CREATE TABLE marketing_data(date1 text,
						  marketing_spending DECIMAL(8,4),
						  promo VARCHAR(15));
SELECT*from marketing_data;

ALTER TABLE marketing_data
ADD COLUMN date DATE;
select * from marketing_data;
--changing the datetype
UPDATE marketing_data
SET date = TO_DATE(date1, 'DD/MM/YYYY');
SELECT * FROM marketing_data;

--removing the date column
CREATE TABLE marketingnew AS
SELECT date,marketing_spending,promo 
FROM marketing_data;

select* from marketingnew;

-- Drop the original table
DROP TABLE marketing_data;

-- Rename the new table to the original table name
ALTER TABLE marketingnew
RENAME TO marketing_data;

SELECT * FROM marketing_data;

SELECT date, COUNT(*) as date_unique_count
FROM marketing_data
GROUP BY date
HAVING COUNT(*) > 1; --- all the data is unique
ALTER TABLE marketing_data
ADD CONSTRAINT PK_marketing_data PRIMARY KEY (date);
						  					  
-- importing data from the visitors 
CREATE TABLE visitors(date DATE,
					 visitors INT);
					 
SELECT * FROM visitors;

-- checking if date is unique

SELECT date, COUNT(*) as date_unique_count
FROM visitors
GROUP BY date
HAVING COUNT(*) > 1; ---all the dates are unique
ALTER TABLE visitors
ADD CONSTRAINT PK_visitors PRIMARY KEY (date);

-- connecting revenue verticlaly as it is continous data using union to remove the duplicated rows 
CREATE VIEW revenue as(
SELECT * FROM revenue1
UNION 
SELECT * FROM revenue2);

SELECT*FROM revenue;

-- we will join the data with full join, the marketingdata, visitors have the same amount of rows
-- but the revenue1 had three rows empty, but just to make sure all the data is in place 
-- full join will be used to assure all the data even null is there

-- creating a table for the merged data 
CREATE TABLE event_data as (
	SELECT r.date,
        r.week_id, 
        r.month_number, 
        r.month_id,
        r.year,
        r.day_name,
        r.revenue,
        m.marketing_spending,
        m.promo,
        v.visitors
	FROM revenue as r
	FULL JOIN marketing_data as m
	ON r.date = m.date
	FULL JOIN visitors as v
	ON r.date = v.date
	);	
SELECT * FROM event_data;
-- creating a temporary table for data cleaning to avoid modifying the original data set 
-- before making sure the data is correct

CREATE TEMPORARY TABLE cleaning_event_data AS
	(
	SELECT *
	FROM event_data
	);
SELECT * FROM cleaning_event_data
ORDER BY date DESC;


-- check for duplicates
SELECT *
FROM cleaning_event_data
WHERE date IN (
    SELECT date
    FROM cleaning_event_data
    GROUP BY date
    HAVING COUNT(*) > 1
	ORDER BY date DESC
);-- there are four duplicates after joining according to the date but 
-- after investigating the marketing spending the data is the same with marketing spend,promo,visitors
-- but has null in week_id,month_id,month_number,day_name and revenue
-- deleteing the duplicates

DELETE FROM cleaning_event_data
WHERE ctid IN (
    SELECT
        ctid
    FROM (
        SELECT
            ctid,
            ROW_NUMBER() OVER (PARTITION BY date ORDER BY (SELECT NULL)) AS row_num
        FROM cleaning_event_data
        WHERE week_id IS NULL AND month_number IS NULL AND day_name IS NULL AND revenue IS NULL
    ) AS duplicaterowsnull
    WHERE row_num = 1
);

-- cleaning the data
-- checking for null values
SELECT*FROM cleaning_event_data
WHERE NOT(cleaning_event_data is not null);

-- we do not need the month_id, year for our further 
-- analysis this this columns will be deleted (month number will be used for visualisation)
ALTER TABLE cleaning_event_data 
DROP COLUMN month_id,
DROP COLUMN year;

---for visitors the null will be filled with average 
UPDATE cleaning_event_data
SET
	visitors=COALESCE(visitors,(SELECT AVG(visitors)FROM cleaning_event_data WHERE visitors IS NOT NULL)),
	revenue=COALESCE(revenue,(SELECT AVG(revenue)FROM cleaning_event_data WHERE revenue IS NOT NULL))
WHERE visitors IS NULL OR revenue is NULL;

-- filling in marketing spend depeding on the avg spend from speicific 
-- promotion to keep very accurate 
UPDATE cleaning_event_data
SET marketing_spending = (
    CASE
        WHEN promo = 'Promotion Red' THEN (
            SELECT AVG(marketing_spending) FROM cleaning_event_data WHERE promo = 'Promotion Red' AND marketing_spending IS NOT NULL)
        WHEN promo = 'Promotion Blue' THEN (
            SELECT AVG(marketing_spending) FROM cleaning_event_data WHERE promo = 'Promotion Blue' AND marketing_spending IS NOT NULL)
        WHEN promo = 'No Promo' THEN (
            SELECT AVG(marketing_spending) FROM cleaning_event_data WHERE promo = 'No Promo' AND marketing_spending IS NOT NULL)
    END
)
WHERE marketing_spending IS NULL;
-- checking if all the values are filled and if any are still na
SELECT*FROM cleaning_event_data
WHERE NOT(cleaning_event_data is not null); -- all the na values are filled

-- check if the text value are all distinct and the same
SELECT DISTINCT(promo)
FROM cleaning_event_data; -- only three distinct spelling is correct


-- for better visibility marketing spending will be rounded to two decimal points
ALTER TABLE cleaning_event_data
ALTER COLUMN marketing_spending TYPE numeric(8,2);
SELECT*FROM cleaning_event_data;

-- after cleaning our data we can replace our event_data with the cleaned table
CREATE TABLE cleaned_data AS
SELECT * 
FROM cleaning_event_data;

select*from cleaned_data;

-- How much revenue did we generate in total and by campaign?
SELECT 
SUM(revenue) as promo_revenue,
COALESCE(promo, 'Total') AS promo
FROM cleaned_data
GROUP BY rollup(promo)
ORDER BY promo_revenue;

-- Which day we had the highest average visitors?
select day_name,
round(avg(visitors),0) as avg_visitors
from cleaned_data
group by day_name
order by avg_visitors DESC
LIMIT 1; -- Thursdays have the highest avgerage visitors of 2245

-- Which promotion costs us the most?
SELECT
promo,
SUM(marketing_spending) as sum_cost
FROM cleaned_data
GROUP BY promo
ORDER BY sum_cost DESC
LIMIT 1; -- promotion blue cost the company the most 99772.99

-- What is the weekly average revenue, visitors and marketing spend?
-- using already exisitng week id
SELECT
week_id,
ROUND(AVG(revenue),0) as avg_revenue,
ROUND(AVG(visitors),0) as avg_visitors,
ROUND(AVG(marketing_spending),0) as avg_marketing_spending
FROM cleaned_data
GROUP BY week_id
ORDER BY week_id DESC;

-- using a created week column
SELECT
EXTRACT(WEEK FROM date) as week,
ROUND(AVG(revenue),0) as avg_revenue,
ROUND(AVG(visitors),0) as avg_visitors,
ROUND(AVG(marketing_spending),0) as avg_marketing_spending
FROM cleaned_data
GROUP BY week
ORDER BY week DESC;
-- saving the outcome of the query to create a visualisation in python
SELECT*FROM cleaned_data
order by date DESC;

-- could also be done with this query COPY (SELECT * FROM cleaned_data ORDER BY date DESC) TO '/path/...' WITH CSV HEADER;















