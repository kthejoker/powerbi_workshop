-- Databricks notebook source
use catalog powerbi_demos;
create schema if not exists nytaxi;
use nytaxi;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Taxi Data
-- MAGIC https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW _ratecode USING CSV
OPTIONS (path 'dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_rate_code.csv', header "true");

CREATE OR REPLACE TABLE ratecode COMMENT "Rate codes for various NYC Taxi rides" AS
select * from _ratecode

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW _paymenttype USING CSV
OPTIONS (path 'dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv', header "true");

CREATE OR REPLACE TABLE paymenttype AS
select * from _paymenttype

-- COMMAND ----------

-- MAGIC %sh wget -nc https://raw.githubusercontent.com/jodb/DatabricksAndAzureMapsWorkshop/main/epsode5/NYTaxiZoneswithLatLong.csv -P /dbfs/

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW _taxizones USING CSV
OPTIONS (path 'dbfs:/tmp/NYTaxiZoneswithLatLong.csv', header 'true');

 CREATE OR REPLACE TABLE taxizone AS
 select * from _taxizones

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW _date
AS 
WITH calendarDate AS (
  select
    explode(
      sequence(
        to_date('2016-01-01'),
        to_date('2019-12-31'),
        interval 1 day
      )
    ) AS calendarDate
)
--SELECT * FROM calendarDate
select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt,
  CalendarDate,
  year(calendarDate) AS CalendarYear,
  date_format(calendarDate, 'MMMM') as CalendarMonth,
  month(calendarDate) as MonthOfYear,
  date_format(calendarDate, 'EEEE') as CalendarDay,
  dayofweek(calendarDate) as DayOfWeek,
  weekday(calendarDate) + 1 as DayOfWeekStartMonday,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as IsWeekDay,
  dayofmonth(calendarDate) as DayOfMonth,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as IsLastDayOfMonth,
  dayofyear(calendarDate) as DayOfYear,
  weekofyear(calendarDate) as WeekOfYearIso,
  quarter(calendarDate) as QuarterOfYear
from
  calendarDate;
  
CREATE OR REPLACE TABLE date AS 
SELECT * from _date

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW  _rawyellowtrips USING CSV
OPTIONS (pathGlobFilter "yellow_tripdata_201[789]-*.csv.gz", path "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/", header "true", inferSchema "true");       
CREATE OR REPLACE TABLE rawyellowtrips  AS   SELECT 
VendorID,
to_date(tpep_pickup_datetime) as pickupDate,
hour(to_timestamp(tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss")) AS pickupHour,
to_timestamp(tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss") AS pickup,
to_timestamp(tpep_dropoff_datetime, "yyyy-MM-dd HH:mm:ss") AS dropoff,
(cast(to_timestamp(tpep_dropoff_datetime, "yyyy-MM-dd HH:mm:ss") as long) 
- cast(to_timestamp(tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss") as long) ) / 60.0 AS trip_duration_minutes,
store_and_fwd_flag,
RatecodeID,
PULocationID,
DOLocationID,
payment_type,
CAST(passenger_count AS int) AS passenger_count,
CAST(trip_distance AS float) AS trip_distance,
CAST(fare_amount AS float) AS fare_amount,
CAST(extra AS float) AS extra,
CAST(mta_tax AS float) AS mta_tax,
CAST(tip_amount AS float) AS tip_amount,
CAST(tolls_amount AS float) AS tolls_amount,
CAST(total_amount AS float) AS total_amount
FROM _rawyellowtrips

-- COMMAND ----------

CREATE OR REPLACE TABLE dailytrips as
select 
  pickupdate,
  tz.borough,
  sum(trip_distance) as TotalDistance,
  sum(total_amount) as TotalFare,
  count(1) as TripCount
from
rawyellowtrips ryt
left join
taxizone tz
on ryt.dolocationid = tz.locationid
group by
pickupdate,
tz.borough

-- COMMAND ----------

OPTIMIZE dailytrips;
OPTIMIZE rawyellowtrips;
OPTIMIZE date;
OPTIMIZE taxizone;
OPTIMIZE paymenttype;
OPTIMIZE ratecode;

-- COMMAND ----------

ANALYZE TABLE dailytrips COMPUTE STATISTICS;
ANALYZE TABLE rawyellowtrips COMPUTE STATISTICS;
ANALYZE TABLE date COMPUTE STATISTICS;
ANALYZE TABLE taxizone COMPUTE STATISTICS;
ANALYZE TABLE paymenttype COMPUTE STATISTICS;
ANALYZE TABLE ratecode COMPUTE STATISTICS;

-- COMMAND ----------

CREATE OR REPLACE TABLE rawyellowtrips_lc CLUSTER BY (pickupdate, pulocationid, dolocationid) as 
select * from powerbi_demos.nytaxi.rawyellowtrips

-- COMMAND ----------

optimize powerbi_demos.nytaxi.rawyellowtrips_lc;

-- COMMAND ----------


