-- Databricks notebook source
-- MAGIC %md 
-- MAGIC # Mounting to the bucket

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC ls /mnt/lahari-bigdata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Mounting to the Project folder 

-- COMMAND ----------

-- MAGIC
-- MAGIC %fs
-- MAGIC
-- MAGIC ls /mnt/lahari-bigdata/fall2022/project/nypd-arrests-data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Bronze Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #https://spark.apache.org/docs/latest/sql-data-sources-csv.html
-- MAGIC
-- MAGIC
-- MAGIC df=spark.read.option("delimiter", ";").option("header", "true").csv("dbfs:/mnt/lahari-bigdata/fall2022/project/nypd-arrests-data/nypd-big-data-arrests-data.csv.bz2/")
-- MAGIC df.show(truncate=False)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC
-- MAGIC #https://ganeshchandrasekaran.com/how-to-save-the-spark-data-frame-as-an-external-table-in-databricks-6e7ce0ab12cc
-- MAGIC
-- MAGIC df.write.format("csv").option("path","dbfs:/mnt/lahari-bigdata/fall2022/project/nypd-arrests-data/bronze/").saveAsTable("projectbronze")

-- COMMAND ----------

select * from projectbronze limit 5;

-- COMMAND ----------

select count(*) from projectbronze;

-- COMMAND ----------

describe extended projectbronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating Bronze Delta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Removed two columns-Lon_Lat and Law_CT_CD(because similiar ones exist already)
-- MAGIC ## Changed the datatypes of 9 columns during the delta creation itself

-- COMMAND ----------


CREATE TABLE projectdelta (
arrest_id STRING, 
arrest_date date,
arrest_boro string,
pd_cd int,
ky_cd int,
pd_desc string ,
offense_desc string,
law_code string,
arrest_precinct int,
jurisdiction int,
perp_age string,
perp_sex string,
perp_race string,
x_coord double,
y_coord double,
latitude double,
longitude double
)
LOCATION 'dbfs:/mnt/lahari-bigdata/fall2022/project/nypd-arrests-data/bronze_delta/'


-- COMMAND ----------

INSERT INTO projectdelta select 
arrest_key,
to_date(ARREST_DATE,'M/d/y'),
arrest_boro,
pd_cd ,
ky_cd,
PD_DESC, 
OFNS_DESC,
LAW_CODE,
ARREST_PRECINCT,
JURISDICTION_CODE,
AGE_GROUP,
PERP_SEX,
PERP_RACE,
X_COORD_CD,
Y_COORD_CD,
Latitude,
Longitude
from projectbronze;

-- COMMAND ----------

select * from projectdelta limit 5;

-- COMMAND ----------

select count(*) from projectdelta;

-- COMMAND ----------

describe extended projectdelta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Validation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Checking the number of null values in each column

-- COMMAND ----------

select
    sum(case when arrest_id is null then 1 else 0 end) as arrest_id,
    sum(case when arrest_date is null then 1 else 0 end) as arrest_date,
    sum(case when arrest_boro is null then 1 else 0 end) as arrest_boro,
    sum(case when pd_cd is null then 1 else 0 end) as pd_cd,
    sum(case when ky_cd is null then 1 else 0 end) as ky_cd,
    sum(case when pd_desc is null then 1 else 0 end) as pd_desc,
    sum(case when offense_desc is null then 1 else 0 end) as offense_desc,
    sum(case when law_code is null then 1 else 0 end) as law_code,
    sum(case when arrest_precinct is null then 1 else 0 end) as arrest_precinct ,
    sum(case when jurisdiction is null then 1 else 0 end) as jurisdiction,
    sum(case when perp_age is null then 1 else 0 end) as perp_age,
    sum(case when perp_race is null then 1 else 0 end) as perp_race,
    sum(case when x_coord is null then 1 else 0 end) as x_coord,
    sum(case when y_coord is null then 1 else 0 end) as y_coord,
    sum(case when latitude is null then 1 else 0 end) as latitude,
    sum(case when longitude is null then 1 else 0 end) as longitude
    
from projectdelta;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Cleaning

-- COMMAND ----------

select * from projectdelta where x_coord is NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## We can see that there exists only one row where x_coord, y_coord, latitude,longitude are null.

-- COMMAND ----------

delete from projectdelta where x_coord is NULL;

-- COMMAND ----------

select * from projectdelta limit 5;

-- COMMAND ----------

select count(*) from projectdelta where ky_cd is NULL and pd_desc is NULL and offense_desc is NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Above we can see that we 9169 rows where ky_cd , offense_desc, and pd_desc are null so we drop them in below

-- COMMAND ----------

delete from projectdelta where ky_cd is null and pd_desc is null and offense_desc is null;

-- COMMAND ----------

select
    sum(case when arrest_id is null then 1 else 0 end) as arrest_id,
    sum(case when arrest_date is null then 1 else 0 end) as arrest_date,
    sum(case when arrest_boro is null then 1 else 0 end) as arrest_boro,
    sum(case when pd_cd is null then 1 else 0 end) as pd_cd,
    sum(case when ky_cd is null then 1 else 0 end) as ky_cd,
    sum(case when pd_desc is null then 1 else 0 end) as pd_desc,
    sum(case when offense_desc is null then 1 else 0 end) as offense_desc,
    sum(case when law_code is null then 1 else 0 end) as law_code,
    sum(case when arrest_precinct is null then 1 else 0 end) as arrest_precinct ,
    sum(case when jurisdiction is null then 1 else 0 end) as jurisdiction,
    sum(case when perp_age is null then 1 else 0 end) as perp_age,
    sum(case when perp_race is null then 1 else 0 end) as perp_race,
    sum(case when x_coord is null then 1 else 0 end) as x_coord,
    sum(case when y_coord is null then 1 else 0 end) as y_coord,
    sum(case when latitude is null then 1 else 0 end) as latitude,
    sum(case when longitude is null then 1 else 0 end) as longitude
    
from projectdelta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## By deleting nulls in ky_cd, pd_desc , offense_desc we can see that pd_cd,law_code nulls are also removed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Deleting nulls in other columns(arrest_boro, jurisdiction, perp_age)

-- COMMAND ----------

delete from projectdelta where arrest_boro is NULL;

-- COMMAND ----------

delete from projectdelta where jurisdiction is NULL;

-- COMMAND ----------

delete from projectdelta where perp_age is NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Final Validation once again

-- COMMAND ----------

select
    sum(case when arrest_id is null then 1 else 0 end) as arrest_id,
    sum(case when arrest_date is null then 1 else 0 end) as arrest_date,
    sum(case when arrest_boro is null then 1 else 0 end) as arrest_boro,
    sum(case when pd_cd is null then 1 else 0 end) as pd_cd,
    sum(case when ky_cd is null then 1 else 0 end) as ky_cd,
    sum(case when pd_desc is null then 1 else 0 end) as pd_desc,
    sum(case when offense_desc is null then 1 else 0 end) as offense_desc,
    sum(case when law_code is null then 1 else 0 end) as law_code,
    sum(case when arrest_precinct is null then 1 else 0 end) as arrest_precinct ,
    sum(case when jurisdiction is null then 1 else 0 end) as jurisdiction,
    sum(case when perp_age is null then 1 else 0 end) as perp_age,
    sum(case when perp_race is null then 1 else 0 end) as perp_race,
    sum(case when x_coord is null then 1 else 0 end) as x_coord,
    sum(case when y_coord is null then 1 else 0 end) as y_coord,
    sum(case when latitude is null then 1 else 0 end) as latitude,
    sum(case when longitude is null then 1 else 0 end) as longitude
    
from projectdelta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Following command shows the count of rows after clearing NULL values

-- COMMAND ----------


select count(*) from projectdelta;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Total 9201 null records are cleaned out of 5.2 million rows

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creation of Silver Table

-- COMMAND ----------

CREATE TABLE projectsilver (
arrest_id STRING, 
arrest_date date,
arrest_boro string,
pd_cd int,
ky_cd int,
pd_desc string ,
offense_desc string,
law_code string,
arrest_precinct int,
jurisdiction int,
perp_age string,
perp_sex string,
perp_race string,
x_coord double,
y_coord double,
latitude double,
longitude double
)
LOCATION 'dbfs:/mnt/lahari-bigdata/fall2022/project/nypd-arrests-data/silver/'


-- COMMAND ----------

insert into projectsilver select * from projectdelta;

-- COMMAND ----------

select count(*) from projectsilver;



-- COMMAND ----------

describe extended projectsilver;


-- COMMAND ----------

select * from projectsilver limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##  5 Queries on Silver table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Subquery

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The following is the subquery to retrieve description of crime and other details of MinorMale(under 18 years age) Arrests whose borough of arrest is Brooklyn('K') and arrest date is between September 2020 and December 2020

-- COMMAND ----------

SELECT arrest_id,arrest_date,arrest_boro,perp_age,perp_sex,pd_desc FROM projectsilver WHERE arrest_date between '2020-11-01' and '2020-12-31' and arrest_id IN (SELECT arrest_id FROM projectsilver WHERE perp_age = '<18' and perp_sex='M' and arrest_boro='K');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SQL Functions (Count and groupby)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The following is a query to get the count of Asians/Pacific Islanders involved in each offense.

-- COMMAND ----------

select offense_desc , count(perp_race) as count_asians From projectsilver where perp_race='ASIAN / PACIFIC ISLANDER' group by offense_desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The following is a query for printing the number of Females who have committed the Rape Crime

-- COMMAND ----------

WITH my_cte AS (
  SELECT offense_desc, perp_sex
  FROM projectsilver
)
SELECT count(perp_sex) as count_female_rapists
FROM my_cte
WHERE offense_desc="RAPE" and perp_sex="F";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## LEAD

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The following query is for retrieving the information of Female UNKNOWN race crimes whose age is greater than 65 ordered by crime arrest dates

-- COMMAND ----------

SELECT 
	arrest_id,
	offense_desc,
    perp_race,
	perp_age,
    perp_sex,
    arrest_date,
	LEAD(arrest_date, 1) OVER (ORDER BY arrest_date) AS next_arrest_date
FROM 
	projectsilver
where
   perp_race="UNKNOWN" and perp_sex="F" and perp_age="65+";



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Row_number

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### The following is a query to get data of above 65 age Female arrests in the order of their internal classification code(pd_cd)

-- COMMAND ----------

SELECT 
  arrest_id,pd_desc, offense_desc, pd_cd, perp_age, perp_sex,ROW_NUMBER() OVER( Partition by offense_desc ORDER BY pd_cd ASC) AS row_number
FROM projectsilver
WHERE perp_age="65+" and perp_sex="F";

-- COMMAND ----------


