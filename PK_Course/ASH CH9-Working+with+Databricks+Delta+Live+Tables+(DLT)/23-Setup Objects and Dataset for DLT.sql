-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####1. Create a multinode shared mode cluster to work with DLT generated tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Cleanup previous runs

-- COMMAND ----------

-- MAGIC %run ../utils/cleanup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Setup the catalog and external location

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS DLT_CAT;
CREATE DATABASE IF NOT EXISTS DLT_CAT.DB09;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS external_src
URL 'abfss://section09-src@databricks00pk00course.dfs.core.windows.net/'
WITH (CREDENTIAL `cred-external-location`);


-- COMMAND ----------


CREATE EXTERNAL VOLUME IF NOT EXISTS DLT_CAT.DB09.extsource
LOCATION 'abfss://section09-src@databricks00pk00course.dfs.core.windows.net/';


-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS external_landingzone
URL 'abfss://section09-landing-zone@databricks00pk00course.dfs.core.windows.net/'
WITH (CREDENTIAL `cred-external-location`);

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS DLT_CAT.DB09.landingzone
LOCATION 'abfss://section09-landing-zone@databricks00pk00course.dfs.core.windows.net/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Ingest some data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("/Volumes/DLT_CAT/DB09/landingzone/customers/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("/Volumes/DLT_CAT/DB09/landingzone/invoices/")

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/DLT_CAT/DB09/extsource/customers_1.csv /Volumes/DLT_CAT/DB09/external_landingzone/customers

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/DLT_CAT/DB09/extsource/invoices_2021.csv /Volumes/DLT_CAT/DB09/external_landingzone/invoices

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/DLT_CAT/DB09/extsource/invoices_2022.csv /Volumes/DLT_CAT/DB09/external_landingzone/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Create and run the DLT pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Check the final results table

-- COMMAND ----------

select * from DLT_CAT.DB09.daily_sales_uk_2022

-- COMMAND ----------

describe extended DLT_CAT.DB09.daily_sales_uk_2022

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####7. Ingest some more data

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/extsource/customers_2.csv /Volumes/dev/demo_db/landing_zone/customers

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/extsource/invoices_01-06-2022.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/extsource/invoices_02-06-2022.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC cp /Volumes/dev/demo_db/extsource/invoices_03-06-2022.csv /Volumes/dev/demo_db/landing_zone/invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####8. Run the DLT pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####9. Check the results table

-- COMMAND ----------

select * from DLT_CAT.DB09.daily_sales_uk_2022

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####10. Check your SCD2 dimension

-- COMMAND ----------

select * from DLT_CAT.DB09.customers where customer_id=15311

-- COMMAND ----------

 SELECT *
FROM dlt_cat.db09.customers

