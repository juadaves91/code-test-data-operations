# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <div style="background-color:#89afc7;height:230px;color:white">
# MAGIC <div style="padding-left: 10px;float:left;width:50%;height:200px;color:white">
# MAGIC <h2>Data Engineer Test</h2>
# MAGIC 
# MAGIC <b>Juan David Escobar Escobar.</b>
# MAGIC * Oct 2022, Medellin, Colombia.
# MAGIC * https://www.linkedin.com/in/jdescobar/
# MAGIC * https://github.com/juadaves91/unir-tfm-alzheimer-diagnostic-deep-learning
# MAGIC </div>
# MAGIC 
# MAGIC <div style="float:right;width:50%;height:200px">
# MAGIC   <div style="float:right;padding-top: 15px;padding-right: 10px">
# MAGIC <img style="border-radius: 50%" src="https://media-exp1.licdn.com/dms/image/C5603AQHhxR-QKjuTYw/profile-displayphoto-shrink_200_200/0/1661897414952?e=1671062400&v=beta&t=QFDVjmUPM-8UyURqIvppyoLnrgZS1LJDmfskgCSDDzU">
# MAGIC     </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <p>
# MAGIC Linux, Bash</br>
# MAGIC You need to run a hive query for a sequence of 30 dates (all dates in September 2021)..</br>
# MAGIC The size of the table data and cluster bandwidth require that the query be run for each date individually. The query string will accept 'DATE' as an argument, as follows:</br>
# MAGIC HQL_QUERY="select utc_date, sum(1) as num_rows from my_table where utc_date = '${DATE}' group by utc_date"</br>
# MAGIC Using bash, write a script which will execute this query for all dates in September 2021 and store the result to a single file. The file will have 2 columns and up to 30 rows.</br>
# MAGIC </p>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS test;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS test.sequencial_dates;
# MAGIC CREATE TABLE IF NOT EXISTS test.sequencial_dates AS
# MAGIC WITH dates AS (
# MAGIC     SELECT date_add("2021-09-01", exp_days.pos) as utc_date
# MAGIC     FROM (SELECT posexplode(split(repeat(",", 29), ","))) exp_days
# MAGIC ),dates_expanded AS (
# MAGIC   SELECT
# MAGIC     utc_date   
# MAGIC   FROM dates
# MAGIC )
# MAGIC SELECT * FROM dates_expanded;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM test.sequencial_dates;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT concat_ws(',', collect_set(s.utc_date)) FROM (SELECT * FROM test.sequencial_dates AS dt)s;

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC #!/bin/bash
# MAGIC 
# MAGIC # creating sequencial table
# MAGIC hive -S -e "DROP TABLE IF EXISTS test.sequencial_dates; CREATE TABLE IF NOT EXISTS test.sequencial_dates AS WITH dates AS ( SELECT date_add('2021-09-01', exp_days.pos) as utc_date FROM (SELECT posexplode(split(repeat(',', 29), ','))) exp_days ),dates_expanded AS ( SELECT utc_date FROM dates ) SELECT * FROM dates_expanded;"
# MAGIC 
# MAGIC # creating list of the sequencial table (September 2021)
# MAGIC DATE=$(hive -S -e "SELECT concat_ws(',', collect_set(s.utc_date)) FROM (SELECT * FROM test.sequencial_dates AS dt)s;")
# MAGIC 
# MAGIC # saving the result in file
# MAGIC hive -e "select utc_date, sum(1) as num_rows from my_table where utc_date in '${DATE}' group by utc_date;" > /sequencial_dates.txt

# COMMAND ----------

df = spark.read.format('text').load(
  '/sequencial_dates.txt',
  header=True,
  inferSchema=True
)
dbutils.data.summarize(df)