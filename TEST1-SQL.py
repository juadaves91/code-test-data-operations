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
# MAGIC You have a database with the following tables:
# MAGIC 
# MAGIC 
# MAGIC  - tblDimDate             - a table of dates, i.e., a calendar
# MAGIC  - tblOrder               - a table of 'Orders', also referred to as Campaigns
# MAGIC  - tblAdvertiserLineItem  - a table of 'Advertiser Line Items' (ALI for short).
# MAGIC  
# MAGIC Each ALI is a component of a campaign.
# MAGIC Therefore, the relation of tblAdvertiserLineItem to tblOrder is many-to-one, with the foriegn key relationship described below.
# MAGIC Use the sample data and schema descriptions below to provide the following queries:

# COMMAND ----------

# DBTITLE 1,Database creation
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test;

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse/test.db/", True)

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/

# COMMAND ----------

# DBTITLE 1,Table Time Dimesion
# MAGIC %sql
# MAGIC /*
# MAGIC Author: Juan Escobar
# MAGIC Creation Date: 05/10/2022
# MAGIC Description: Creation Table Time Dimesion
# MAGIC Documentation: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
# MAGIC */
# MAGIC 
# MAGIC DROP TABLE IF EXISTS test.tblDimDate;
# MAGIC CREATE TABLE IF NOT EXISTS test.tblDimDate AS
# MAGIC WITH dates AS (
# MAGIC     SELECT date_add("2021-01-01", exp_days.pos) as date
# MAGIC     FROM (SELECT posexplode(split(repeat(",", 730), ","))) exp_days
# MAGIC ),dates_expanded AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     year(date) as year,
# MAGIC     month(date) as month,
# MAGIC     day(date) as day,
# MAGIC     date_format(date, "D") as day_of_week
# MAGIC   FROM dates
# MAGIC ),dates_expanded_calculated AS (
# MAGIC   SELECT
# MAGIC     date AS dateDay,
# MAGIC     lpad(month, 2, "0") AS iMonth,
# MAGIC     date_format(date, "MMMM") AS sMonth, 
# MAGIC     year AS iYear,
# MAGIC     date_format(date, "D") AS iYearDay, 
# MAGIC     lpad(date_format(date, "d"), 2, "0") AS iMonthDay, 
# MAGIC     lpad(day, 2, "0") AS iWeekDay,    
# MAGIC     date_format(date, "D") AS sWeekDay,
# MAGIC     date_format(date,'yyyyMM') AS iYearMonth,
# MAGIC     weekofyear(date) - 1 AS iWeek,
# MAGIC     if(((month = 1 AND day = 1 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 1 AND day BETWEEN 1 AND 3)) -- New Year's Day
# MAGIC           OR (month = 1 AND day_of_week = 1 AND day BETWEEN 15 AND 21) -- MLK Jr
# MAGIC           OR (month = 2 AND day_of_week = 1 AND day BETWEEN 15 AND 21) -- President's Day
# MAGIC           OR (month = 5 AND day_of_week = 1 AND day BETWEEN 25 AND 31) -- Memorial Day
# MAGIC           OR ((month = 7 AND day = 4 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 7 AND day BETWEEN 4 AND 6)) -- Independence Day
# MAGIC           OR (month = 9 AND day_of_week = 1 AND day BETWEEN 1 AND 7) -- Labor Day
# MAGIC           OR ((month = 11 AND day = 11 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 11 AND day BETWEEN 11 AND 13)) -- Veteran's Day
# MAGIC           OR (month = 11 AND day_of_week = 4 AND day BETWEEN 22 AND 28) -- Thanksgiving
# MAGIC           OR ((month = 12 AND day = 25 AND day_of_week between 1 AND 5) OR (day_of_week = 1 AND month = 12 AND day BETWEEN 25 AND 27)) -- Christmas
# MAGIC          ,true, false) as us_holiday
# MAGIC   FROM dates_expanded
# MAGIC )
# MAGIC SELECT * FROM dates_expanded_calculated;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC test.tblDimDate;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test.tblDimDate;

# COMMAND ----------

# DBTITLE 1,Query 1
# MAGIC %sql
# MAGIC /*
# MAGIC Author: Juan Escobar
# MAGIC Creation Date: 05/10/2022
# MAGIC Description: Write an SQL query to return all months in the current year for which there are exactly 30 days.
# MAGIC */
# MAGIC 
# MAGIC SELECT sMonth, COUNT(sMonth) AS countDaysByMonth
# MAGIC FROM test.tblDimDate
# MAGIC WHERE iYear = YEAR(CURRENT_DATE)
# MAGIC GROUP BY sMonth
# MAGIC HAVING countDaysByMonth = 30

# COMMAND ----------

# DBTITLE 1,Query 2
# MAGIC %sql
# MAGIC /*
# MAGIC Author: Juan Escobar
# MAGIC Creation Date: 05/10/2022
# MAGIC Description: tblDimDate should have one row (one date) for every date between the first date and the last date in the table. Write a SQL query to determine how many dates are missing, if any,
# MAGIC between the first date and last date. You do not need to supply a list of the missing dates.
# MAGIC 
# MAGIC MIN: 2021-01-01
# MAGIC MAX: 2023-01-01
# MAGIC TOTAL DAYS = (365 * 2) + 1 = 730 
# MAGIC */
# MAGIC 
# MAGIC SELECT COUNT(DISTINCT dateDay),
# MAGIC        COUNT(dateDay),
# MAGIC        IF(COUNT(DISTINCT dateDay) != COUNT(dateDay),'TRUE','FALSE') as IF_MISSING_DATES
# MAGIC FROM test.tblDimDate
# MAGIC WHERE dateDay >= '2021-01-01' AND dateDay <= '2023-01-01'

# COMMAND ----------

# MAGIC %md
# MAGIC <h2>Helper Funtions</h2>

# COMMAND ----------

from random import randrange
from datetime import timedelta

def random_date(start, end):
  '''
  This auxiliar function will return a random datetime between two datetime 
  objects.

  Args:
      start (datetime): Initial date.
      end (datetime): End date.

  Returns:
      datetime: random date between Initial and end date.
  '''  
  delta = end - start
  int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
  random_second = randrange(int_delta)
  return start + timedelta(seconds=random_second)

# COMMAND ----------

from random import randrange
from datetime import timedelta

def dates_generator(init_date, end_date):
  '''
  This auxiliar function will return a random datetimes between two datetime 
  objects.

  Args:
      init_date (datetime): Initial date.
      end_date (datetime): End date.

  Returns:
      datetime: datestart, dateend, datecreate, dateupdated.
  '''  
  import random
  from dateutil.relativedelta import relativedelta
  
  datecreate = random_date(init_date, end_date)
  dateupdated = datecreate + timedelta(minutes = random.randint(1, 1440))
  dateupdated = random.choice([None, dateupdated]) 
  
  datestart = None
  if (dateupdated is not None):
    datestart = dateupdated + relativedelta(months =+ random.randint(1, 3))
    datestart = random.choice([None, dateupdated])

  dateend = None
  if (datestart is not None):
    dateend = datestart + relativedelta(months =+ random.randint(1, 3))
    dateend = random.choice([None, dateend])
    
    
  return datestart, dateend, datecreate, dateupdated

# COMMAND ----------

def set_status(datestart, dateend, datecreate, dateupdated):
  '''
  This auxiliar function will return a status according to dates
  set status ('ACTIVE', 'INACTIVE', 'PROPOSED', 'ENDED').

  Args:
      datestart (datetime): Initial date.
      dateend (datetime): End date.
      datecreate (datetime): Creation date.
      dateupdated (datetime): Last updated date.

  Returns:
      str: sstatus 
  '''  
  if (datecreate is not None) and (dateupdated is None) and (datestart is None) and (dateend is None):
    sstatus = 'PROPOSED'  
  elif (datecreate is not None) and (dateupdated is not None) and (datestart is None) and (dateend is None):
    sstatus = 'INACTIVE'    
  elif (datestart is not None) and (dateend is None):
    sstatus = 'ACTIVE'      
  elif (dateend is not None):
    sstatus = 'ENDED'
  return sstatus

# COMMAND ----------

# MAGIC %md
# MAGIC <h2>Data Generation (Orders)</h2>

# COMMAND ----------

# MAGIC %md
# MAGIC <b>Summary</b>
# MAGIC 
# MAGIC * Author: Juan Escobar
# MAGIC * Creation Date: 05/10/2022
# MAGIC * Description: Populate Table tblOrder
# MAGIC * Documentation: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=82706456#LanguageManualTypes-tinyint </br>
# MAGIC                  https://spark.apache.org/docs/latest/sql-ref-datatypes.html</br>
# MAGIC                  https://sparkbyexamples.com/pyspark/pyspark-timestamp-difference-seconds-minutes-hours/</br>
# MAGIC 
# MAGIC <b>Ex row:</b>
# MAGIC 
# MAGIC * id: 12 ('auto_increment')
# MAGIC * idOrder: 12
# MAGIC * sStatus: ACTIVE sstatus: PRELAUNCH ('ACTIVE', 'INACTIVE', 'PROPOSED', 'ENDED')
# MAGIC * dateStart: 2009-01-06 00:00:00
# MAGIC * dateEnd: 2009-02-28 00:00:00
# MAGIC * dateCreate: 2009-01-06 16:13:09
# MAGIC * dateUpdated: 2019-01-09 18:30:11 (Hive doesn't handle default values)

# COMMAND ----------

def orders_generator(num_orders):
  '''
  This function generates a number of objects stored in a list of type Order. 
  Implement validations to make sense of the data.

  Args:
      num_orders (int): number of objects of type order to generate.

  Returns:
      List(dic): List of objects of type orders.
  '''    
  from datetime import datetime, timedelta
  
  result = [] 
  init_date = datetime.strptime('1/1/2021 12:00 AM', '%m/%d/%Y %I:%M %p')
  end_date = datetime.strptime('1/1/2023 12:00 AM', '%m/%d/%Y %I:%M %p')
  for i in range(num_orders):
    datestart, dateend, datecreate, dateupdated = dates_generator(init_date, end_date)
    sstatus = set_status(datestart, dateend, datecreate, dateupdated)
    order = {
      'id' : i + 1,
      'idOrder' :  i + 1,
      'sStatus' : sstatus,
      'dateStart' : str(datestart),
      'dateEnd' : str(dateend),    
      'dateCreate' : str(datecreate),
      'dateUpdated' : str(dateupdated)
    }
    result.append(order)
  
  return result

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DateType, FloatType, DoubleType, TimestampType

data_orders_generated = orders_generator(1000)
data_orders_generated_json = spark.sparkContext.parallelize(data_orders_generated)

schema = StructType([ \
    StructField("id",IntegerType(),True), \
    StructField("idOrder",IntegerType(),True), \
    StructField("sStatus",StringType(),True), \
    StructField("dateStart", TimestampType(), True), \
    StructField("dateEnd", TimestampType(), True), \
    StructField("dateCreate", TimestampType(), True), \
    StructField("dateUpdated", TimestampType(), True) \
  ])

df_orders = sqlContext.read.json(data_orders_generated_json, schema=schema)
df_orders.printSchema()
df_orders.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Table Order
df_orders.write\
         .format("parquet")\
         .mode("overwrite")\
         .saveAsTable('test.tblOrder')  

# COMMAND ----------

spark.sql("select * from test.tblOrder").show()

# COMMAND ----------

# MAGIC %md
# MAGIC <h2>Data Generation (Advertiser Line Item)</h2>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b>Summary:</b>
# MAGIC 
# MAGIC * Author: Juan Escobar
# MAGIC * Creation Date: 05/10/2022
# MAGIC * Description: Populate Table tblOrder
# MAGIC * Documentation: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=82706456#LanguageManualTypes-tinyint
# MAGIC 
# MAGIC <b>Ex row:</b>
# MAGIC 
# MAGIC   
# MAGIC * id: 248049 ('auto_increment')
# MAGIC * idOrder: 112011
# MAGIC * sstatus: PRELAUNCH ('ACTIVE', 'INACTIVE', 'PROPOSED', 'ENDED')
# MAGIC * datestart: 2021-10-01 00:00:00
# MAGIC * dateend: 2021-11-30 23:59:59
# MAGIC * sratetype: CPM
# MAGIC * fclientrate: 4.2
# MAGIC * datecreate: 2021-09-29 00:05:06  
# MAGIC * dateupdated: 2021-09-29 00:20:44 (Hive doesn't handle default values)
# MAGIC * fbudget: 5009.5

# COMMAND ----------

def advertiser_generator(num_advertiser):
  '''
  This function generates a number of objects stored in a list of type advertiser. 
  Implement validations to make sense of the data.

  Args:
      num_orders (int): number of objects of type order to generate.

  Returns:
      List(dic): List of objects of type orders.
  '''    
  import random

  result = [] 
  list_sratetype = ['CPM', 'CPC', 'CPA']
  i = 0
  df_orders_count_rows = df_orders.count()
  for i in range(num_advertiser):
    random_row = df_orders.filter(df_orders.id == random.randint(1, df_orders_count_rows)).first()
    advertiser = {
                  'id' : i + 1,
                  'idOrder' : random_row["idOrder"],
                  'sStatus' : random_row["sStatus"],
                  'dateStart' : str(random_row["dateStart"]),
                  'dateEnd' : str(random_row["dateEnd"]),
                  'sRateType' : random.choice(list_sratetype),
                  'fClientRate' : float(round(random.uniform(1, 10), 1)),
                  'dateCreate' : str(random_row["dateCreate"]),
                  'dateUpdated' : str(random_row["dateUpdated"]),
                  'fBudget' : round(random.uniform(1000, 10000), 1)
                }
    result.append(advertiser)
    i = i + 1
        
  return result

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DateType, FloatType, DoubleType, TimestampType

data_dvertiser_generated = advertiser_generator(1500)
data_dvertiser_generated_json = spark.sparkContext.parallelize(data_dvertiser_generated)

schema = StructType([ \
    StructField("id",IntegerType(),True), \
    StructField("idOrder",IntegerType(),True), \
    StructField("sStatus",StringType(),True), \
    StructField("dateStart", TimestampType(), True), \
    StructField("dateEnd", TimestampType(), True), \
    StructField("sRateType", StringType(), True), \
    StructField("fClientRate", FloatType(), True), \
    StructField("dateCreate", TimestampType(), True), \
    StructField("dateUpdated", TimestampType(), True), \
    StructField("fBudget", DoubleType(), True) \
  ])

df_dvertiser = sqlContext.read.json(data_dvertiser_generated_json, schema=schema)
df_dvertiser.printSchema()
df_dvertiser.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Table Adverstier Line Item
df_dvertiser.write\
            .format("parquet")\
            .mode("overwrite")\
            .saveAsTable('test.tblAdvertiserLineItem')  

# COMMAND ----------

spark.sql("select * from test.tblAdvertiserLineItem").show()

# COMMAND ----------

# DBTITLE 1,Query 3
# MAGIC %sql
# MAGIC /*
# MAGIC Author: Juan Escobar
# MAGIC Creation Date: 05/10/2022
# MAGIC Description: Write an SQL query to identify all orders scheduled to run in November 2021, for which
# MAGIC there are not yet any records in tblAdvertiserLineItem.
# MAGIC 
# MAGIC tblAdvertiserLineItem
# MAGIC 
# MAGIC WHERE dateDay >= '2021-01-01' AND dateDay <= '2023-01-01' 
# MAGIC */
# MAGIC SELECT * 
# MAGIC FROM (SELECT tblOr.id,
# MAGIC         tblOr.idOrder,
# MAGIC         tblOr.sStatus,
# MAGIC         tblOr.dateStart,
# MAGIC         tblOr.dateEnd,
# MAGIC         tblOr.dateCreate,
# MAGIC         tblOr.dateUpdated
# MAGIC FROM test.tblOrder as tblOr
# MAGIC   INNER JOIN test.tblDimDate AS tblDim
# MAGIC     ON to_date(dateStart) = tblDim.dateDay
# MAGIC WHERE tblDim.iMonth = '11') AS NovOrders  
# MAGIC LEFT JOIN test.tblAdvertiserLineItem tblAdv
# MAGIC   ON NovOrders.idOrder = tblAdv.idOrder

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Author: Juan Escobar
# MAGIC Creation Date: 05/10/2022
# MAGIC Description: Write an SQL query to count total number of campaigns in tblOrder grouped by campaign duration. 
# MAGIC Campaign duration would be the number of days between dateStart and dateEnd..
# MAGIC */
# MAGIC SELECT count(*) as amountCampaigns,
# MAGIC         datediff(to_date(tblOr.dateEnd), to_date(tblOr.dateStart)) as duration
# MAGIC FROM test.tblOrder as tblOr 
# MAGIC GROUP BY 2

# COMMAND ----------

# DBTITLE 1,QUESTION_1
# MAGIC %md
# MAGIC <h4>Database design:</h4>
# MAGIC <h5>What are the advantages and disadvantages of creating and using normalized tables? </h5>
# MAGIC 
# MAGIC <p> 
# MAGIC Normalization is the process of simplifying the relationship between fields in a record.
# MAGIC Through normalization, a set of data in one record is replaced by several records that are simpler and more predictable and therefore more manageable. Normalization is carried out for four reasons:
# MAGIC </p> 
# MAGIC <p> 
# MAGIC <b>Advantages:</b></br>
# MAGIC   
# MAGIC 1. Structure data so that relationships can be represented.</br>
# MAGIC 2. Allow easy retrieval of data in response to query and report requests.</br>
# MAGIC 3. Simplify data maintenance by updating, inserting, and deleting data.</br>
# MAGIC 4. Reduce the need to restructure or reorganize data when new applications emerge.</br>
# MAGIC 5. Reduce data duplication.</br>
# MAGIC 6. The data they access is more logically organized in a normalized database.</br>
# MAGIC 7. Enforces referential integrity over Data.</br>
# MAGIC </p> 
# MAGIC 
# MAGIC <p> 
# MAGIC <b>Disadvantages:</b></br>
# MAGIC 
# MAGIC 1. Reduces database performance.</br>
# MAGIC 2. The normalization of a database is a complex and difficult task.</br>
# MAGIC 3. More tables to join when querying data.</br>
# MAGIC 5. As the type of typical structure progresses, the exposure becomes slower and slower.</br>
# MAGIC 6. Adequate information is needed on the different structures.</br>
# MAGIC </p> 
# MAGIC 
# MAGIC <h5>What are the advantages and disadvantages of creating and using non-normalized tables? </h5>
# MAGIC 
# MAGIC 
# MAGIC <p> 
# MAGIC <b>Advantages:</b></br>
# MAGIC 
# MAGIC 1. They allow queries to be optimized due to the few joins required.</br>
# MAGIC 2. They store related information in a single entity and allow it to be analyzed more easily.</br>
# MAGIC 3. Enables the way to extract information from various topics.</br>
# MAGIC 4. Allows the creation of metrics, indicators, KPIs, descriptive and inferential analysis of the data.</br>
# MAGIC 5. Allow to observe, what is happening?</br>
# MAGIC 6. understand why it happens?</br>
# MAGIC 7. predict what will happen?</br>
# MAGIC 8. Collaborate, what the team should do?</br>
# MAGIC 9. decide, which way to go?</br>
# MAGIC 10. Consolidated or integrated information.</br>
# MAGIC 11. May contain historical data.</br>
# MAGIC 12. Structure to consult and analyze the information.</br>
# MAGIC 13. Extends the reporting capabilities of the organization</br>
# MAGIC </p> 
# MAGIC 
# MAGIC <p> 
# MAGIC <b>Disadvantages:</b></br>
# MAGIC 
# MAGIC 1. They require a lot of storage space.</br>
# MAGIC 2. They are complex to standardize by creating views from joins of a Datamart or DWH.</br>
# MAGIC 3. It can contain data at a more detailed or low level.</br>
# MAGIC 4. They require a high processing capacity.</br>
# MAGIC 5. They are not usually used in transactional systems, they are mainly used in BigData, BI and ML applications.</br>
# MAGIC </p> 