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

'''
Python
You need to run a hive query for a sequence of 30 dates (all dates in September 2021)..
The size of the table data and cluster bandwidth require that the query be run for each date individually. The hive query will accept 'DATE' as an argument, as follows.
HQL_QUERY = "select utc_date, sum(1) as num_rows from my_table where utc_date = '${DATE}' group by utc_date"
Using Python, write a script which will execute this query for all dates in September 2021 and store the result to a single file. The file will have 2 columns and up to 30 rows.
'''

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql.types import StructType,StructField, DateType

df_pd = pd.date_range(start="2021-09-01",end="2021-09-30")
list_dates = df_pd.to_list()
df_pd = df_pd.to_frame()

str_dates = list()
for data in list_dates:
  str_dates.append(data.strftime("%Y-%m-%d"))

DATE = tuple(str_dates)
print(DATE)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,  DateType, StringType, TimestampType
from pyspark.sql.functions import col

schema = StructType([ 
    StructField("utc_date", DateType(), True)
  ])
sparkDF=spark.createDataFrame(df_pd, schema=schema) 
sparkDF.select([col(c).cast("string") for c in sparkDF.columns])
display(sparkDF)

# COMMAND ----------

from pyspark.sql.functions import *

sparkDF.write\
         .format("parquet")\
         .mode("overwrite")\
         .saveAsTable('test.sequencial_dates_py', path='/tmp/sequencial_dates_py')\
         #.partitionBy('utc_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select utc_date, 
# MAGIC        sum(1) as num_rows
# MAGIC from test.sequencial_dates_py
# MAGIC where utc_date in ('2021-09-01', '2021-09-02')
# MAGIC group by utc_date

# COMMAND ----------

df_result = spark.sql(f"select utc_date, sum(1) as num_rows from test.sequencial_dates_py where utc_date in {DATE} group by utc_date")


# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE test.sequencial_dates_py

# COMMAND ----------

display(df_result)

# COMMAND ----------

from pyspark.sql.functions import *

df_result.write\
         .format("parquet")\
         .mode("overwrite")\
         .saveAsTable('test.sequencial_dates_py2', 
                      path='/tmp/sequencial_dates_py')

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE test.sequencial_dates_py2

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE test.sequencial_dates_py2;
# MAGIC SELECT * FROM test.sequencial_dates_py2

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE test.sequencial_dates_py

# COMMAND ----------

# MAGIC %fs ls /tmp/sequencial_dates_py

# COMMAND ----------

dbutils.fs.rm("/tmp/sequencial_dates_py", True)

# COMMAND ----------

