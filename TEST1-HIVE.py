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
# MAGIC 
# MAGIC <p>
# MAGIC Hive, HDFS</br>
# MAGIC 
# MAGIC QUESTION_1) Given a table in Hive, how do we identify where the data is stored?</br>
# MAGIC QUESTION_2) How can we see what partitions the table may have?</br>
# MAGIC QUESTION_3) How can we see if the table is missing any partitions?</br></br>
# MAGIC 
# MAGIC Given a table, 'auctions' with the following DDL:</br>
# MAGIC      CREATE TABLE auctions (</br>
# MAGIC            auctionid string COMMENT 'the unique identifier for the auction',</br>
# MAGIC            idlineitem bigint COMMENT 'the Line Item ID associated with the auction',</br>
# MAGIC            arysegments array<string> COMMENT 'the array of segments associated with the auction'</br>
# MAGIC      )</br>
# MAGIC      PARTITIONED BY (utc_date string)</br>
# MAGIC      ROW FORMAT DELIMITED</br>
# MAGIC      FIELDS TERMINATED BY '\t'</br>
# MAGIC      STORED AS TEXTFILE</br>
# MAGIC      LOCATION '/bidder_data/auctions' ;</br>
# MAGIC </p> 
# MAGIC <p>     
# MAGIC <b>What would be the queries to answer the following questions:</b></br>
# MAGIC QUERY_1) Provide an HQL query to provide a distribution of the number of auctions and line items, grouped by the number of segments within each auction record.</br>
# MAGIC QUERY_2) Provide an HQL query to provide the distinct count of auctions and line items, associated to each segment within arysegments. (HINT: You will need to expand the</br>
# MAGIC segment array first.)</br>
# MAGIC </p>

# COMMAND ----------

# DBTITLE 1,QUESTION_1) Given a table in Hive, how do we identify where the data is stored?
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]
 
df = spark.createDataFrame(data=data2)
df.write.mode("overwrite").saveAsTable("Employees2")

spark.sql("show tables").show(50, truncate=False)
df_describe_formatted = spark.sql("describe formatted Employees")
display(df_describe_formatted)

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/

# COMMAND ----------

# DBTITLE 1,QUESTION_2) How can we see what partitions the table may have?
df_describe_formatted.where("col_name='# Partitioning'").show()

# COMMAND ----------

#describe formatted <table> partition <partition spec>

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS Employees;

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/employees/

# COMMAND ----------

# DBTITLE 1,QUESTION_3) How can we see if the table is missing any partitions?
# MAGIC %sql
# MAGIC msck repair table mytable;
# MAGIC 
# MAGIC Partitions missing from filesystem:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE auctions (
# MAGIC       auctionid string COMMENT 'the unique identifier for the auction',
# MAGIC       idlineitem bigint COMMENT 'the Line Item ID associated with the auction',
# MAGIC       arysegments array<string> COMMENT 'the array of segments associated with the auction'
# MAGIC )
# MAGIC PARTITIONED BY (utc_date string)
# MAGIC ROW FORMAT DELIMITED
# MAGIC FIELDS TERMINATED BY '\t'
# MAGIC STORED AS TEXTFILE
# MAGIC LOCATION '/bidder_data/auctions' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC set hive.exec.dynamic.partition.mode=nonstrict

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO auctions 
# MAGIC SELECT 'au1',2, array('seg1','seg2','seg3'), from_unixtime(unix_timestamp("2012-10-05 11:30:00 UTC", "yyyy-MM-dd HH:mm:ss z")) as utc_date
# MAGIC UNION ALL SELECT 'au2',3, array('seg4','seg5'), from_unixtime(unix_timestamp("2012-10-06 11:30:00 UTC", "yyyy-MM-dd HH:mm:ss z")) as utc_date
# MAGIC UNION ALL SELECT 'au3',3, array('seg11','seg7'), from_unixtime(unix_timestamp("2012-10-07 11:30:00 UTC", "yyyy-MM-dd HH:mm:ss z")) as utc_date
# MAGIC UNION ALL SELECT 'au4',3, array('seg8','seg9'), from_unixtime(unix_timestamp("2012-10-07 11:30:00 UTC", "yyyy-MM-dd HH:mm:ss z")) as utc_date
# MAGIC UNION ALL SELECT 'au5',4, array('seg10','seg11'), from_unixtime(unix_timestamp("2012-10-07 11:30:00 UTC", "yyyy-MM-dd HH:mm:ss z")) as utc_date

# COMMAND ----------

# DBTITLE 1,QUERY_1) 
# MAGIC %sql
# MAGIC /*
# MAGIC QUERY_1) Provide an HQL query to provide a distribution of the number of auctions and line items, grouped by the number of segments within each auction record.
# MAGIC */
# MAGIC 
# MAGIC SELECT count(auctionid), 
# MAGIC        count(idlineitem), 
# MAGIC        arysegments 
# MAGIC FROM auctions 
# MAGIC GROUP BY arysegments

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC QUERY_2) Provide an HQL query to provide the distinct count of auctions and line items, associated to each segment within arysegments. (HINT: You will need to expand the
# MAGIC segment array first.)
# MAGIC */
# MAGIC SELECT COUNT(DISTINCT auctionid, idlineitem),
# MAGIC        segment 
# MAGIC FROM auctions 
# MAGIC LATERAL VIEW posexplode(arysegments) t1 AS q1, segment 
# MAGIC GROUP BY segment

# COMMAND ----------

