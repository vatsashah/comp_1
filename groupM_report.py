#!/usr/bin/env python
# coding: utf-8

# In[18]:


import MySQLdb
import pygsheets
import json
import math
import gspread
import ipaddress
import pandas as pd
import datetime
import requests
import springserve
springserve.set_credentials(
    "vatsa.shah@lgads.tv",
    "@123Vatsashah",
    base_url='https://console.springserve.com/api/v0',
)
from pyspark.sql.types import DecimalType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession,SQLContext
sc = SparkSession.builder.appName('groupM_report').getOrCreate()
sqlContext = SQLContext(sparkContext=sc.sparkContext, sparkSession=sc)

from pyspark.sql import SparkSession 

spark = SparkSession.builder.master("local").getOrCreate() 
sc = spark.sparkContext

@udf("string")
def long_to_ip(ip_long):
    return ipaddress.ip_address(ip_long).__str__()

def toPySparkDf(data, spark):
    from pyspark.sql import SQLContext
    from pyspark.sql.dataframe import DataFrame
    sqlctx = SQLContext(spark)
    return DataFrame(data, sqlctx)


# In[20]: 


start_date = str(datetime.date(2025, 4, 1))[:10]
end_date = str(datetime.datetime.now() - datetime.timedelta(days = 1))[:10]
quarter_complete = (datetime.datetime.now() - datetime.timedelta(days = 1 + 90)).timetuple().tm_yday*100/91
# divided by 90 since there are 90 days in q1
# subtracted first 90 days since there were 90 days in q1

quarter_complete = str(math.ceil(quarter_complete*100)/100) + "%"

week_to_eliminate = (datetime.datetime.now() + datetime.timedelta(days = 4)).isocalendar()[1] 
# added 5 days becuase week starts every friday -> calc = 7 - (nos. of days till friday) ex. 1st jan was wed hence, calc = 7 - 2 (wed and thurs) "q1 2025"


spark = SparkSession.builder.master("local").getOrCreate()
sc = spark.sparkContext

report = springserve.reports.run(start_date, end_date,dimensions=["demand_tag_id", "adomain", "detected_adomain"], interval = "day", timezone= "UTC", demand_partner_ids=[61239])
df = spark.read.json(sc.parallelize(report.raw["data"])).select("demand_tag_name", "date", "adomain", "detected_adomain", "wins", "impressions", "revenue")

if report.get_next_page() == True:
    report.get_all_pages()
    df2 = spark.read.json(sc.parallelize(report.raw["data"])).select("demand_tag_name", "date", "adomain", "detected_adomain", "wins", "impressions", "revenue")
    df = df.union(df2)

df = df.filter(~col("demand_tag_name").contains("GPM_Flex_DC_CTV_Universal")).select("date", "adomain", "detected_adomain", "wins", "impressions", "revenue")

df = df.withColumn("domain", when(col("adomain")!="unknown", col("adomain")).otherwise(col("detected_adomain"))).drop("detected_adomain").drop("adomain").filter(col("domain")!="unknown")
df = df.select(col("date").substr(0, 10).alias("date"), col("domain").alias("Advertiser Domain"), "wins", "impressions", "revenue")
df = df.groupBy("date","Advertiser Domain").agg(sum(col("wins")).alias("wins"), sum(col("impressions")).alias("impressions"), sum(col("revenue")).alias("revenue"))
df = df.withColumn("week", (ceil( (dayofyear('date') + 4) / 7) - 13)).drop("date")
# df = df.withColumn("week", (ceil( (dayofyear('date') + 5) / 7) - 13*{1, 2 or 3})).drop("date")
# adding 5. From above calc. substract 13*{1, 2 or 3} (depending on q2, q3 or q4)

df = df.withColumn("week_number", concat( lit("Week_"), lpad( df.week, 2, '0')))
df_tot = df.groupBy("Advertiser Domain").agg(round(sum(col("revenue")),2).alias("Quarterly Rev"))

df = df.groupBy("Advertiser Domain", "week_number").agg(sum(col("wins")).alias("wins"), sum(col("impressions")).alias("impressions"), sum(col("revenue")).alias("revenue"))
df = df.withColumn("Win_Fill%", col("impressions")/col("wins"))

df = df.groupBy("Advertiser Domain").pivot("week_number").agg(round(sum(col("revenue")),2).alias("Revenue"), concat(100*sum(col("Win_Fill%")), lit("%")).alias("Win_Fill%"))

# df.show(50,False)

df = df_tot.join(df, ["Advertiser Domain"], "inner")

# For debugging use following path:
# service_file = "/home/alpha/notebook/Vatsa/Domain_to_brand/client_secret_new.json"
# gc = pygsheets.authorize(service_file="/home/alpha/notebook/Vatsa/Domain_to_brand/client_secret_new.json")

gc = pygsheets.authorize(service_file="/mnt/alpha/var/jupyterhub/notebook/Vatsa/Domain_to_brand/client_secret_new.json")
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1II60dj29GxTgMO2vFL6feVjy2RE5fKCIpuMU80jEVu4/edit?usp=sharing")

wk = sh.worksheet_by_title("domain mapping")
df_temp = pd.DataFrame(wk.get_all_records())
df_domain_map = spark.createDataFrame(df_temp)

df = df.join(df_domain_map, ["Advertiser Domain"], "left").orderBy(col("Quarterly Rev").desc())

# make these below lines active post 2nd week of quarter (lines 107 till 117)

# final_col_rev = "Week_" + str(week_to_eliminate - 1 - 13).zfill(2) + "_Revenue"
# initial_col_rev = "Week_" + str(week_to_eliminate - 2 - 13).zfill(2) + "_Revenue"
# final_col_win = "Week_" + str(week_to_eliminate - 1 - 13).zfill(2) + "_Win_Fill%"
# initial_col_win = "Week_" + str(week_to_eliminate - 2 - 13).zfill(2) + "_Win_Fill%"

# df = df.withColumn("Rev % Change WoW", concat(100*(col(final_col_rev)-col(initial_col_rev))/col(final_col_rev),lit("%")))

# df = df.withColumn("final_win", col(final_col_win).substr(lit(0), length(col(final_col_win)) - 1).cast(DecimalType(18,2)))
# df = df.withColumn("initial_win", col(initial_col_win).substr(lit(0), length(col(initial_col_win)) - 1).cast(DecimalType(18,2)))

# df = df.withColumn("WinFill % Change WoW", concat(100*(col("final_win") - col("initial_win"))/col("final_win"),lit("%"))).drop(col("final_win")).drop(col("initial_win"))

# make the below 2 lines inactive once the above lines are made active

df = df.withColumn("Rev % Change WoW", lit("-"))
df = df.withColumn("WinFill % Change WoW", lit("-"))

item_list = df.columns
col_list = ['Advertiser Domain', "Seller", "Quarterly Rev", 'Rev % Change WoW', "WinFill % Change WoW"]
rem_list = list(set(item_list) - set(col_list))
rem_list.sort()
col_list.extend(rem_list)

df_pandas = df.select(*col_list).na.fill("-").na.fill(0).toPandas()

gc = pygsheets.authorize(service_file="/mnt/alpha/var/jupyterhub/notebook/Vatsa/Domain_to_brand/client_secret_new.json")
sheetUrl="https://docs.google.com/spreadsheets/d/1II60dj29GxTgMO2vFL6feVjy2RE5fKCIpuMU80jEVu4/edit?usp=sharing"
sh = gc.open_by_url(sheetUrl)

wk = sh.worksheet_by_title("week")
df = pd.DataFrame(wk.get_all_records())
df_week = spark.createDataFrame(df)
week_list = df_week.select('q2').rdd.flatMap(lambda x: x).collect()
cell_list = df_week.select('cell').rdd.flatMap(lambda x: x).collect()

sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1II60dj29GxTgMO2vFL6feVjy2RE5fKCIpuMU80jEVu4/edit?usp=sharing")
wk = sh.worksheet_by_title("Q2 - Magnite")
wk.clear()
header = "Percent of quarter complete: " + quarter_complete
last_update = "Last updated on: " + end_date + " (Data in UTC timezone)"
wk.update_value('A1', header)
wk.update_value('A2', last_update)
wk.update_value('A4', "Week")
i=0
for c in cell_list:
        wk.update_value(cell_list[i], week_list[i])
        i+=1
wk.set_dataframe(df_pandas, "A5", copy_head =True)




