#!/usr/bin/env python
# coding: utf-8

# In[19]:


import MySQLdb
import pandas as pd
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, lit
from pyspark.sql import SparkSession,SQLContext
spark = SparkSession.builder.appName("Category_Blocking").getOrCreate()
sc=spark.sparkContext
sqlContext = SQLContext(sc)

conn = MySQLdb.connect(user="alpha", password="h3!!0D0CT0r@ALPH", host="campaignsdb.alphonso.tv", database="Campaigns_DB", port=3398)
print("Connection Established")


# In[20]:


# Creating the df of the brand meta data present in SQL table

import requests
import json

url = "http://etcd.alphonso.tv:2379/v2/keys/alphonso/services/ums-prod/config/admin-auth-token"
data = requests.get(url)
json_object = json.loads(data.text)
auth_token = json_object["node"]["value"]

headers={
    'Content-Type': 'application/json',
    "authorization-token": auth_token
}

total_val = 300000
limit = 1000
list_df = []

for i in range(0,int(total_val/limit)):
    url = "https://pas.lgads.tv/v1/adomain?offset=" + str(i*limit) + "&limit=" + str(limit)
    response_all = requests.request("GET", url, headers=headers)
    list_adomains = response_all.json()
    if len(list_adomains) == 0: break
    for adomain in list_adomains:
        db_adomain = adomain["adomain"]
        db_category = ""
        if adomain["klazify_parent_category"] != None: db_category += adomain["klazify_parent_category"]
        if adomain["klazify_brand"] != None: db_category += adomain["klazify_brand"] 
        if adomain["ss_industry"] != None: db_category += adomain["ss_industry"]
        if adomain["ss_brand"] != None: db_category += adomain["ss_brand"]
        if adomain["manual_category"] != None: db_category += adomain["manual_category"]
        if adomain["other_info"] != None: db_category += adomain["other_info"]
        if adomain["adomain"] != None: db_category += adomain["adomain"]
        temp_list = [db_adomain, db_category]
        list_df.append(temp_list)

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
columns = StructType([StructField('domain', StringType(), True), StructField('Category', StringType(), True)])
df_adomain = spark.createDataFrame(data = list_df, schema = columns)

# In[28]:


# Reading from sheetUrl

import os
import pygsheets
import json
import springserve
springserve.set_credentials(
    "vatsa.shah@lgads.tv",
    "@123Vatsashah",
    base_url='https://console.springserve.com/api/v0',
)
gc = pygsheets.authorize(service_file="/mnt/alpha/var/jupyterhub/notebook/Vatsa/Domain_to_brand/client_secret_new.json")
sheetUrl="https://docs.google.com/spreadsheets/d/1YIN7zyvB6ylST8b571uFB3rzwXX-XNY8J31bX6OmSpo/edit#gid=0"
sh = gc.open_by_url(sheetUrl)

# Creating dataframe for category list to it's synonym mapping
wk = sh.worksheet_by_title('Cleaned category list')
df_temp = wk.get_as_df()
df_key = spark.createDataFrame(df_temp)

# Creating dataframe for the users category choice
wk = sh.worksheet_by_title('Supply Tag Level')
df_temp = wk.get_as_df()
df_ui = spark.createDataFrame(df_temp)

# Creating dataframe for the remove_adomains
wk = sh.worksheet_by_title('remove_from_list')
df_temp = wk.get_as_df()
df_remove = spark.createDataFrame(df_temp)

# Creating dataframe for the add_adomains
wk = sh.worksheet_by_title('add_to_list')
df_temp = wk.get_as_df()
df_add = spark.createDataFrame(df_temp)

# Creating all the required lists from the df created

category_list_id = df_key.select('list_id').rdd.flatMap(lambda x: x).collect()
category_names_list = df_key.select('k_0').rdd.flatMap(lambda x: x).collect()
supply_tag_id = df_ui.select('Supply Tag ID').rdd.flatMap(lambda x: x).collect()
df_remove = df_remove.withColumn('arr', *[array(df_remove.drop('list_id').drop('list_name').columns)])
remove_adomains = df_remove.select('arr').rdd.flatMap(lambda x: x).collect()
df_add = df_add.withColumn('arr', *[array(df_add.drop('list_id').drop('list_name').columns)])
add_adomains = df_add.select('arr').rdd.flatMap(lambda x: x).collect()

# Creating the 2 dimensional list of true/false choice given by user

category_choice = []
for i in range(len(category_names_list)):
    temp_list = df_ui.select(category_names_list[i]).rdd.flatMap(lambda x: x).collect()
    category_choice.append(temp_list)
choice_list = list(map(list, zip(*category_choice)))

# Creating the 2 dimensional list of category synonyms

alt_key = []
for i in range(0,21):
    key_col = 'k_'+str(i)
    temp_list = df_key.select(key_col).rdd.flatMap(lambda x: x).collect()
    alt_key.append(temp_list)
alt_key_list = list(map(list, zip(*alt_key)))

# Updating adomains list in springserve

for i in range(len(category_list_id)):
    adomain_list = []
    for j in range(0,21):
        if alt_key_list[i][j] == "": continue
        temp_list = df_adomain.filter((col("Category").contains(alt_key_list[i][j])) | col("Category").contains(alt_key_list[i][j].lower()) | col("Category").contains(alt_key_list[i][j].title())).select("domain").rdd.flatMap(lambda x: x).collect()
        adomain_list.extend(temp_list)
    pharma_list = df_adomain.filter(col("Category").contains("Pharma")).select("domain").rdd.flatMap(lambda x: x).collect()
    adomain_list = [j for j in adomain_list if j not in pharma_list]
    
    adomain_list = list(set(adomain_list) - set(remove_adomains[i]))
    adomain_list.extend(add_adomains[i])
    
    adomain_list = list(set(adomain_list))
    while '' in adomain_list: adomain_list.remove('')
    if adomain_list == []:
        print("Updating list for " + alt_key_list[i][0] +" category, True")
        continue
    
    remove_adomains[i] = list(set(remove_adomains[i]))
    while '' in remove_adomains[i]: remove_adomains[i].remove('')

    ss_list = springserve.advertiser_domain_lists.get(category_list_id[i])
    resp = ss_list.bulk_delete(remove_adomains[i])
    resp.ok
    
    j=0
    temp_list = []
    for adomain in adomain_list:
        if j%500 == 0:
            response = ss_list.bulk_create(temp_list)
            temp_list = []
        j+=1
        temp_list.append(adomain)
    response = ss_list.bulk_create(temp_list)
    print("Updated list for " + alt_key_list[i][0] +" category", response.ok)

print("\n")

# Attaching the category list to the chosen supply tags


for i in range(0,len(supply_tag_id)):

    print("\nUpdating ",i+1, "/", len(supply_tag_id)," supply tag. Supply tag ID: ",supply_tag_id[i])
    if i%500 == 0: all_tags = springserve.supply_tags.get()
    tag = [tmp for tmp in all_tags if tmp.id == supply_tag_id[i]]
    if len(tag) == 0: 
        print("supply tag id ", supply_tag_id[i], "is deleted. Therefore skipping.")
        continue
    tag = tag[0]
    if tag.advertiser_domain_targeting == "Allowlist":
        print("supply tag id ", supply_tag_id[i], "has allowlist of adomains attached. Therefore skipping.")
        continue
    k = 0
    for j in range(len(category_list_id)):
        if choice_list[i][j] == "FALSE": k = k+1
    if k == len(category_list_id):
        print("No blocklist chosen. Therefore skipping.")
        continue
    if tag.active == False:
        print("supply tag id ", supply_tag_id[i], "is inactive. Therefore skipping.")
        continue
    selected_list = []
    previous_list = []
    for j in range(len(category_list_id)):
        if choice_list[i][j] == "TRUE":
            selected_list.append(category_list_id[j])
    previous_list = tag.advertiser_domain_list_ids
    selected_list = set(selected_list)
    previous_list = set(previous_list)
    if previous_list == selected_list:
        print("Selection not changed. Hence, skipping")
        continue
    external_list = [i for i in previous_list if i not in category_list_id]
    tag.advertiser_domain_list_ids.clear()
    for j in range(len(external_list)):
        tag.advertiser_domain_list_ids.append(external_list[j])
        tag.save().ok
    for j in range(len(category_list_id)):
        tag.advertiser_domain_list_ids.append(category_list_id[j])
        tag.advertiser_domain_targeting = "Blocklist"
        if choice_list[i][j] == "FALSE": 
            tag.advertiser_domain_list_ids.remove(category_list_id[j])
            print("Removing the updated list of adomains belonging to "+ category_names_list[j] +" category")
        else:
            print("Attaching the updated list of adomains belonging to "+ category_names_list[j] +" category")
        tag.save().ok

