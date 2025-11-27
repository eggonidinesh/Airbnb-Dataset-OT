# Databricks notebook source
# MAGIC %md
# MAGIC ### Use your app registration credentials

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.otairbnbdatalakedin.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.otairbnbdatalakedin.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.otairbnbdatalakedin.dfs.core.windows.net", "3e5f54c4-9fe0-49fc-bdb5-3a48cae77ac5")
spark.conf.set("fs.azure.account.oauth2.client.secret.otairbnbdatalakedin.dfs.core.windows.net", "FKF8Q~kSaYxXvIu8Hj2soIsCpBsHE2kgLakRccDz")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.otairbnbdatalakedin.dfs.core.windows.net", "https://login.microsoftonline.com/68adc809-ee77-4af0-b1db-9627e0e18bfb/oauth2/token")

# COMMAND ----------


from pyspark.sql.functions import * 
from pyspark.sql.types import *


# COMMAND ----------

df_air = spark.read.format('csv')\
            .option("header",True)\
            .option("inferSchema",True)\
            .load('abfss://bronze@otairbnbdatalakedin.dfs.core.windows.net/airbnb')



# COMMAND ----------

df_air.display()

# COMMAND ----------

df_air.write.format('parquet')\
.mode('append')\
.option("path","abfss://silver@otairbnbdatalakedin.dfs.core.windows.net/airbnb")\
.save()
