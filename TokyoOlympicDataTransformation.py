# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting - Creating conn between Azure DB and Azure Data factory

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "f4edeab3-e6e7-4e8c-8cf3-954b3788b99c",
"fs.azure.account.oauth2.client.secret": 'AUA8Q~lqVMGFkGH5gnNLpcgHmzIgNNupxSh5MblS',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/761c2df9-1e95-43e7-af6d-50f09dcdeec6/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@olympicdataacc.dfs.core.windows.net", # container@storageacc
mount_point = "/mnt/tokyoolympic1",
extra_configs = configs)
  

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if connection works
# MAGIC
# MAGIC Note - APP needs to have access to our container.(Storage account contributor on container IAM)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolympic1"
# MAGIC

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 1,Accessing files using spark
athletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic1/raw-data/athteles.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic1/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic1/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferschema","true").load("/mnt/tokyoolympic1/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferschema","true").load("/mnt/tokyoolympic1/raw-data/teams.csv")


# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()
coaches.printSchema()

# COMMAND ----------

entriesgender.show(10)

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

entriesgender = entriesgender.withColumn("Female",entriesgender.Female.cast(IntegerType()))\
    .withColumn("Male",entriesgender.Male.cast(IntegerType()))\
    .withColumn("Total",entriesgender.Total.cast(IntegerType()))

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show(5)

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show(5)

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

# Find top 5 countries with max number of gold
top_gold_medal_countries = medals.orderBy("Gold",ascending=False).select("Team_Country","Gold").show(5)

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic1/transformed_data/athletes")
coaches.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic1/transformed_data/coaches")
entriesgender.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic1/transformed_data/entriesgender")
medals.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic1/transformed_data/medals")
teams.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic1/transformed_data/teams")

