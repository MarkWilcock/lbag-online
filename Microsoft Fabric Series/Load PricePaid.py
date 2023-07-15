#!/usr/bin/env python
# coding: utf-8

# ## Load PricePaid Notebook
# 
# 
# 

# Load a CSV file into a Spark dataframe and then save the data as tables in our lakehouse.
# 
# The CSV file contains property sales in England since 1995.  The original data is from HM Land Registry in the UK.
# For more details - see https://www.gov.uk/government/collections/price-paid-data.  
# The data contains over 28m rows - each row represents the sale of a property in England with details about the date, price paid, the property type (whether Flat, Terrace etc) and geographic data, most importantly the postcode.

# In[1]:


# Details of the external CSV file
storage_account = "zomalextrainingstorage"
container = "datasets"
folder = "price-paid"
filename = "pp-complete.csv"
#sas_token = r"" # Blank since the datasets container is anonymous access

# Create the full file path to the CSV file using the WASB (blob) access protocol
wasbs_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/{folder}/{filename}"
wasbs_path


# In[2]:


# Load the data in the CSV file into a Spark dataframe.  Show the first two rows. 
df = spark.read.option("header","false").csv(wasbs_path)
df.limit(2).show()


# In[3]:


# Count the number of rows and show the column names and types
print(f"The file has {df.count()} rows") 
df.printSchema()


# In[4]:


# Before we save the dataframe to a table, configure Spark so that it optimises performance of the tables.  (This is boiler-plate code.)
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true") # Enable Verti-Parquet write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write


# In[5]:


# If we don't need to clean the file, we can save it now to a table in the lakehouse
table_name = "PricePaidPySparkOriginal"
df.write.mode("overwrite").format("delta").save(f"Tables/{table_name}")
print(f"Spark dataframe saved to delta table: {table_name}")


# In[6]:


# We need to clean our file. Firstly We do not need the final 6 columns.  Remove these.
df = df.drop("_c10", "_c11", "_c12", "_c13", "_c14", "_c15")

df.limit(2).show()


# In[7]:


# The original CSV file does not have a column header row.  Rename the first 10 columns to more meaningful names.  See the price paid documentation for more details about the column names.
df = df\
    .withColumnRenamed("_c0", "TransactionId")\
    .withColumnRenamed("_c1", "Price")\
    .withColumnRenamed("_c2", "TransactionDate")\
    .withColumnRenamed("_c3", "Postcode")\
    .withColumnRenamed("_c4", "PropertyTypeCode")\
    .withColumnRenamed("_c5", "IsNewBuild")\
    .withColumnRenamed("_c6", "Duration")\
    .withColumnRenamed("_c7", "PAON")\
    .withColumnRenamed("_c8", "SAON")\
    .withColumnRenamed("_c9", "Street")

df.limit(2).show()
print("cleaned schema")
df.printSchema()


# In[8]:


#  There are two columns that are not the default string datyatype.  Change these.
from pyspark.sql.functions import col

df = df.withColumn("Price",col("Price").cast('Integer'))
df = df.withColumn("TransactionDate",col("TransactionDate").cast('Date'))

print("updated schema")
df.printSchema()
df.show(2)


# In[9]:


from pyspark.sql.functions import year
df = df.withColumn("TransactionYear", year(df.TransactionDate))
df.printSchema()
df.show(2)


# In[10]:


# Write the cleaned spark dataframe to a table in the lakehouse
table_name = "PricePaidPySparkClean"
df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"Tables/{table_name}")
print(f"Spark dataframe saved to delta table: {table_name}")

