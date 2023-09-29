#!/usr/bin/env python
# coding: utf-8

# ## Load PricePaid data
# 
# 
# 

# # Load a CSV file into a Spark dataframe and then save the data as tables in our lakehouse.
#  
# The CSV file contains property sales in England since 1995.  The original data is from HM Land Registry in the UK.
# For more details - see https://www.gov.uk/government/collections/price-paid-data.  
# The data contains over 28 million rows - each row represents the sale of a property in England with details about the date, price paid, the property type (whether Flat, Terrace etc) and geographic data, most importantly the postcode.
# 

# Build the string of the full path to access the external source data PricePaid file. 

# In[2]:


# Details of the external CSV file
storage_account = "zomalextrainingstorage"
container = "datasets"
folder = "price-paid"
filename = "pp-complete.csv"
#sas_token = r"" # Blank since the datasets container is anonymous access

# Create the full file path to the CSV file using the WASB (blob) access protocol
wasbs_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/{folder}/{filename}"
wasbs_path


# Load the data in the CSV file into a Spark dataframe.  Show the first two rows.

# In[ ]:


df = spark.read.option("header","false").csv(wasbs_path)
df.limit(2).show()


# In[ ]:


# Alternatively, if the files has been manually upload to the Files are of the lakehouse we can load the data from there
#df = spark.read.format("csv").option("header","false").load("Files/pp-complete.csv")
# df now is a Spark DataFrame containing CSV data from "Files/pp-complete.csv".
#display(df.limit(2))


# Count the rows  and show the schema (list of column names and data types)

# In[ ]:


print(f"The file has {df.count():,} rows") 
df.printSchema()


#  Before we save the dataframe to a table, configure Spark so that it optimises performance of the tables.  (This is boiler-plate code.)

# In[ ]:


spark.conf.set("sprk.sql.parquet.vorder.enabled", "true") # Enable Verti-Parquet write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write


# If we don't need to clean the file, we can save it now to a table in the lakehouse

# In[ ]:


# table_name = "PricePaidOriginal_1"
# df.write.mode("overwrite").format("delta").save(f"Tables/{table_name}")
# print(f"Spark dataframe saved to delta table: {table_name}")


# We need to clean our file. Firstly We do not need the final 6 columns.  Remove these.

# In[ ]:


df_clean = df.drop("_c10", "_c11", "_c12", "_c13", "_c14", "_c15")
df_clean.limit(2).show()


# The original CSV file does not have a column header row.  Rename the first 10 columns to more meaningful names.  
# See the price paid documentation for more details about the column names.

# In[ ]:


df_clean = df_clean\
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

df_clean.show(2)
df_clean.printSchema()


# There are two columns that are not the default string data type.  Change these.

# In[ ]:


from pyspark.sql.functions import col

df_clean = df_clean.withColumn("Price",col("Price").cast('Integer'))
df_clean = df_clean.withColumn("TransactionDate",col("TransactionDate").cast('Date'))

df_clean.printSchema()
df_clean.show(2)


# Add a new column, TransactionYear, based on the TransactionDate.  We will use this later to plot number of sales by year in Power BI.  
# This is necessary since it is not possible to create DAX calculated columns in the Power BI web model pane.

# In[ ]:


from pyspark.sql.functions import year
df_clean = df_clean.withColumn("TransactionYear", year(df_clean.TransactionDate))
df_clean.printSchema()
df_clean.show(2)


# Write the cleaned Spark dataframe to a table in the lakehouse

# In[ ]:


# table_name = "PricePaidClean_1"
# df.write.mode("overwrite").format("delta").option("mergeSchema", "true").save(f"Tables/{table_name}")
# print(f"Spark dataframe saved to delta table: {table_name}")


# We want to create some plots using the pandas and seaborn libraries but 28m rows is too large for efficient pandas operations
# Firstly, We sample 1 row in 1000 to get a smaller dataset of 28K rows.  Hopefully by doing a random sample, this smaller dataset is representative of the full dataset.
# We then convert the small sample dataframe from a Spark dataframe to a pandas one.

# In[ ]:


fraction_to_extract = 0.001
df_sample = df_clean.sample(withReplacement=False, fraction=fraction_to_extract)
print(f"The sample file has {df_sample.count():,} rows")
df_sample.show(2)


# Convert the sample dataframe from a Spark to a pandas dataframe (so we can plot with pandas / seaborn)

# In[ ]:


df_pd_sample = df_sample.toPandas()
df_pd_sample


# Let's create a separate copy to experiment with the Data Wrangler

# In[ ]:


df_wrangler = df_pd_sample.copy()


# In[ ]:


# Code generated by Data Wrangler for pandas DataFrame

def clean_data(df_wrangler):
    # Clone column 'PropertyTypeCode' as 'PropertyTypeName'
    df_wrangler['PropertyTypeName'] = df_wrangler.loc[:, 'PropertyTypeCode']
    # Replace all instances of "T" with "Terraced" in column: 'PropertyTypeName'
    df_wrangler.loc[df_wrangler['PropertyTypeName'].str.lower() == "T".lower(), 'PropertyTypeName'] = "Terraced"
    # Replace all instances of "S" with "Semi" in column: 'PropertyTypeName'
    df_wrangler.loc[df_wrangler['PropertyTypeName'].str.lower() == "S".lower(), 'PropertyTypeName'] = "Semi"
    return df_wrangler

df_wrangler_clean = clean_data(df_wrangler.copy())
df_wrangler_clean.head()


# Plot the number of transactions by year.  
# This code was generated by Chat GPT 3.5.  The prompt was _write a python script to plot a chart in seaborn of the number of transactions (rows) by TransactionYear_

# In[ ]:


import seaborn as sns
import matplotlib.pyplot as plt

# Assuming you have already converted your Spark DataFrame to a Pandas DataFrame
# If not, make sure to convert it as shown in the previous response

# Plot the number of transactions by TransactionYear
plt.figure(figsize=(10, 6))
sns.countplot(data=df_pd_sample, x='TransactionYear', palette='viridis')
plt.title('Number of Transactions by Year')
plt.xlabel('Transaction Year')
plt.ylabel('Number of Transactions')
plt.xticks(rotation=45)

# Show the plot
plt.tight_layout()
plt.show()


# Code written by Chat GPT.  The prompt was 
# _I have a pandas dataframe df_pd_sample.  It has a categorical column PropertyTypeCode.  Count the number of rows grouped by PropertyTypeCode_

# In[ ]:


# Assuming you have a Pandas DataFrame 'df_pd_sample' with the necessary data
# If not, replace 'df_pd_sample' with the actual name of your DataFrame

# Group the DataFrame by 'PropertyTypeCode' and count the rows in each group
property_type_counts = df_pd_sample.groupby('PropertyTypeCode').size().reset_index(name='Count')

# Display the result
print(property_type_counts)


# The property types are:
# * T = Terraced
# * F = Flat
# * S = Semi
# * D = Detached
# * O = Other
# 
# The Other property type includes garages (typically low price) and office buildings (typically high price) so may confuse any predictive model.   
# Code written by Chat GPT.  The prompt was _Remove the rows where the value of the PropertyTypeCode  is O_

# Let's try Data Wrangler.  Python's answer to the Power BI Query Editor

# In[ ]:





# I want meaningingful names, not codes for the property types: Here's my prompt to ChatGPT.  

# In[ ]:


# Create a mapping dictionary
property_type_mapping = {
    'O': 'Other',
    'D': 'Detached',
    'S': 'Semi',
    'F': 'Flat',
    'T': 'Terraced'
}

# Use the mapping dictionary to create the new column 'PropertyTypeName'
df_pd_sample['PropertyTypeName'] = df_pd_sample['PropertyTypeCode'].map(property_type_mapping)

df_pd_sample.head()


# In[ ]:


# Assuming you have a Pandas DataFrame 'pandas_df'
# If not, make sure to load your data into a Pandas DataFrame

# Filter rows where PropertyTypeCode is not equal to 'O'
df_pd_sample = df_pd_sample[df_pd_sample['PropertyTypeCode'] != 'O']

# Reset the index of the DataFrame
df_pd_sample.reset_index(drop=True, inplace=True)

#Let's look at the results
df_pd_sample.groupby('PropertyTypeCode').size().reset_index(name='Count')


# Code originally written by Chat GPT.  The prompt was   
# _Write Python script to train test and build a regression model.  The dependent variable (label) is the Price column.  use only the columns TransactionYear, PropertyTypeCode and IsNewBuild in the model parameters_
# 
# I have simplified - removed the IsNewBuild column from the model

# In[ ]:


import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import seaborn as sns
import matplotlib.pyplot as plt

# Assuming you have a Pandas DataFrame 'pandas_df' with the necessary columns
# If not, make sure to load your data into a Pandas DataFrame

# Select the independent variables and dependent variable
#X = df_pd_sample[['TransactionYear', 'PropertyTypeCode', 'IsNewBuild']]
X = df_pd_sample[['TransactionYear', 'PropertyTypeCode']]
y = df_pd_sample['Price']

# Convert categorical columns to numerical using one-hot encoding
X = pd.get_dummies(X, columns=['PropertyTypeCode'], drop_first=True)

# Split the dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize and train the linear regression model
model = LinearRegression()
model.fit(X_train, y_train)

# Make predictions on the test set
y_pred = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Mean Squared Error: {mse:.2f}")
print(f"R-squared (R2) Score: {r2:.2f}")

# Plot the actual vs. predicted prices
plt.figure(figsize=(10, 6))
sns.scatterplot(x=y_test, y=y_pred)
plt.title('Actual vs. Predicted Prices')
plt.xlabel('Actual Price')
plt.ylabel('Predicted Price')
plt.show()


# Describe the model parameters (with ChatGPT's help)
