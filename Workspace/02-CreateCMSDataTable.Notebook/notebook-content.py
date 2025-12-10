# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9299b816-7204-473f-a211-22a475ee9c6f",
# META       "default_lakehouse_name": "cms_lakehouse",
# META       "default_lakehouse_workspace_id": "92601de7-1223-48ca-ae12-fc8182ecc6e4",
# META       "known_lakehouses": [
# META         {
# META           "id": "9299b816-7204-473f-a211-22a475ee9c6f"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# ### 02 - Create Lakehouse Table (Delta Parquet) from CSV files 
# This is second Notebook of the solution and uses CSV files downloaded in the previous step to create Lakehouse Table (Silver Layer) to be  used in the subsequent step for building the Gold Layer (Star Schema) tables for reporting.

# CELL ********************

source_dir = "Files/cms_raw"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from urllib.parse import urlparse

# function to extract the name of the file form the file full path
# name of the file represents year value for the data for that file 
# example file_path "abfss://f16554c0-3959-4537-9128-ad08c6ac1692@onelake.dfs.fabric.microsoft.com/96391b77-cc32-4714-b791-70a15ccc429f/Files/cms_raw/2013.csv?version=1727404441686?flength=3375263337"
# year value is 2013 which is name of the file
def extract_year_from_file_name(path):
    # Parse the URL to get the path
    parsed_url = urlparse(path)
    # Extract the file name with extension
    file_name_with_extension = os.path.basename(parsed_url.path)
    # Remove the extension
    file_name, extension = os.path.splitext(file_name_with_extension)
    return int(file_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

#create UDF wrapper around the function which extracts name of the file
extract_year_from_file_name_udf = udf(extract_year_from_file_name, IntegerType())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import input_file_name, regexp_extract

# load pyspark dataframe with data from all csv files "Files/cms_raw/2013.csv"
# add a column to the dataframe for the file_path using input_file_name() function 
df = spark.read.format("csv").option("header","true").load(source_dir + "/*.csv").withColumn("file_path", input_file_name())

# use the UDF to extract year value from the file path and add it as a column to data frame
df = df.withColumn("Year", extract_year_from_file_name_udf(df["file_path"]))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import LongType, DecimalType
from pyspark.sql.functions import lit, col, concat

#make minor transformation to set the appropriate data types for the various columns
#as well as add a few new columns based on existing columns
df = df.withColumn("Tot_Drug_Cst", df.Tot_Drug_Cst.cast(DecimalType(10,2))) \
        .withColumn("Tot_30day_Fills", df.Tot_30day_Fills.cast(DecimalType(10,2))) \
        .withColumn("GE65_Tot_30day_Fills", df.GE65_Tot_30day_Fills.cast(DecimalType(10,2))) \
        .withColumn("GE65_Tot_Drug_Cst", df.GE65_Tot_Drug_Cst.cast(DecimalType(10,2))) \
        .withColumn("Prscrbr_City_State", concat(df.Prscrbr_City, lit(", "), df.Prscrbr_State_Abrvtn)) \
        .withColumn("Prscrbr_Full_Name", concat(df.Prscrbr_Last_Org_Name, lit(", "), df.Prscrbr_First_Name)) \
        .withColumn("Tot_Clms", df.Tot_Clms.cast(LongType())) \
        .withColumn("Tot_Day_Suply", df.Tot_Day_Suply.cast(LongType())) \
        .withColumn("Tot_Benes", df.Tot_Benes.cast(LongType())) \
        .withColumn("GE65_Tot_Clms", df.GE65_Tot_Clms.cast(LongType())) \
        .withColumn("GE65_Tot_Benes", df.GE65_Tot_Benes.cast(LongType())) \
        .withColumn("GE65_Tot_Day_Suply", df.GE65_Tot_Day_Suply.cast(LongType())) \
        .drop("file_path")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#write out the table to Lakehouse 
df.write.mode("overwrite").format('delta').saveAsTable("cms_provider_drug_costs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
