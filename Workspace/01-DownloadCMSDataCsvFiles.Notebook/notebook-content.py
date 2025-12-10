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

# ### 01 - Download CMS Medicare Part D data files (CSV format) to Lakehouse
# 
# - [CMS Medicare Part D Prescribers - by Provider and Drug](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug) dataset is available for download from CMS Website. 
# - This Notebook use [Public API Open Data Catalog](https://data.cms.gov/data.json) metadata json file published by CMS to identify and download dataset files to the Lakehouse
# - Dataset contains one file for each year, Title field available for each in Metadata json is used tor identity the year value. Example - Title "Medicare Part D Prescribers - by Provider and Drug : 2016-12-31" indicates the file is for the year 2016

# CELL ********************

#create the sub-directory in Files folder where the CSV files will be downloaded
lakehouse_dir = "Files/cms_raw"

notebookutils.fs.mkdirs(lakehouse_dir)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Documentation provided at the following location  https://data.cms.gov/sites/default/files/2024-05/39b98adf-b5e0-4487-a19e-4dc5c1503d41/API%20Guide%20Formatted%201_5.pdf
# was used as basis for the following code which parses the Public API Open Data Catalog json file to identity the dataset files 

import requests
url = "https://data.cms.gov/data.json"
title= "Medicare Part D Prescribers - by Provider and Drug"
csv_distros =[]
response = requests.request("GET", url)

if response.ok:
    response = response.json()
    dataset = response['dataset']
    for set in dataset:
        if title == set['title']:
            for distro in set['distribution']:
                if 'mediaType' in distro.keys():
                    if distro['mediaType'] == "text/csv":
                        csv_distros.append(distro)        
else:
    error_message = f"An error occrred in downloading the files from CMS Website: {response}"
    print(error_message)
    notebookutils.notebook.exit(error_message, 1)

#print(csv_distros)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#create spark dataframe with rows for all files for the dataset
#downloadURL and title are the 2 fields of interest which are added as column in the dataframe
selected_dataset = [{"downloadURL": obj["downloadURL"], "title": obj["title"]} for obj in csv_distros]
df = spark.createDataFrame(selected_dataset)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import regexp_extract

#identify Year value from the Title and add that as a column to dataframe
df = df.withColumn("year", regexp_extract("title", r"(\d{4})", 1))
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random, time


#function to download the file from URL
def download_file(url, filename, retries = 10, interval = 45):

    attempt = 0

    #usually APIs are rate limited so good idea to have retry pattern implemented for downloads
    while attempt < retries:
        try:

            response = requests.get(url)    
            print(f"Status Code: {response.status_code}")  # Print the status code
            response.raise_for_status()  # Check if the request was successful

            with open(filename, 'wb') as file:
                file.write(response.content)
            
            #file downloaded succesfully so break out of the while loop
            break       
        except requests.exceptions.RequestException as e:            

            attempt += 1
            print(f"Attempt {attempt} failed: {e}")

            if attempt < retries:
                print(f"Retrying in {interval} seconds...")                
                time.sleep(interval)
            else:
                print("All attempts failed. Download unsuccessful.")
                raise Exception("Failed to download file after multiple attempts")

#function to process each DataFrame Row which corresponds to single file in teh dataset
#downloaded file is named based on the year value associated with data
def process_partition(partition):
    for row in partition:
        year_value = row['year']
        output_file = "/lakehouse/default/" + lakehouse_dir + "/" + row['year'] + ".csv"
        download_file(row['downloadURL'], output_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#process the dataframe where each row represents a file to be downloaded from CMS file
df.rdd.foreachPartition(process_partition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
