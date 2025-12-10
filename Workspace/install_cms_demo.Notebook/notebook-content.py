# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### 🚀 Quick Install: End-to-End Microsoft Fabric Sample
# 
# **Dataset**: https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers/medicare-part-d-prescribers-by-provider-and-drug  
# **Source**: Centers for Medicare & Medicaid Services (CMS)
# 
# This notebook sets up a complete demo and learning environment in **Microsoft Fabric**, provisioning the following components:
# 
# - **Lakehouse** with structured storage  
# - Supporting **Notebooks** for data exploration  
# - **Data Factory Pipeline** for data ingestion and transformation to build out the Medallion Architecture  
# - **Semantic Model** for reporting  
# - **Power BI Report** for visualization
# - **Data Agent** generative AI-based capability to chat with your data using natural language  (please see the Pre-Requisite section  [GitHub Repo Setup Guide](https://github.com/isinghrana/fabric-samples-healthcare/blob/main/analytics-bi-directlake-starschema/quick-setup.md#prerequisites) for required settings)  
# 
# To get started, simply click **Run All**. The notebook will execute in about **5–10 minutes**.  
# The final step triggers the **Data Factory pipeline asynchronously**, which loads the data into the Lakehouse.  
# This pipeline run typically takes **20 to 45 minutes** to complete, and since it runs asynchronously, the notebook session can be safely stopped after submission.
# 
# ---
# 
# ### 🧠 Learn by Doing
# 
# This notebook is not just a quick install—it’s also a great learning resource for:
# 
# - **Automation in Microsoft Fabric**
# - **Calling Fabric REST APIs**
# 
# Each major step is clearly documented with markdown cells to explain its purpose, making it easy to follow and adapt for your own scenarios.
# 
# ---
# 
# ### 🔧 Libraries Used
# 
# This notebook leverages two powerful libraries designed for automation in Microsoft Fabric:
# 
# - [**Semantic Link (SemPy)**](https://learn.microsoft.com/en-us/python/api/semantic-link/overview-semantic-link?view=semantic-link-python): A Python SDK for interacting with Fabric resources, including REST API calls, item management, and long-running operations.
# - [**Semantic Link Labs**](https://github.com/microsoft/semantic-link-labs) with additional utilities and examples for scripting and automation in Fabric environments.
# - [**Fabric Data Agent SDK**](https://learn.microsoft.com/en-us/fabric/data-science/fabric-data-agent-sdk): A Python SDK to create, configure and publish a Fabric Data Agent
# 
# These libraries simplify authentication, abstract complex API workflows, and are ideal for building automation solutions.
# 


# CELL ********************

%pip install semantic-link==0.12.1 semantic-link-labs==0.9.10 fabric-data-agent-sdk==0.1.14a0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configure Resource and Notebook Names
# 
# This cell initializes key variables that define the names of resources and notebooks used throughout the demo setup. These variables can be customized to avoid naming conflicts or reuse issues, especially if resources were recently deleted.
# 
# - Resource creation (lakehouse, semantic model, report, pipeline) steps are automatically skipped if the corresponding resource already exists.
# - You may change the default names for resources to avoid conflicts with previously deleted resources that are still locked.
# - The flag `invoke_datafactory_pipeline_step` is set to `True` by default, which means the Data Factory pipeline will be triggered at the end of the notebook to load data. You can set this to `False` to skip pipeline execution.
# - The flag `invoke_data_agent_create_step` is set to `True` by default, which means the Data Agent will be created. You can set this to `False` to skip data agent creation.
# 
#  

# CELL ********************

# name of the resources to be created:
lakehouse_name = "cms_lakehouse"
semantic_model_name = "cms_semantic_model" 
datafactory_pipeline_name = "cms_pipeline"
report_name = "cms_report"
data_agent_name = "cms_data_agent"

# Name for the Notebooks to be imported and executed
download_cmsdata_notebook_import_name = "01-DownloadCMSDataCsvFiles"
create_data_table_notebook_import_name = "02-CreateCMSDataTable"
create_starschema_table_notebook_import_name = "03-CreateCMSStarSchemaTables"

invoke_datafactory_pipeline_step = True
invoke_data_agent_create_step = True   #set to value False if you desire not to setup Data Agent

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import Libraries and Set Advanced Configuration
# 
# This cell imports required libraries and then initialized key configuration variables
# 
# ### **Initialize Advanced Configuration Variables**
# These variables define paths and URLs used for downloading and deploying demo components. You typically **do not need to modify these** unless you're customizing the demo setup.
# 
# #### Key Configuration:
# - **Base Directory**: `Files/cmsdemofiles` — where demo files will be stored in the lakehouse
# - **Artifact ZIP URL**: GitHub link to the ZIP file containing definitions for:
#   - Data Factory Pipeline
#   - Power BI Semantic Model
#   - Report
# - **Notebook Import URLs**: GitHub links to the notebooks used in the demo
# - **Relative Paths for Deployment Files**:
#   - Data Factory pipeline JSON and platform files
#   - Semantic model and report folders
# #### Fixed Constants:
# These are activity names used in the Data Factory pipeline definition. **Do not change** unless the pipeline definition is updated:
# - `DownloadCMSDataset`
# - `CreateCMSDataFlatTable`
# - `CreateCMSStarSchemaTables`
# 
# > ⚠️ **Note**: These configurations are critical for the automation steps that follow. Any changes should be made with caution and a clear understanding of the dependencies.


# CELL ********************

import requests
import zipfile
import os
import sempy_labs as labs
import sempy.fabric as semfabric
import base64
import json

from urllib.parse import urlparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, LongType, DateType

from fabric.dataagent.client import (
    FabricDataAgentManagement,
    create_data_agent
)

#initialization of additional configuration variables which shouldn't be changed unless you know what you are doing

#base directory is created in the lakehouse as part of the demo setup
base_dir_relative_path = "Files/cmsdemofiles"

#external links
#artifact zip file from Github with definition files for Data Factory Pipeline, Power BI Semantic Model and Report
artifactzip_github_url = "https://github.com/isinghrana/fabric-samples-healthcare/raw/refs/heads/main/analytics-bi-directlake-starschema/demoautomation/artifacts.zip"

#github urls from where notebooks for the sample are import
download_cmsdata_notebook_github_url = "https://raw.githubusercontent.com/isinghrana/fabric-samples-healthcare/refs/heads/main/analytics-bi-directlake-starschema/01-DownloadCMSDataCsvFiles.ipynb"
create_data_table_notebook_github_url = "https://raw.githubusercontent.com/isinghrana/fabric-samples-healthcare/refs/heads/main/analytics-bi-directlake-starschema/02-CreateCMSDataTable.ipynb"
create_starschema_table_notebook_github_url = "https://raw.githubusercontent.com/isinghrana/fabric-samples-healthcare/refs/heads/main/analytics-bi-directlake-starschema/03-CreateCMSStarSchemaTables.ipynb"

#data factory definition files are extracted from artifact zip file, relative paths to files which are modified during deployment
datafactory_pipeline_jsonfile_relativepath = "/cms_pipeline.DataPipeline/pipeline-content.json"
datafactory_platform_file_relativepath = "/cms_pipeline.DataPipeline/.platform"

#these are fixed constant values from Data Factory Pipeline definition file
#DO NOT UPDATE unless the pipeline definition file is updated
datafactory_pipeline_downloadcmsdataset_notebookactivityname =  "DownloadCMSDataset"
datafactory_pipeline_createcmsdataflattable_notebookactivityname = "CreateCMSDataFlatTable"
datafactory_pipeline_createcmsstarschematables_notebookactivityname = "CreateCMSStarSchemaTables"

#semantic model definition and report files are extracted from artifact zip file, relative paths to the respective folders
semanticmodel_relative_path = "/CMS_Direct_Lake_Star_Schema.SemanticModel"
report_relative_path = "/CMS Medicare Part D Star Schema.Report"

#files used for data agent creation
data_agent_instructions_path = "/agent_instructions.txt"
data_agent_datasource_instructions_path = "/AI_Skills_01_NotesForModel.txt"
data_agent_datasource_query_examples_path = "/AI_Skills_02_SQL_Examples.json"

#names of the tables to be used as source for Fabric Data Agent
dim_drug_table_name = "cms_provider_dim_drug"
dim_geography_table_name = "cms_provider_dim_geography"
dim_provider_table_name = "cms_provider_dim_provider"
dim_year_table_name = "cms_provider_dim_year"
fact_costs_table_name = "cms_provider_drug_costs_star"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create and Mount Lakehouse for Demo Files
# 
# This cell ensures that the required **Lakehouse** is available and sets up the directory structure for storing demo installation files.
# 
# ### Key Actions:
# 
# 1. **Lakehouse Initialization**:
#    - Checks if a Lakehouse named `cms_lakehouse` already exists.
#    - If it exists, retrieves its ID.
#    - If not, creates a new Lakehouse with the specified name.
# 
# 2. **Directory Setup**:
#    - Constructs the full path (`base_dir_full_path`) for the demo files directory inside the Lakehouse.
#    - Creates the directory if it doesn't exist.
# 
# 3. **Mounting the Lakehouse Directory**:
#    - Mounts the Lakehouse directory to a local path (`mount_point`) so it can be accessed using standard Python file operations.
#    - Saves the local mount path in `base_dir_local_path` for use in subsequent steps.
# 
# > 📁 **Note**: This setup allows seamless access to files stored in the Lakehouse using both Spark and Python code. It also ensures that the demo environment is isolated and reproducible.


# CELL ********************

# this cell creates lakehouse if it doesn't exist, creates the base directory where installation files are downloaded from Github
# notebook does not use any default lakehouse so mounting of the lakehouse as well as saving the local path to mount point in base_dir_local_path variable

lakehouse_exists = any(item['displayName'] == lakehouse_name for item in notebookutils.lakehouse.list())

if (lakehouse_exists):
    #lakehouse already exist so get the lakehouse id for the existing lakehouse
    print(f'Lakehouse exists so using the existing lakehouse : {lakehouse_name}')
    lakehouse_id = notebookutils.lakehouse.get(lakehouse_name)['id']    
else:
    #create lakehouse as it does not exist
    print(f'Creating lakehouse : {lakehouse_name}')
    lakehouse = notebookutils.lakehouse.create(lakehouse_name)    
    lakehouse_id = lakehouse['id']
    
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]                                  

#directory initialization
#base_dir_full_path is the fully qualified path for the lakehouse directory which can be used in Spark code
base_dir_full_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/{base_dir_relative_path}"
notebookutils.fs.mkdirs(base_dir_full_path)

lakehouse_table_dir_full_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"

#mounting of the lakehouse directory     
mount_point = "/mnt/lakehouse/" + lakehouse_name + "/" + base_dir_relative_path
print(f'base_dir full: {base_dir_full_path}, mount_point: {mount_point}')

notebookutils.fs.mount(base_dir_full_path, mount_point)

#local path of the mount point is also saved in variable to be used in subsequent steps in the notebook, plain python code (non-Spark) requires local path 
base_dir_local_path = notebookutils.fs.getMountPath(mount_point)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#common utility functions

#reads file contents and returns it to the caller
#plain python code so need to use the local path
def get_file_contents(local_file_path):
    with open(local_file_path, "r", encoding="utf-8") as file:
        file_content = file.read()
    return file_content

#function is used in steps to import semantic model and report
#input arugment is folder with definition files
#directory and subdirectories are walked through and a dictionary returned where key is the part path and value is the content of the file
def get_fabricitemdef_partdict(definitionfiles_local_path) -> dict[str,str]:

    def_dict = {}

    for root, dirs, files in os.walk(definitionfiles_local_path):
        #print(f'Current directory: {root}')
        for file in files:
            #print(f'  File: {file}')
            part_key = root.replace(definitionfiles_local_path, "") + "/" + file
            part_key = part_key.lstrip('/')
            #print(f'part_key: {part_key}')

            with open( root + "/" + file, "r", encoding="utf-8") as file:
                payload = file.read()
                def_dict[part_key] = payload

    return def_dict    

# use FabricRestClient from Sempy library to make POST requests to REST API calls
# FabricRestClient is a convenient method available in Sempy library - https://learn.microsoft.com/en-us/python/api/semantic-link-sempy/sempy.fabric.fabricrestclient?view=semantic-link-python
# Library has quite a few advantages which you can read more here - https://fabric.guru/using-fabricrestclient-to-make-fabric-rest-api-calls
# couple clear ones to call out:
# 1. Don't have to generate and pass the authentication token, sempy uses the environment details to authenticate the user automatically
# 2. Abstraction of the Long Running Operation where library automatically checks for long running operations to be complete - https://learn.microsoft.com/en-us/rest/api/fabric/articles/long-running-operation
def fabriclient_post(url, request_body):

    client = semfabric.FabricRestClient(credential=None)
    
    response = client.request(method = "POST", path_or_url=url, lro_wait=True, json = request_body)
    print(response.status_code)
    print(response.text)
    response.raise_for_status()  # Raise an error for bad status codes   

# used for data factory pipeline, semantic model and report
# each of these artifacts have .platform file with name of the artifact 
# and this function updates the displayName attribute
def update_displayname_platformfile(json_str, display_name) -> str:

    json_data = json.loads(json_str)
    json_data['metadata']['displayName'] = display_name

    updated_json_str = json.dumps(json_data, indent=4)
    #print(updated_json_str)
    return updated_json_str


def item_exists(item_name, item_type) -> bool:

    items_df = semfabric.list_items(item_type)

    if item_name in items_df['Display Name'].values:
        print(f'{item_name} of type {item_type} exists')
        return True
    else:
        print(f'{item_name} of type {item_type} does not exist')
        return False    

def lakehouse_table_exists(workspace_id: str, lakehouse_id: str, table_name: str) -> bool:
    """
    Checks if a table exists in the specified Lakehouse.

    Args:
        workspace_id (str): The ID of the workspace.
        lakehouse_id (str): The ID of the lakehouse.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    tables_df = labs.lakehouse.get_lakehouse_tables(workspace=workspace_id, lakehouse=lakehouse_id)
    return table_name in tables_df["Table Name"].values


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Download and Unzip Demo Artifacts
# 
# This step downloads the demo artifact ZIP file from GitHub and extracts its contents. The artifact includes:
# 
# - **Data Factory Pipeline** definition files
# - **Semantic Model** files
# - **Report** definition files

# CELL ********************

#download artifacts zip file - Data Factory Pipeline, Semantic Model and REport files from GitHub which be used to create corresponding Fabric Items

#function used to download artifact zip file
def download_binary_file(url, output_path):
    try:        
        response = requests.get(url=url, stream = True)
        
        response.raise_for_status()  # Raise an error for bad status codes
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"File downloaded successfully to: {output_path}")
    except requests.exceptions.RequestException as e:
        print(f"Download failed: {e}")
        raise RuntimeError(f"Failed to download file from {url}") from e


def unzip_file(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Extracted all contents to '{extract_to}'")

artifact_filename = urlparse(artifactzip_github_url).path.split('/')[-1]

if notebookutils.fs.exists(base_dir_full_path + "/" + artifact_filename):
    print (f"{base_dir_full_path}/{artifact_filename} already exists so skipping artifact zip file download step")    
else:            
    download_path = base_dir_local_path + '/' + artifact_filename
    print(f'downloading artifacts zip from - {artifactzip_github_url} to location {download_path}')
    download_binary_file(artifactzip_github_url, download_path)
    print('artifact file downloaded successfully and now unzipping the artifact file')
    unzip_file(download_path, base_dir_local_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import Required Notebooks from GitHub
# 
# This cell automates the import of three notebooks from GitHub into the current Fabric workspace. These notebooks are essential for executing different stages of the demo setup.
# 
# These notebook IDs are stored in variables and used later when orchestrating the Data Factory pipeline.
# 
# > ⏳ **Note**: If a notebook was recently deleted, its name may not be immediately reusable. You can either wait a few minutes or change the import name in the configuration cell to avoid conflicts.


# CELL ********************

def import_notebook(notebook_import_name, githuburl, workspace_id, lakehouse_name) -> str:
    #import notebook and return notebookid
    
    if item_exists(notebook_import_name, "Notebook"):
        print(f'{notebook_import_name} already exists so skipping import')
    else:
        print(f'{notebook_import_name} does not exist so importing from {githuburl}')
        result = labs.import_notebook_from_web(notebook_name = notebook_import_name, url = githuburl)        
        
        #update the default lakehouse        
        notebookutils.notebook.updateDefinition(name = notebook_import_name, workspaceId = workspace_id, defaultLakehouse = lakehouse_name, defaultLakehouseWorkspace= workspace_id)
    
    notebook_id = semfabric.resolve_item_id(item_name = notebook_import_name, type = "Notebook")
    print(f"notebookname: {notebook_import_name}, notebook_id: {notebook_id}")
    return notebook_id


#import notebooks and get Notebook Ids for all 3 notebooks to be used in subsequent steps
download_cmsdata_notebook_id = import_notebook(download_cmsdata_notebook_import_name, download_cmsdata_notebook_github_url, workspace_id, lakehouse_name)
create_data_table_notebook_id = import_notebook(create_data_table_notebook_import_name, create_data_table_notebook_github_url, workspace_id, lakehouse_name)
create_starschema_table_notebook_id = import_notebook(create_starschema_table_notebook_import_name, create_starschema_table_notebook_github_url, workspace_id, lakehouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Data Factory Pipeline via REST API
# 
# This cell automates creation of a **Data Factory pipeline** using the Fabric REST API if it doesn't already exist. The pipeline orchestrates the execution of imported notebooks to ingest and transform CMS data.
# 
# **Key Steps**
# - Reads the pipeline definition (`pipeline-content.json`) and metadata (`.platform`) from the extracted artifact files.
# - Updates the pipeline JSON with workspace and notebook IDs using `update_pipeline_json` function. 
# - Updates the `.platform` file to set the correct display name.
# - Constructs a POST request body with the updated pipeline definition and metadata.
# - Uses `FabricRestClient` to send the request to the Fabric API endpoint.

# CELL ********************

#import data factory pipeline using REST API - https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/items/create-data-pipeline?tabs=HTTP

# Ensures each activity (e.g., `DownloadCMSDataset`, `CreateCMSDataFlatTable`, `CreateCMSStarSchemaTables`) is correctly linked to its corresponding notebook
# by updating the pipeline definition JSON by replacing placeholder notebook activity references with actual notebook IDs from the current workspace.
def update_pipeline_json(json_str, workspace_id, pipelineactivity_notebook_mapping) -> str:
    
    data = json.loads(json_str)    

    for notebook_activity_name, notebook_id in pipelineactivity_notebook_mapping.items():
        
        for activity in data['properties']["activities"]:
            
            if activity.get("name") == notebook_activity_name:
                print(f'Replacing {notebook_activity_name} with {notebook_id}')
                                            
                activity["typeProperties"]["workspaceId"] = workspace_id
                activity["typeProperties"]["notebookId"] = notebook_id

    updated_json_str = json.dumps(data, indent=4)
    #print(updated_json_str)
    return updated_json_str


if item_exists(datafactory_pipeline_name, "DataPipeline"):
    print(f'{datafactory_pipeline_name} exists so skipping the step')
else:
    print(f'{datafactory_pipeline_name} does not exist so creating the pipeline')

    datafactory_pipeline_jsonfile_local_path = base_dir_local_path + datafactory_pipeline_jsonfile_relativepath
    datafactory_platform_file_local_path = base_dir_local_path + datafactory_platform_file_relativepath

    #read file contents
    platform_file_payload =  get_file_contents(datafactory_platform_file_local_path)
    pipeline_json_payload =  get_file_contents(datafactory_pipeline_jsonfile_local_path)

    #pipeline defintion has Json nodes for Notebook Activities, need to update the JSON with appropriate NotebookId from this workspace
    notebookmapping_dict = {
        datafactory_pipeline_downloadcmsdataset_notebookactivityname : download_cmsdata_notebook_id,
        datafactory_pipeline_createcmsdataflattable_notebookactivityname : create_data_table_notebook_id,
        datafactory_pipeline_createcmsstarschematables_notebookactivityname: create_starschema_table_notebook_id    
    }

    #workspace id and notebook ids need to be updated/replaced from the original pipeline definition json
    pipeline_json_payload = update_pipeline_json(pipeline_json_payload, workspace_id, notebookmapping_dict)

    platform_file_payload = update_displayname_platformfile(platform_file_payload, datafactory_pipeline_name)

    #create post request body
    create_datafactory_pipeline_request_body = {
        "displayName": datafactory_pipeline_name,
        "description": "cms_pipeline to ingest and process data",
        "definition" : {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": base64.b64encode(pipeline_json_payload.encode('utf-8')),
                    "payloadType": "InlineBase64"
                },
                {
                    "path": ".platform",
                    "payload": base64.b64encode(platform_file_payload.encode('utf-8')),
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }

    create_pipeline_uri = f"v1/workspaces/{workspace_id}/dataPipelines"
    fabriclient_post(create_pipeline_uri, create_datafactory_pipeline_request_body)   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import Semantic Model via REST API
# 
# This step creates a **Power BI Semantic Model** in the Fabric workspace using the REST API if it doesn't already exist. The semantic model defines the structure and relationships of the CMS data for reporting and analysis.
# 
# 
# **Key Steps**
# - Reads all definition files from the semantic model folder (`CMS_Direct_Lake_Star_Schema.SemanticModel`) into a dictionary using `get_fabricitemdef_partdict`.
# - Updates the .platform file to set the correct display name.
# - Constructs a POST request body with the updated pipeline definition and metadata.
# - Uses `FabricRestClient` to send the request to the Fabric API endpoint.
# - Lastly, updates the Semantic Model to point to the Lakehouse from this Workspace

# CELL ********************

#import semantic model using REST API - https://learn.microsoft.com/en-us/rest/api/fabric/semanticmodel/items/create-semantic-model?tabs=HTTP

if item_exists(semantic_model_name, "SemanticModel"):
    print(f'Semantic Model {semantic_model_name} already exists so skipping the step')
else:    

    create_semantic_model_uri = f"v1/workspaces/{workspace_id}/semanticModels"

    #start with body which will get populated using the model defintion 
    create_semantic_model_request_body = {
        "displayName": semantic_model_name,
        "description": "cms semantic model created using API",
        "definition" : {
            "parts": []
            }
        }

    #read the semantic model definition folder into a dictionary to be used to be populate the request body for API Post call 
    semanticmodel_local_path = base_dir_local_path + semanticmodel_relative_path
    print(f'semantic model definition files path: {semanticmodel_local_path}')

    semantic_model_part_dict = get_fabricitemdef_partdict(semanticmodel_local_path)   
   
    #populate the request body using dictionary
    for key, value in semantic_model_part_dict.items():        

        if ".platform" in key:
            value = update_displayname_platformfile(value, semantic_model_name)

        new_part = {
            "path": key,
            "payload" : base64.b64encode(value.encode('utf-8')),
            "payloadType": "inlineBase64"
        }

        create_semantic_model_request_body["definition"]["parts"].append(new_part)
   
    fabriclient_post(create_semantic_model_uri, create_semantic_model_request_body)   

    print('Semantic Model created successfully and updating the semantic model to point to lakehouse in this workspace')
    
    #update the semantic model to point to lakehouse in this workspace
    labs.directlake.update_direct_lake_model_lakehouse_connection(
        dataset = semantic_model_name,
        lakehouse =  lakehouse_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Import Power BI Report via REST API
# 
# This step creates a **Power BI Report** in the Fabric workspace using the REST API if it doesn't alrady exist. The report is based on the semantic model created in the previous step and provides visual insights into CMS data.
# 
# **Key Steps**
# - Reads all definition files from the semantic model folder (`CMS_Direct_Lake_Star_Schema.Report`) into a dictionary using `report_part_dict`.
# - Updates the `.platform` file to set the correct display name.
# - Updates the `definition.pbir` file to bind the report to the semantic model.
# - Constructs a POST request body with the updated pipeline definition and metadata.
# - Uses `FabricRestClient` to send the request to the Fabric API endpoint.

# CELL ********************

#import report using REST API - https://learn.microsoft.com/en-us/rest/api/fabric/report/items/create-report?tabs=HTTP

def update_reportdef_semanticmodelid(report_def_str, id) -> str:

    report_def_json = json.loads(report_def_str)

    # Replace the pbiModelDatabaseName value    
    report_def_json["datasetReference"]["byConnection"]["pbiModelDatabaseName"] = id
    # Convert back to JSON string
    updated_json_str = json.dumps(report_def_json, indent=4)
    #print(updated_json_str)
    return updated_json_str

if item_exists(report_name, "Report"):
    print(f'Report {report_name} alerady exists so skipping the step')
else:    
    
    #need to get semantic model id because report definition.pbir file needs to be updated with the semantic model craeted as part of the setup
    #in this workspace
    semantic_model_id = semfabric.resolve_item_id(semantic_model_name, type = "SemanticModel")
    create_report_uri = f"v1/workspaces/{workspace_id}/reports"

    #start with body which will get populated using the model defintion 
    create_report_request_body = {
        "displayName": report_name,
        "description": "report created using API",
        "definition" : {
            "parts": []
            }
        }

    #read the semantic model definition folder into a dictionary to be used to be populate the request body for API Post call 
    report_local_path = base_dir_local_path + report_relative_path
    print(f'report definition files path: {report_local_path}')

    report_part_dict = get_fabricitemdef_partdict(report_local_path)   
    
    #populate the request body using dictionary
    for key, value in report_part_dict.items():              

        if ("definition.pbir" in key):
            value = update_reportdef_semanticmodelid(value, semantic_model_id)        
            #print(f'Updated definition json: {value}')
        elif (".platform" in key):
            value = update_displayname_platformfile(value, report_name)

        new_part = {
            "path": key,
            "payload" : base64.b64encode(value.encode('utf-8')),
            "payloadType": "inlineBase64"
        }           
    
        create_report_request_body["definition"]["parts"].append(new_part)
                
    fabriclient_post(create_report_uri, create_report_request_body)
    print('report created successfully')  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pre-Requisites for Fabric Data Agent
# 
# This step sets up **empty gold layer tables** that act as source datasets for the **Fabric Data Agent**. This initialization is a **required pre-requisite** before the Data Agent can be configured.
# 
# Key points:
# 
# *   The tables represent the gold layer structure and are essential for enabling the Fabric Data Agent to operate correctly.
# *   Tables are created **only if they do not already exist**, and **only if** the `create_data_agent` flag (defined in the notebook configuration) is set to `True`.
# *    After creating the tables, a metadata refresh is executed using the Fabric REST API to ensure the Lakehouse SQL Endpoint reflects the latest schema. It helps ensure that newly created tables are visible and usable by the Fabric Data Agent SDK in subsequent step.
# *   Data is loaded into the table in the **next step** which triggers a **Fabric Pipeline asynchronously** to ingest the actual dataset.


# CELL ********************

if not invoke_data_agent_create_step:
    print("Skipping Data Agent Pre-Requisite Step")
else:

    #####################
    #Step 1: Create empty Lakehouse Tables with appropriate schema as they need to be specified as data source for the Data Agent. Data will
    #         be loaded into tables by Pipeline

    # Create empty Lakehouse Tables only if they don't already exis

    if not lakehouse_table_exists(workspace_id, lakehouse_id, dim_drug_table_name):
        print(f'Creating {dim_drug_table_name}...')
        # Define schema
        dim_drug_table_schema = StructType([
            StructField("Brnd_Name", StringType(), True),
            StructField("Gnrc_Name", StringType(), True),
            StructField("Max_Year", IntegerType(), True),
            StructField("Min_Year", IntegerType(), True),
            StructField("drug_key", IntegerType(), True)
        ])

        # Create empty DataFrame
        dim_drug_table_df = spark.createDataFrame([], dim_drug_table_schema)
        
        dim_drug_table_df \
            .write.format('delta') \
            .option('path',f'{lakehouse_table_dir_full_path}/{dim_drug_table_name}') \
            .save()

    if not lakehouse_table_exists(workspace_id, lakehouse_id, dim_geography_table_name):
        print(f'Creating {dim_geography_table_name}....')
        
        # Define schema
        dim_geography_table_schema = StructType([
            StructField("Prscrbr_City", StringType(), True),
            StructField("Prscrbr_City_State", StringType(), True),
            StructField("Prscrbr_State_Abrvtn", StringType(), True),
            StructField("Prscrbr_State_FIPS", StringType(), True),
            StructField("Max_Year", IntegerType(), True),
            StructField("Min_Year", IntegerType(), True),
            StructField("geo_key", IntegerType(), True)
        ])

        # Create empty DataFrame
        dim_geography_table_df = spark.createDataFrame([], dim_geography_table_schema)
        
        dim_geography_table_df \
            .write.format('delta') \
            .option('path',f'{lakehouse_table_dir_full_path}/{dim_geography_table_name}') \
            .save()


    if not lakehouse_table_exists(workspace_id, lakehouse_id,dim_provider_table_name):
        print(f'Creating {dim_provider_table_name} .....')

        # Define schema
        dim_provider_table_schema = StructType([
            StructField("Prscrbr_First_Name", StringType(), True),
            StructField("Prscrbr_Full_Name", StringType(), True),
            StructField("Prscrbr_Last_Org_Name", StringType(), True),
            StructField("Prscrbr_NPI", StringType(), True),
            StructField("Prscrbr_Type", StringType(), True),
            StructField("Prscrbr_Type_Src", StringType(), True),
            StructField("Max_Year", IntegerType(), True),
            StructField("Min_Year", IntegerType(), True),
            StructField("provider_key", IntegerType(), True)
        ])

        # Create empty DataFrame
        dim_provider_table_df = spark.createDataFrame([], dim_provider_table_schema)

        dim_provider_table_df \
            .write.format('delta') \
            .option('path',f'{lakehouse_table_dir_full_path}/{dim_provider_table_name}') \
            .save()


    if not lakehouse_table_exists(workspace_id, lakehouse_id, dim_year_table_name):
        print(f'Creating {dim_year_table_name} .....')

        # Define schema
        dim_year_table_schema = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Year_Date_Key", DateType(), True)
            ])

        # Create empty DataFrame
        dim_year_table_df = spark.createDataFrame([], dim_year_table_schema)

        dim_year_table_df \
            .write.format('delta') \
            .option('path',f'{lakehouse_table_dir_full_path}/{dim_year_table_name}') \
            .save()


    if not lakehouse_table_exists(workspace_id, lakehouse_id, fact_costs_table_name):
        print(f'Creating {fact_costs_table_name} .....')

        # Define schema
        fact_costs_table_schema = StructType([
            StructField("GE65_Bene_Sprsn_Flag", StringType(), True),
            StructField("GE65_Sprsn_Flag", StringType(), True),
            StructField("GE65_Tot_30day_Fills", DecimalType(10,2), True),
            StructField("GE65_Tot_Benes", LongType(), True),
            StructField("GE65_Tot_Clms", LongType(), True),
            StructField("GE65_Tot_Day_Suply", LongType(), True),
            StructField("GE65_Tot_Drug_Cst", DecimalType(10,2), True),
            StructField("Tot_30day_Fills", DecimalType(10,2), True),
            StructField("Tot_Benes", LongType(), True),
            StructField("Tot_Clms", LongType(), True),
            StructField("Tot_Day_Suply", LongType(), True),
            StructField("Tot_Drug_Cst", DecimalType(10,2), True),
            StructField("Year", IntegerType(), True),
            StructField("drug_key", IntegerType(), True),
            StructField("geo_key", IntegerType(), True),
            StructField("provider_key", IntegerType(), True)
        ])

        # Create empty DataFrame
        fact_costs_table_df = spark.createDataFrame([], fact_costs_table_schema)

        fact_costs_table_df \
            .write.format('delta') \
            .option('path',f'{lakehouse_table_dir_full_path}/{fact_costs_table_name}') \
            .save()

    ###########################  
    # Step 2: Fabric Data Agent SDK functions most likely use Lakehouse SQL Endpoint so execute metadata refresh operation using REST API
    #         otherwise newly created tables might not be available be set as source for Data Agent

    #Instantiate the client
    client = semfabric.FabricRestClient()

    # Get the SQL endpoint to sync with the lakehouse
    sqlendpoint = client.get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['id']

    # URI for the call 
    uri = f"v1/workspaces/{workspace_id}/sqlEndpoints/{sqlendpoint}/refreshMetadata" 

    # payload = {} # empty payload, all tables
    # Example of setting a timeout, { "timeout": {"timeUnit": "Seconds", "value": "120"}  }
    payload = {} 

    fabriclient_post(uri, payload)  


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Trigger Data Factory Pipeline to Load Data
# 
# This step initiates the execution of the **Data Factory pipeline** to ingest and process CMS data into the Lakehouse.
# 
# ### 🧠 What Happens Next:
# The pipeline orchestrates the execution of the three imported notebooks:
# - Download CMS data
# - Create flat data table
# - Create star schema tables
# 
# Once the pipeline completes, the data is available in the Lakehouse for querying and reporting.
# 
# > ⏱️ **Note**: The pipeline runs **asynchronously**—this cell only triggers the job via an API call. The notebook session does not need to remain active, and can be safely stopped after submission.
# 
# > 📍 **Monitoring Tip**: You can monitor the pipeline run from the **Monitoring Hub** or by opening the pipeline and selecting **Run > View Run History** to track progress and completion.


# CELL ********************

#invoke Data Factory Pipeline to load data to Lakehouse using Fabric REST API - https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job?tabs=HTTP

if not invoke_datafactory_pipeline_step:
    print('Skipping invocation of Data Factory Pipeline setp')
else:
    datafactory_pipeline_id = semfabric.resolve_item_id(datafactory_pipeline_name, type = "DataPipeline")
    print(datafactory_pipeline_id)

    url = f"v1/workspaces/{workspace_id}/items/{datafactory_pipeline_id}/jobs/instances?jobType=Pipeline"

    client = semfabric.FabricRestClient()
    response = client.request(method = "POST", path_or_url=url)
    print(response.status_code)
    print(response.text)
    response.raise_for_status()  # Raise an error for bad status codes   

    print("Data Factory Pipeline Job submitted successfully - monitor Pipeline Run from Monitoring Hub or open the pipeline then use Run > View Run History menu to actively monitor the pipeline. Once pipeline job complete data is available in Lakehouse for querying and reporting")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# 
# ## Create Fabric Data Agent using Fabric Data Agent SDK
# 
# Key points:
# 
# *   Data Agent is created **only if it does not already exist**, and **only if** the `create_data_agent` flag (defined in the notebook configuration) is set to `True`.
# *   Creates the Data Agent, sets Lakehouse tables as data source and applies configuration (including instructions, few-shot query examples) and publishes the agent
# 
# > ⏳ **Note**: This step is placed at the end of the notebook intentionally—so even if it fails (e.g., due to tenant-level Fabric Data Agent settings not being enabled), other components like Reports, Lakehouse, and Pipelines are still set up successfully


# CELL ********************

#Create Data Agent as the pre-requisite of empty table creation and SQL Endpoint metadata synch is complete

if not invoke_data_agent_create_step:

    print("Skipping Data Agent Pre-Requisite Step ")

elif item_exists(data_agent_name, "DataAgent"):

    print(f'Data Agent {data_agent_name} alerady exists so skipping the step')
    
else:    
    data_agent = create_data_agent(data_agent_name)

    data_agent_instructions_local_path = base_dir_local_path + data_agent_instructions_path
    data_agent_instructions =  get_file_contents(data_agent_instructions_local_path)
    data_agent.update_configuration(instructions = data_agent_instructions)

    data_agent.add_datasource(artifact_name_or_id = lakehouse_name, workspace_id_or_name = workspace_id, type="lakehouse")
    datasource = data_agent.get_datasources()[0]
    datasource.pretty_print()

    datasource.select("dbo", dim_drug_table_name)
    datasource.select("dbo", dim_geography_table_name)
    datasource.select("dbo", dim_provider_table_name)
    datasource.select("dbo", dim_year_table_name)
    datasource.select("dbo", fact_costs_table_name)

    data_agent_datasource_instructions_local_path = base_dir_local_path + data_agent_datasource_instructions_path
    data_agent_datasource_instructions = get_file_contents(data_agent_datasource_instructions_local_path)    
    datasource.update_configuration(instructions = data_agent_datasource_instructions)

    data_agent_datasource_query_examples_local_path = base_dir_local_path + data_agent_datasource_query_examples_path
    data_agent_datasource_query_examples = get_file_contents(data_agent_datasource_query_examples_local_path)
    data_agent_datasource_query_examples_dict = json.loads(data_agent_datasource_query_examples)
    datasource.add_fewshots(data_agent_datasource_query_examples_dict)

    data_agent.publish()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
