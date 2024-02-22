# Lake Demos

VantageCloud Lake Demos Public Repository.

Purpose is to store all the public lake demos here in a single project where the community can collaborate. 


<b style = 'font-size:24px;font-family:Arial;color:#00233C'>Available Demos List</b>



### 1. Environment Setup Automation (Demo_Environment_Setup_Automation.ipynb) ###
**Python Notebook**

**Three files required:**
1. Environment variables file vars.json
2. 0_Demo_Environment_Setup_Automation.ipynb
3. 1_Load_Base_Demo_Data.ipynb

**Alternate - use Apache Airflow**
1. Upload Demo_Setup_Airflow_Python.py to Airflow
2. Edit vars.json and upload as "Variables".
3. Execute the DAG
4. Run 1_Load_Base_Demo_Data.ipynb

### Environment Setup Checklist ###

**To initiate the configuration of the environment used for these demos, perform the steps in "Environment Setup Automation"; either by running the Jupyter Notebook or the Airflow DAG.  Prior to running these scripts, perform the following:**
1. Edit vars.json to reflect the target environment
2. Validate other environment and hierarchy settings in vars.json
3. Clusters are set up to be active during nominal business hours **USA TIME**.  Adjust as necessary in the notebook or DAG
4. If using Airflow, upload the new vars.json to Variables in Airflow Admin Screen
5. When the setup is complete, use the Admin notebook to check cluster status, suspend/resume as needed

#### This notebook will create the Lake environment hierarchy design; ####
- Takes some environmental declarations (users, databases, etc.) from the json file
- Uses **US BUSINESS HOURS** for Clusters active time.  Adjust if needed.
- GRANTs to retail_sample_data for the DEMO_AUTH_NOS to all objects
- Creates a Repositories.PubAuth Authorization Object for accessing open object stores.
- Creates two databases; "demo" and "demo_ofs" each with default NDS and OFS storage respectively.
<br>

Per the design, **SYSDBA** is the account DBA, **CGADMIN** is Compute Group Administrator, users are in the **Business Users** Profile.

<hr>

## 2. Base Data Loading (1_Demo_Setup_Base_Data.ipynb) ##
**Python Notebook**
<br>
**Purpose is to load minimal data to the local Lake system to run the base demo notebooks**
1. Log in as SYSDBA
2. Loads two dimension tables to BFS storage from S3
    - demo.Customer_BFS
    - demo.Accounts_Mapping_BFS
3. Loads one fact table to OFS Storage from S3
    - demo_OFS.Txn_History
    


## 3. Environment Administration (Demo_Admin.ipynb) ##
**Vantage SQL Kernel**
1. Log in as CGADMIN/password
2. Compute Group Status
3. RESUME/SUSPEND/DROP
4. DBC login in case one needs DBC

<hr>

## 4. Data Engineering (Data_Engineering_Exploration.ipynb) ##
**Vantage SQL Kernel**
1. Create OFS Table from S3 "CashApp" transactions
2. Create Foreign Table from S3 "Banking History"
3. Review Tables - Dimensions in BFS, CashApp in OFS, Banking History in S3
4. Execute Joins and Analytics:
    - Identify Customers who have experienced Fraud
    - Show the victim's full behavioral path through their Banking relationship
5. Execute Joins across the Query Fabric (QueryGrid)

<hr>

## 5. Open Analytics Framework (Data_Science_OAF.ipynb) ##
**Python Notebook**
(python 3.8)
1. Credentials and UES URI inherited from vars.json
2. Create custom container - install libraries and versions
3. Upload model and scoring script
4. Execute Feature Engineering - pass it to scoring.
5. Evaluate Model

<hr>

## 5. Data Science Process - Python (Data_Science_OAF.ipynb) ##
**Appendix Section - Create the model**
1. OneHotEncode
2. Test/Train Split
3. Train Model
4. Test Model
5. Confusion Matrix

<hr>

<b style = 'font-size:20px;font-family:Arial;color:#00233C'>VantageCloud Lake Fundamentals</b>
<p style = 'font-size:18px;font-family:Arial;color:#00233C'>Notebooks illustrating the feature/function basics</p>
<p style = 'font-size:18px;font-family:Arial;color:#00233C'>See <a href = 'Fundamentals/README.md'>README</a> for more details</p>

### 1. Native Object Store ###
<a href = 'Fundamentals/Native-Object-Store/NOS_Fundamentals_SQL.ipynb'>Fundamentals/Native-Object-Store/NOS_Fundamentals_SQL.ipynb</a>

<hr>

<b style = 'font-size:20px;font-family:Arial;color:#00233C'>Demos in UseCases Folder</b>
<p style = 'font-size:18px;font-family:Arial;color:#00233C'>Each Use Case has its own data loading notebook.  Typically, the data is loaded from an S3 bucket; bucket name and any credentials are inherited from vars.json file.</p>
<p style = 'font-size:18px;font-family:Arial;color:#00233C'>See <a href = 'UseCases/README.md'>README</a> for more details</p>


### 1. Native KMeans Clustering ###
<a href = 'UseCases/Native-KMeans/KMeans_Clustering_Python.ipynb'>UseCases/Native-KMeans/KMeans_Clustering_Python.ipynb</a>

### 2. Native GLM Numeric Regression ###
<a href = 'UseCases/Native-GLM-Regression/Regression_Python.ipynb'>UseCases/Native-GLM-Regression/Regression_Python.ipynb</a>

### 3. Sentiment Analysis using Native functions ###
<a href = 'UseCases/Native-Sentiment-Analysis/Sentiment_Analysis_Python.ipynb'>UseCases/Native-Sentiment-Analysis/Sentiment_Analysis_Python.ipynb</a>

### 4. Churn Prediction using Native Data Prep, VAL, model training XGBOOST, scoring with BYOM OR OAF ###
<a href = 'UseCases/Churn-Prediction-OAF/Churn-Prediction-OAF.ipynb'>UseCases/Churn-Prediction-OAF/Churn-Prediction-OAF.ipynb</a>


### 5. System Scaling and Monitoring ###
<a href = 'UseCases/Scaling/Demo 1 - Generate Workload.ipynb'>UseCases/Scaling/Demo 1 - Generate Workload.ipynb</a>
<br>
<a href = 'UseCases/Scaling/Demo 2 - Real-Time Monitoring.ipynb'>UseCases/Scaling/Demo 2 - Real-Time Monitoring.ipynb</a>
<br>
<a href = 'UseCases/Scaling/Demo 3 - System Monitoring Queries.ipynb'>UseCases/Scaling/Demo 3 - System Monitoring Queries.ipynb</a>

### 6. Proximity to Climate Risk/Geospatial Analysis ###
<a href = 'UseCases/Proximity-To-Climate-Risk/Proximity_To_Climate_Risk.ipynb'>UseCases/Proximity-To-Climate-Risk/Proximity_To_Climate_Risk.ipynb</a>

### 7. Vector Embeddings for Customer Segmentation ###
<a href = 'UseCases/Vector-Embeddings-Segmentation/Segmentation_With_Vector_Embedding.ipynb'>UseCases/Vector-Embeddings-Segmentation/Segmentation_With_Vector_Embedding.ipynb</a>