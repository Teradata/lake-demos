# Lake Use Case Demos

Use Case subfolder.  This contains additional demos built on the parent environment framework.

Notebooks in this folder use the environment setup file vars.json in the parent directory.  All Use Cases have their own data loading script which will generally load data from an S3 bucket.  Bucket information and keys are read from the vars.json environment file.


<b style = 'font-size:24px;font-family:Arial;color:#E37C4D'>Available Demos List</b>



### 1. Native KMeans Clustering (Native-KMeans/KMeans_Clustering_Python.ipynb) ###
**Python 3.8 Notebook**
<br>
**Creates and evaluates a customer segmentation model using a retail data set, with feature engineering, and "operational" feature engineering using fit/transform and fit/columntransformer.**
1. Log in as data_scientist user
2. ColumnSummary
3. Fit Tables - OutlierFilter, SimpleImputer, NonLinearCombine, ScaleFit
4. Transform - ColumnTransformer, ScaleFitTransform
5. KMeans, KMeansPredict
6. Silhouette for model evaluation

<hr>

### 2. Native GLM Numeric Regression (Native-GLM-Regression/Regression_Python.ipynb) ###
**Python 3.8 Notebook**
<br>
**Creates and scale a numeric-only feature set using historic housing price data to predict sales price**
1. Log in as data_scientist user
2. AntiSelect to select features
3. SimpleImputer and ScaleFit/Transform to prepare data set
4. Native GLM/GLMPredict
5. Regression Evaluator

<hr>

### 3. Sentiment Analysis using Native functions (Native-Sentiment-Analysis/Sentiment_Analysis_Python.ipynb) ###
**Python 3.8 Notebook**
<br>
**Uses an Amazon Fine Foods data set to generate a Sentiment score for reviews, and then compares the generated sentiment to the "star" rating the reviewer gave the product**
1. Log in as data_scientist user
2. Execute the SentimentExtractor Function
3. Analyze the results - use Bincode Fit/Transform to create a categoric sentiment out of author "rating"
4. Use ClassificationEvaluator to evaluate results, generate a heatmap.

<hr>

### 4. Text Classification using Native functions (Native-Text-Classification/Python_Text_Classification.ipynb) ###
**Python 3.8 Notebook**
<br>
**Uses an Amazon Fine Foods data set as input to a Naive Bayes Text Classification workflow to predict author ratings value.  Clean up the data set, tokenize the review column, and train/evaluate a native NBTC model, evaluate the results.**
1. Log in as data_scientist user
2. ConvertTo to convert numeric to categorical rating
3. TextParser to tokenize data
4. Native NBTC train/predict
5. Evaluate using Classification Evaluator, plot a heatmap

<hr>

### 5. Churn Prediction using Native Data Prep, VAL, model training XGBOOST, scoring with BYOM OR OAF (Churn-Prediction-OAF/Churn-Prediction-OAF.ipynb) ###
**Python 3.8 Notebook**
<br>
**Note 2 - Just below the imports is a function definition - this function converts nPath format to Plotly Sankey, which is nice to visualize the user paths in the notebook.  This cell can be hidden by selecting the cell and Selecting View->Collapse Selected Cell.**
<br>
<hr>

**Uses the "Retail DSE" Data Set to show sessionize/npath to create churn predictors, Sentiment Analysis on comments to create sentiment polarity, then native functions for one hot encoding, VAL transformations, to train and test an open source xgboost model.**
<br>
**Data Prep and Model Training**
1. Log in as data_scientist user
2. Connect to source data - use VALIDTIME for customer records
3. Sessionize web data, use NPath to identify final event, and two events preceeding
4. Use VAL to One-hot Encode the events, use teradataml assign to binary-code Churn
5. Use Native SentimentExtractor, OneHotEncodingFit/Transform, ConvertTo, ScaleFit to create sentiment polarity
6. Use teradataml/pandas to join, VAL to fill NULLs and create final analytic data set
7. Train/Test Split
8. Train open-source XGBoost model, evaluate locally
9. Save the model for use in OAF AND in BYOM
<br>
**Model scoring using PMMLPredict**
1. Use teradataml methods for loading the model and metadata
2. Execute PMMLPredict as a teradataml DataFrame
3. Evaluate accuracy
<br>
**Model scoring using APPLY/OAF**
1. Connect to existing runtime, or create a new one
2. Install libraries
3. Upload model and scoring code
4. Execute APPLY table operator
5. Evaluate model accuracr
<br>

<hr>

### 6. System Scaling and Monitoring Demos (Scaling/Demo...ipynb) ###
**Python 3.8 Notebooks**
<br>
**Note 2 - The system scaling aspect of the demo requires a compute profile with scaling capabilities.  If the user is using the default vars.json, the proper compute group and profile will be created automatically in Environment Setup and Automation.**
<br>
<hr>
<br>

**Demo 1 - Generate Workload.ipynb**
1. Workload Profile Setup - Define the queries, concurrency, and duration of run
2. Workload Execution - Submit the workload job for parallel execution
3. Thread monitoring and control - Monitor the status of the connections, stop them if desired
<br>
**Demo 2 - Real-Time Monitoring.ipynb**
1. Connect to the VantageCloud Lake System - Connect as a user with access to the metrics service and performance monitoring functions
2. Key Metrics Queries - Queries that monitor active users, Cluster CPU stats, and number of instances
3. Dashboard - Update and plot stats every three seconds
<br>
**Demo 3 - System Monitoring Queries.ipynb**
1. Connect to the VantageCloud Lake System - Connect as a user with access to the metrics service and performance monitoring functions
2. Current Resource Utilization - The current system utilization
3. Historic Resource Utilization - Queries showing how to query historical resource usage data
4. Cluster Events - Queries to analyze what compute resources were available when
5. Active User and Session Monitoring - For active sessions, queries that monitor users and SQL steps and text. This requires a running workload provided in Demo 1
6. Query Logging - Database Query Logs

### 7. Proximity to Climate Risk (Proximity-To-Climate-Risk/Proximity...ipynb) ###
**Python 3.8 Notebooks**
<br>
<hr>

**This demonstration notebook will illustrate how analysts can leverage location and proximity information at scale to analyze which specific addresses are within a certain proximity to flooding.**

1. Uses flood zone data from the 2023 flooding in North New Zealand as a basis for calculating the risk of over 6 million individual addresses
2. Shows common open-source and client-side approaches to Geospatial Analysis
3. Contrasts the same capabilities in-database to run at extreme scale and performance
4. Offers interactive map-based visualizations

### 8. Using Vector Embeddings for Customer Segmentation (UseCases/Vector-Embeddings-Segmentation/Segmentation...ipynb)
**Python 3.8 Notebook**
<br>
<hr>

**This demonstration notebook illustrates how vector embedding can be used to create predictive features - in this case an optimum customer segmentation based on text similarity in customer reviews.**

1. Uses the native TD_WordEmbeddings function to vectorize retail customer comments based on the GloVe 6B 50d word vector model
2. Passes this vector table to multiple iterations of the kmeans function
3. Displays the series of WCSS values in order to identify the "elbow" point which would indicate an ideal number of segments