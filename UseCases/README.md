# Lake Use Case Demos

Use Case subfolder.  This contains additional demos built on the parent environment framework.

This folder has its own Setup and data loading scripts, utilizing a copy of vars.json in the parent, and assumes the files match.


<b style = 'font-size:24px;font-family:Arial;color:#E37C4D'>Available Demos List</b>



### 1. Demos Data Loading Script (Setup/Load_Demos_Data.ipynb) ###
**Python Notebook**
Note that the cells can be selectively edited/executed to load specific data sets.

**Required files:**
1. Load_Demos_Data.ipynb
2. vars.json in the parent directory (../vars.json)
3. csv files:
    - customer_mapping.csv
    - amazon_reviews.csv
    - nltk_stopwords.csv
    - bin_table.csv
    - housing_full_partition.csv
    - UK_Retail_Data.csv

<hr>

### 2. Native KMeans Clustering (Native-KMeans/KMeans_Clustering_Python.ipynb) ###
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

### 3. Native GLM Numeric Regression (Native-GLM-Regression/Regression_Python.ipynb) ###
**Python 3.8 Notebook**
<br>
**Creates and scale a numeric-only feature set using historic housing price data to predict sales price**
1. Log in as data_scientist user
2. AntiSelect to select features
3. SimpleImputer and ScaleFit/Transform to prepare data set
4. Native GLM/GLMPredict
5. Regression Evaluator

<hr>

### 4. Sentiment Analysis using Native functions (Native-Sentiment-Analysis/Sentiment_Analysis_Python.ipynb) ###
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
**Note 1 - This demo has a specific Data Loading Notebook - execute Setup/Load_Churn_Demo_Data.ipynb first!**
<br>
**Note 2 - Just below the imports is a function definition - this function converts nPath format to Plotly Sankey, which is nice to visualize the user paths in the notebook.  This cell can be hidden by selecting the cell and Selecting View->Collapse Selected Cell.**
<br>
**Note 3 - As of the January-2023 Lake Drop, OAF is unavailable due to a known issue.  Will be available in FEB-2023 drop.**
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
**Model execution using OAF Not Available in JAN-2023 Drop**
