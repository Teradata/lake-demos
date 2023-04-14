# Load dependency packages
import sys
import csv
import numpy as np
import pandas as pd
from xgboost import XGBClassifier, Booster
import warnings

# pickle will issue a caution warning, if model pickling was done with
# different library version than used here. The following disables any warnings
# that might otherwise show in the scriptlog files on the Advanced SQL Engine
# nodes in this case. Yet, do keep an eye for incompatible pickle versions.
warnings.filterwarnings('ignore')

# Know your data: You must know in advance the number and data types of the
# incoming columns from the SQL Engine database!
# For this script, the input expected format is:
colNames = ['customer_id',
 'session_id',
 'Web Chat_prior_1_event',
 'Store Visit_prior_1_event',
 'Purchase_prior_1_event',
 'Neutral Call_prior_1_event',
 'Product Return_prior_1_event',
 'Return Policy Inquiry_prior_1_event',
 'Service Inquiry_prior_1_event',
 'Product Browsing_prior_1_event',
 'Complaint Call_prior_1_event',
 'Online Feedback_prior_1_event',
 'Service Inquiry_prior_2_event',
 'Web Chat_prior_2_event',
 'Product Browsing_prior_2_event',
 'Online Feedback_prior_2_event',
 'Complaint Call_prior_2_event',
 'Neutral Call_prior_2_event',
 'Return Policy Inquiry_prior_2_event',
 'Store Visit_prior_2_event',
 'b_cancel',
 'b_gender',
 'sum_polarity_POS',
 'sum_polarity_NEG',
 'sum_polarity_NEU',
 'sampleid']



model = XGBClassifier()
booster = Booster()
booster.load_model('xgb_churn_model')
model._Booster = booster


d = csv.DictReader(sys.stdin.readlines(), fieldnames = colNames)

df = pd.DataFrame(d, columns = colNames)

# Use try...except to produce an error if something goes wrong in the try block
try:
    # Exit gracefully if DataFrame is empty
    if df.empty:
        sys.exit()

    # Specify the rows to be scored by the model and call the predictor.
    X_test = df[df.columns.drop(['session_id', 'customer_id', 'b_cancel', 'sampleid'])].astype(float)
    
    y_prob = model.predict_proba(X_test)
    df[['prob_0', 'prob_1']] = y_prob
    
    y_pred = model.predict(X_test)
    df['prediction'] = y_pred

    # Export results to the Database through standard output.
    for index, value in df.iterrows():
        my_str = str(value['customer_id']) + ',' + str(value['session_id']) + ',' + str(value['prob_0']) + ',' + str(value['prob_1']) + ',' + str(value['prediction']) + ',' + str(value['b_cancel'])
        print(my_str)
        

except (SystemExit):
    # Skip exception if system exit requested in try block
    pass
except:    # Specify in standard error any other error encountered
    print("Script Failure :", sys.exc_info()[0], file=sys.stderr)
    raise
    sys.exit()
