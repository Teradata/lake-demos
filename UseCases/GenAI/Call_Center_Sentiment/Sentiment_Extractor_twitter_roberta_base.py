#!/usr/bin/env python3
import sys, csv, os
import warnings
import torch 
import pandas as pd
from transformers import pipeline, AutoModelForSequenceClassification, TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
from transformers.utils import logging
warnings.simplefilter('ignore')
logging.set_verbosity_error() 

# model_name = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
model_path = os.environ.get('OPENAF_MODELS_DIR') + '/twitter-roberta-base-sentiment-latest'

# 1. use the csv reader to parse comma-separated input
# 2. construct the Dataframe from the resulting dictionary

colNames = ['id', 'text']
d = csv.DictReader(sys.stdin.readlines(), fieldnames = colNames)
df = pd.DataFrame(d, columns = colNames)

# Use try...except to produce an error if something goes wrong in the try block
try:
    # Exit gracefully if DataFrame is empty
    # It is possible some partitions won't get any data
    if df.empty:
        sys.exit()
        
    
    #################################
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = model = AutoModelForSequenceClassification.from_pretrained(model_path)
    sentiment_task = pipeline("sentiment-analysis", model = model , tokenizer = tokenizer, device = device)
    #################################

    def get_sentiment(row):
        #sentiments = sentiment_task(texts, batch_size = 10)
        return pd.Series(sentiment_task(row['text'])[0])
        #return sentiments

   # texts = df['text'].tolist()
   # sentiments = get_sentiment(texts)
    
    # sentiment_df = pd.DataFrame(sentiments, columns=['result'])
    
    s_df = df.apply(get_sentiment, axis = 1)
    df = pd.concat([df[['id']], s_df], axis=1)
    # Egress results to the Database through standard output.
    df.to_csv(sys.stdout, index=False, header=False)
        
# raise any errors back to the SQL engine
except (SystemExit):
    # Skip exception if system exit requested in try block
    pass
except:    # Specify in standard error any other error encountered
    print("Script Failure :", sys.exc_info()[0], file=sys.stderr)
    raise
    sys.exit()