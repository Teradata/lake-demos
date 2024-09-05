#!/usr/bin/env python3
import sys, csv
import warnings
import torch 
from sentence_transformers import SentenceTransformer
import pandas as pd

warnings.simplefilter('ignore')


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
        
    
    model = SentenceTransformer('BAAI/bge-small-en-v1.5')
    #################################
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    
    model = model.to(device) #<-- let's load the model once in the gpu (when gpu available)
    #################################



# Read data from stdin, and construct a Pandas DataFrame #
# Data can also be read/processed directly from stdin i
    def create_embeddings(texts):
        # Encode texts using the model in batches
        with torch.no_grad(): # just to be sure, I am not sure if gradient computations are turned off by SentenceTransformer
            embeddings = model.encode(texts, device=device, batch_size=10, convert_to_tensor=True)
        return embeddings.cpu().numpy() # here we go back to cpu
    ####################################################


    ########################################################
    # There is no need to use a apply and do row by row
    texts = df['text'].tolist()
    embeddings = create_embeddings(texts)
    # Now the embeddings are calculated, we concatenate them in the dataframe:
    embedding_cols = [f'V{i}' for i in range(embeddings.shape[1])]
    embedding_df = pd.DataFrame(embeddings, columns=embedding_cols)
    df = pd.concat([df[['id']], embedding_df], axis=1)

    

    # Egress results to the Database through standard output.
    # iterrrows generates a Series, iterate through the series to construct
    # a comma-separated output string
    for index, value in df.iterrows():
        my_str = ''
        for val in value.index:
            my_str = my_str + str(value[val]) + ','
        print(my_str[:-1])
        
# raise any errors back to the SQL engine
except (SystemExit):
    # Skip exception if system exit requested in try block
    pass
except:    # Specify in standard error any other error encountered
    print("Script Failure :", sys.exc_info()[0], file=sys.stderr)
    raise
    sys.exit()