import streamlit as st
# st.set_page_config(layout = 'wide')
import random, os, time
from dotenv import load_dotenv
import pandas as pd

from teradatagenai import VectorStore, VSPattern, VSManager
from teradataml import *

# @st.cache_resource
# def get_VSManager():
#     from teradatagenai import VSManager
#     return VSManager

load_dotenv()

@st.cache_resource
def cc():

    hostname = os.getenv('hostname')
    username = os.getenv('username')
    password = os.getenv('password')
    pat = os.getenv('pat')
    pem_file = os.getenv('pem_file')
    base_url = os.getenv('base_url')

    context=create_context(host=hostname, username=username, password=password)
    set_auth_token(base_url=base_url, pat_token=pat, pem_file=pem_file)

    
    return context


        
# eng = cc()
# VSM = get_VSManager()

st.session_state['eng'] = cc()
# st.session_state['VSM'] = get_VSManager()

# Define the pages
main_page = st.Page("search.py", title = 'Content Search and RAG')
page_1 = st.Page("ask.py", title = 'Exploratory RAG')
page_2 = st.Page("Customer_Dashboard.py", title = 'Patient Satisfaction Dashboard')

# Set up navigation
pg = st.navigation([main_page, page_1, page_2])

# Run the selected page
pg.run()