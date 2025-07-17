import streamlit as st
# st.set_page_config(layout = 'centered')
import random
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from teradatagenai import VectorStore, VSPattern, VSManager
from teradataml import *
from dotenv import load_dotenv

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


        


@st.cache_resource(show_spinner = False)
def get_tdf(table_name):
    return DataFrame(table_name)

@st.cache_resource(show_spinner = False)
def get_sentiment_hist(term):
    return Histogram(data = st.session_state['sentiment_tdf'].loc[st.session_state['sentiment_tdf']['label'] == term], target_columns = 'score',method_type = 'SCOTT').result.to_pandas()

@st.cache_resource(show_spinner = 'Fetching the Vector Store')
def get_vs(name, *args):
    return VectorStore(name)

def get_vs_list(db, *args):
    return VSManager.list()[VSManager.list()['database_name'] == db].to_pandas()

@st.cache_resource(show_spinner = 'Retrieving Similarity Results')
def similarity(term):
    return st.session_state['vs'].similarity_search(question=term)

@st.cache_data(show_spinner = False)
def final_response(q, p=None):
    if p is None:
        return st.session_state['vs'].prepare_response(question = q, similarity_results=res)
    else:
        return st.session_state['vs'].prepare_response(question = q, prompt = p, similarity_results=res)
    

st.session_state['eng'] = cc()    
# st.markdown('<style>' + open('style.css').read() + '</style>', unsafe_allow_html=True)



if 'sentiment_tdf' not in st.session_state:
    st.session_state['sentiment_tdf'] = get_tdf('"demo"."patient_sentiment"')

if 'vs' not in st.session_state:
    st.session_state['vs'] = get_vs('Transcript_Analysis')
elif st.session_state['vs'].name != 'Transcript_Analysis':
    st.session_state['vs'] = get_vs('Transcript_Analysis')

# VSM = st.session_state['VSM']

# check for refresh flag in session
# if 'refresh' not in st.session_state or st.session_state['refresh'] == 1:
#     try:
#         st.session_state['vs_list'] = get_vs_list('ks250119')
#         st.session_state['refresh'] = 0

#     except TeradataMlException as e:
#         st.write('VS Services not available')
#         st.write(e)
#         st.stop()
    


# with st.sidebar:
#     st.write('''Available Vector Stores:''')
    
#     st.write(st.session_state['vs_list'][['vs_name']])
    
#     if st.button('Refresh', use_container_width = True, type = 'tertiary'):
#         st.session_state['refresh'] = 1
        
#     option = st.selectbox('Select a Vector Store', st.session_state['vs_list']['vs_name'].to_list(), index = None)
    
#     if option:
#         st.session_state['vs'] = get_vs(option)

#         st.write('''Current Vector Store Status:''')
#         st.write(st.session_state['vs'].status())


st.title('Patient Satisfaction Dashboard')

st.header('Overall Patient Sentiment', divider="gray")

option = st.selectbox('select', ['joy','anger','sadness','fear'], index = None, label_visibility = 'collapsed')

if option:
    local_df = get_sentiment_hist(option)
    local_df['MaxValue'] = local_df['MaxValue'].round(decimals=3)
    local_df = local_df.set_index('MaxValue')
    fig = px.bar(local_df.sort_index(), x = local_df.index, y = 'Bin_Percent',
                 labels = {'x':'Emotion Strength','Bin_Percent':'Percent of Patients'},
                 title = f'Sentiment Strength Distribution for {option}', height=500)
    st.plotly_chart(fig)

st.header('Patient Outreach', divider="gray")

term = ''
term = st.text_input('Search Patient Call Center Transcripts')
if term != '':
    res = similarity(term)
    df = res.similar_objects.to_pandas().drop(['DataBaseName', 'TableName','index_label'], axis = 1)
    st.write(df)


# st.markdown('Ask detailed questions of the results')
#st.markdown('Use the optional keyword format: to define a response format')


# Initialize chat history
if 'search_messages' not in st.session_state:
    st.session_state['search_messages'] = [{"role": "assistant", "content": "Ask about your patients' feedback"}]

# Display chat messages from history on app rerun
for message in st.session_state['search_messages']:
    with st.chat_message(message["role"]):
        # st.markdown(message["content"])
        st.write(message['content'])

# Accept user input
if prompt := st.chat_input("Ask your question"):
    # Add user message to chat history
    st.session_state['search_messages'].append({"role": "user", "content": prompt})
    # Display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)
        

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        assistant_response = 'Generating Response'
        # Simulate stream of response with milliseconds delay
        for chunk in assistant_response.split():
            full_response += chunk + " "
            time.sleep(0.05)
            # Add a blinking cursor to simulate typing
            message_placeholder.markdown(full_response + "▌")
        message_placeholder.markdown(full_response)
        
        # parse question and prompt
        if 'format:' in prompt: #we have a formatting response
            question = prompt.split('format:')[0]
            fm = prompt.split('format:')[1]
            final_res = final_response(question, fm)
            st.write(final_res)
        else:
            final_res = final_response(prompt)
            st.write(final_res)
        
    # Add assistant response to chat history
    st.session_state['search_messages'].append({"role": "assistant", "content": full_response})
    st.session_state['search_messages'].append({"role": "assistant", "content": final_res})
