import streamlit as st
# st.set_page_config(layout = 'centered')
import random
import time
import pandas as pd

from teradatagenai import VectorStore, VSPattern, VSManager
from teradataml import *


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
    
# st.markdown('<style>' + open('style.css').read() + '</style>', unsafe_allow_html=True)


# VSM = st.session_state['VSM']

# check for refresh flag in session
if 'refresh' not in st.session_state or st.session_state['refresh'] == 1:
    try:
        st.session_state['vs_list'] = get_vs_list('data_scientist')
        st.session_state['refresh'] = 0

    except TeradataMlException as e:
        st.write('VS Services not available')
        st.write(e)
        st.stop()
    


with st.sidebar:
    st.write('''Available Vector Stores:''')
    
    st.write(st.session_state['vs_list'][['vs_name']])
    
    if st.button('Refresh', use_container_width = True, type = 'tertiary'):
        st.session_state['refresh'] = 1
        
    option = st.selectbox('Select a Vector Store', st.session_state['vs_list']['vs_name'].to_list(), index = None)
    
    if option:
        st.session_state['vs'] = get_vs(option)

        st.write('''Current Vector Store Status:''')
        st.write(st.session_state['vs'].status())



st.title('Secure content search and response generation')

st.header('Search docs and data', divider="gray")
term = ''
term = st.text_input('Search term')
if term != '':
    res = similarity(term)
    df = res.similar_objects.to_pandas().drop(['DataBaseName', 'TableName','index_label'], axis = 1)
    st.write(df)


st.header('Generate responses based on the results', divider="gray")
st.markdown('Use the optional keyword format: to define a response format')


# Initialize chat history
if 'search_messages' not in st.session_state:
    st.session_state['search_messages'] = [{"role": "assistant", "content": "Ask about the data"}]

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
        assistant_response = 'Constructing search results'
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
