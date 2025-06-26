# Basic Chat App #


## Teradata Enterprise Vector Store ##
This basic Streamlit application allows users the ability to select existing vector stores, and perform natural language chat.

Chat capabilities include:
1. Similarity Search (similarity_search())
2. Preparing a generative response using the results (prepare_response() )
3. Simplified search and RAG (ask() )

Additionally, a "Patient Satisfaction Analyzer" has been provided to showcase additional LLM analytics and generative features

### Installation ###

1. Clone the lake-demos repo
```
git clone https://github.com/Teradata/lake-demos.git
```
2. Navigate to the lake-demos/UseCases/Enterprise_Vector_Store/chat_app/ folder
```
cd lake-demos/UseCases/Enterprise_Vector_Store/chat_app/
```
3. Create a Python virtual environment
```
python -m venv .venv
```
4. Activate the virtual environment
```
source .venv/bin/activate
```
5. Install required packages
```
pip install -r requirements.txt
```
6. Copy the env file to .env and fix up the values
```
cp env .env
```
7. Run the streamlit app
```
streamlit run main.py
```

### Notes ###
- Be sure the user has the correct PAT value and the .pem key is in the proper file path or local directory
- This is designed to be used for short periods of time - there's no built-in database reconnect, etc.
- The patient Satisfaction Analyzer assumes an existing Vector Store named 'Transcript_Analysis' and a database table "demo.patient_sentiment".