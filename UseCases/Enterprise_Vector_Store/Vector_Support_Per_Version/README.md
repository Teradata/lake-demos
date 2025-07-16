<header>
   <p  style='font-size:36px;font-family:Arial; color:#F0F0F0; background-color: #00233c; padding-left: 20pt; padding-top: 20pt;padding-bottom: 10pt; padding-right: 20pt;'>
       Vector analytics and AI functionality per database version - Overview and Setup
  <br>
       <img id="teradata-logo" src="https://storage.googleapis.com/clearscape_analytics_demo_data/DEMO_Logo/teradata.svg" alt="Teradata" style="width: 125px; height: auto; margin-top: 20pt;">
    </p>
</header>

<hr>

<p style = 'font-size:28px;font-family:Arial;color:#00233C'><b>Overview</b></p>

<p style = 'font-size:16px;font-family:Arial;color:#00233C'>Teradata Vantage provides a suite in-database analytic capabilities for Vector embedding and analytics with support across multiple database versions.  This notebook series reviews these capablities per database version, including:</p>

<p style = 'font-size:18px;font-family:Arial;color:#00233C'><b>Database Version 17.20+ and VantageCloud Enterprise 3.0</b>
<ul style = 'font-size:16px;font-family:Arial;color:#00233C'>
    <li><b>Bring-Your-Own-Model (BYOM)</b> capabilities allow users to generate vector embeddings using open-source models serialized as ONNX format</li>
    <li><b>Vector data</b> stored as FLOAT columns in normal database tables</li>
    <li><b>Similarity analysis</b> using native ClearScape Analytics functions - <b>Vector Distance</b> and <b>KMeans</b></li>
    </ul>
    
<p style = 'font-size:18px;font-family:Arial;color:#00233C'><b>VantageCloud Enterprise 3.1+</b>
<ul style = 'font-size:16px;font-family:Arial;color:#00233C'>
    <li><b>AI Analytic Functions</b> that leverage <b>Cloud-based LLMs</b> for text analytics, including Vector Embedding functions and RAG</li>
    <li><b>VECTOR Datatype</b> Varbyte-based array of vector data stored as single column</li>
    <li><b>Normalization</b> of vector data for efficient similarity analysis</li>
    <li><b>Similarity analysis</b> using VECTOR DATATYPE and additional functions</li>
    </ul>
    
<p style = 'font-size:18px;font-family:Arial;color:#00233C'><b>VantageCloud Lake</b>
<ul style = 'font-size:16px;font-family:Arial;color:#00233C'>
    <li><b>In-platform GPUs</b> leveraging Analytic Compute Clusters for high-scale vector embedding and other Large Language Model tasks</li>
    <li><b>Enterprise Vector Store APIs</b> for creating and managing vector data using Python and/or REST</li>
    <li><b>Similarity Search and RAG APIs</b> using Python</li>
    <li><b>Vector Store UI</b> for managing vector data</li>
    </ul>
    
<hr>
<p style = 'font-size:28px;font-family:Arial;color:#00233C'><b>Demo Setup</b></p>

<p style = 'font-size:16px;font-family:Arial;color:#00233C'>The demo notebooks in this folder require several prerequisites:</p>
<p style = 'font-size:18px;font-family:Arial;color:#00233C'><b>Version 17.20 Notebook</b></p>
<ol style = 'font-size:16px;font-family:Arial;color:#00233C'>
    <li>Target system must have BYOM v6+ installed</li>
    <li>Use a vars.json file that contains the proper host, username, and password to connect to the system - or overwrite these values in the notebook</li>
    <li>Load the data if needed - the "0_Load_Getting_Started_Data.ipynb" will upload the small demo dataset</li>
    </ol>
    
<p style = 'font-size:18px;font-family:Arial;color:#00233C'><b>Version 3.1 Notebook</b></p>
<ol style = 'font-size:16px;font-family:Arial;color:#00233C'>
    <li>Target system must be on Vantage version 3.1 or greater</li>
    <li>Use a vars.json file that contains the proper host, username, and password to connect to the system - or overwrite these values in the notebook</li>
    <li>Load the data if needed - the "0_Load_Getting_Started_Data.ipynb" will upload the small demo dataset</li>
    <li>Many functions make use of an external AWS account that has Bedrock model access.  Ensure the user creates an Authorization Object that contains the access keys/secrets for authentication and authorization for AWS</li>
    <li>Some examples use environment variables to pass AWS credentials.  Fix up the values in the example "env" file and copy it to a file named ".env"</li>
    </ol>
    