import pandas as pd
import streamlit as st
#from streamlit_pandas_profiling import st_profile_report
from ydata_profiling import ProfileReport
from PIL import Image
import re
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ResourceNotFoundError, ServiceRequestError
from botocore.exceptions import ClientError
import boto3
import time
import pyodbc
import sqlalchemy as sqal
import io
from sqlalchemy.exc import SQLAlchemyError
import requests
import base64
import json
from SessionStateVar import *
import os
import oracledb

# Load environment variables for Azure authentication
client_id = os.getenv("client_id")
tenant_id = os.getenv("tenant_id")
client_secret = os.getenv("client_secret")
spn_id = os.getenv("spn_id")
ip_range = os.getenv("ip_range")

def on_change_flat_file(key):
    """
    Reset appropriate session state based on the file upload key.
    
    Parameters:
    key (str): Identifier for the uploaded file type.
    """
    if key =='RuleProfilingFlat':
        reset_dataframe()
    if key=='SourceTableFile':
        reset_crosscolumn_df1()
    if key=='ReferenceTableFile':
        reset_crosscolumn_df2()
    if key == 'flat':
        reset()
    if key=='a':
        reset_joinprofile_df1()
    if key=='e':
        reset_joinprofile_df2()

def sql_spn_change():
    """
    Set a flag in session state to indicate SQL SPN change.
    """
    if 'spn_sql' not in st.session_state:
        st.session_state.spn_sql = 1

def sql_table_select(source):
    """
    Set session state flags based on the selected SQL data source.
    
    Parameters:
    source (str): Identifier for the data source.
    """
    if source == 'eda_source_data':
        if 'src_select_widget_change_eda_source' not in st.session_state:
            st.session_state.src_select_widget_change_eda_source = True

    elif source == 'reference_source':
        if 'src_select_widget_change_reference_CTP' not in st.session_state:
            st.session_state.src_select_widget_change_reference_CTP = True

    elif source=='source_data':
        if 'src_select_widget_change_source_CTP' not in st.session_state:
            st.session_state.src_select_widget_change_source_CTP = True

    elif source=='join_source_data':
        if 'src_select_widget_change_source_JOIN' not in st.session_state:
            st.session_state.src_select_widget_change_source_JOIN = True

    elif source=='join_reference_data':
        if 'src_select_widget_change_reference_JOIN' not in st.session_state:
            st.session_state.src_select_widget_change_reference_JOIN = True

    elif source=='ccp_source_data':
        if 'src_select_widget_change_CCP' not in st.session_state:
            st.session_state.src_select_widget_change_CCP = True

    elif source=='brp_source_data':
        if 'src_select_widget_change_BRP' not in st.session_state:
            st.session_state.src_select_widget_change_BRP = True

def set_verify_api():
    """
    Set a flag in session state to indicate API verification.
    """
    st.session_state.verify_api=1

def set_normalize_json():
    """
    Set a flag in session state to indicate JSON normalization.
    """
    st.session_state.normalize_json=1  

def set_text_input():
    """
    Clear flags related to API verification and JSON normalization.
    """
    if 'verify_api' in st.session_state:
        del st.session_state.verify_api
    if 'normalize_json' in st.session_state:
        del st.session_state.normalize_json

def flat(key,source=None):
    """
    Upload and read flat files (CSV, Excel, Parquet) into a DataFrame.

    Parameters:
    key (str): Unique key for the file uploader widget.
    source (str, optional): Source identifier for session state updates.

    Returns:
    tuple: DataFrame and file name, or (None, None) if upload fails.
    """
    try:
        uploaded_file = st.file_uploader("Source Data",type=["csv","xls","xlsx", "xlsm", "xlsb","parquet"], key=key,on_change=on_change_flat_file,args=(key,))

        
        if uploaded_file is not None:
            
            if uploaded_file.name.split('.')[1] in ['xls', 'xlsx', 'xlsm', 'xlsb']:
                wb = pd.ExcelFile(uploaded_file)
                sheet = st.selectbox('Pick the Sheet Name', wb.sheet_names, key=key+'Sheet',on_change=sql_table_select,args=(source,))
                df=pd.read_excel(uploaded_file,sheet_name=sheet,na_values=[ '', '#N/A', '#N/A N/A', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NULL', 'NaN', 'None', 'n/a', 'nan', 'null'],keep_default_na = False)
                return df,uploaded_file.name
            elif uploaded_file.name.split('.')[1] == 'parquet':
                df = pd.read_parquet(uploaded_file)
                return df,uploaded_file.name
            else:
                #other flat files(with selection box)
                sepe = st.selectbox('Select Delimiter', [',','|',r'\t'],key=key+'Delimiter',on_change=sql_table_select,args=(source,))
                df = pd.read_csv(uploaded_file,encoding_errors= 'replace',sep=sepe,na_values=[ '', '#N/A', '#N/A N/A', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NULL', 'NaN', 'None', 'n/a', 'nan', 'null'],keep_default_na = False)
                return df,uploaded_file.name
        else:
            #st.write('Please upload a File') 
            return None,None
    except Exception as e:
        # Handle any unexpected errors
        st.error("An unexpected error occurred Happened, please check the file type is supported or not")
        return None,None

def adl(key,source=None):
    """
    Connect to Azure Data Lake Storage (ADLS), authenticate using SPN credentials,
    and load a selected file (CSV, Excel, or Parquet) into a pandas DataFrame.

    Parameters:
    key (str): Unique identifier for Streamlit widgets and session state.
    source (str, optional): Source identifier for session state updates.

    Returns:
    tuple: DataFrame and file name, or (None, None) if loading fails.
    """
    #Session Variable to maintain their old state(to stop it from rerunning from the start)
    if key+"Sub_ADLS" not in st.session_state:
        st.session_state[key+'Sub_ADLS'] = False
    #creating one form for credentials
        
    with st.form(key+"my_blob_form"):
        st.subheader("Specify the Credentials:")
        #storing all credentials
        #SPN ID
        st.write(f"**Please add this SPN ID to your Azure Resource:- {spn_id}**")
        st.write(f"**Please add this IP range and IP address in your ADLS:- {ip_range}**")
        account_name = st.text_input('Storage Account Name',placeholder='Enter the Storage account Name', key=key+'AccountName') 
        # account_key = st.text_input('Access Key',placeholder='Enter the Access Key', key=key+'AccountKey')
        container_name = st.text_input('Container Name',placeholder='Enter the your username', key=key+'ContainerName')
        st.write("Taking too long? you can enter the file path as well")
        file_path = st.text_input('File Path',placeholder='Enter the File Path', key=key+'file_path')
        sub = st.form_submit_button("Submit")
        # sub = st.form_submit_button("Submit", key=key+'ADLSSubmit')
        #checking if all the fields are filled or not
        if all([account_name, sub,container_name]):
            st.session_state[key+'Sub_ADLS'] = True
        else:pass
    
    #checking for session state
    if key+'Sub_ADLS' in st.session_state and st.session_state[key+'Sub_ADLS'] == True:
        # st.session_state['Sub_ADLS'] = False
        #creating a connection string for blob
        try:
            # Tenant ID for your Azure Subscription
            TENANT_ID = tenant_id

            # Your Service Principal App ID (Client ID)
            CLIENT_ID = client_id

            # Your Service Principal Password (Client Secret)
            CLIENT_SECRET = client_secret

            credentials = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)

            blobService = BlobServiceClient(
            "https://{}.blob.core.windows.net".format(account_name),
            credential=credentials
            )
            # connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
            # blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            #if condition for file path if already exist
           
            if file_path:
                blob_client = blobService.get_blob_client(container=container_name, blob=file_path)
                # Download the blob data
                blob_data = blob_client.download_blob()
                # Read data into pandas DataFrame
                if file_path.split("/").pop().split('.')[1]=='csv':
                    sepe = st.selectbox('Select Delimiter', [',','|',r'\t'],key=key+'Delimiter',on_change=sql_table_select,args=(source,))
                    df = pd.read_csv(io.StringIO(blob_data.readall().decode('utf-8')),on_bad_lines = 'skip', sep = sepe)
                    return df,file_path.split("/").pop()
                elif file_path.split("/").pop().split('.')[1]=='parquet':
                    df = pd.read_parquet(io.BytesIO(blob_data.readall()))
                    return df,file_path.split("/").pop()
                elif file_path.split("/").pop().split('.')[1] in ['xls', 'xlsx', 'xlsm', 'xlsb']:
                    wb = pd.ExcelFile(io.BytesIO(blob_data.readall()))
                    sheet = st.selectbox('Pick the Sheet Name', wb.sheet_names, key=key+'Sheet',on_change=sql_table_select,args=(source,))
                    df = wb.parse(sheet)
                    return df,file_path.split("/").pop()
                else:
                    try: 
                        df = pd.read_parquet(io.BytesIO(blob_data.readall()))
                        return df,file_path.split("/").pop()
                    except Exception as e:
                        return None,None 
            else:
                container_client = blobService.get_container_client(container_name)
                # if container_client is not None and container_client:
                #     st.write("Connection Established")
                blob_list = []
                #storing all the files and directory in a list in blob
                for blob_i in container_client.list_blobs():
                    blob_list.append(blob_i.name)
                #checking for only csv files in blob
                filtered_values = list(filter(lambda v: re.match(r'(.*)\.(csv|parquet|xls|xlsx|xlsm|xlsb)', v), blob_list))
                #creating one select widget for the available file names with their path
                file_path = st.selectbox("Select File with Path for Profiling", filtered_values, key=key+'ADLSPath',on_change=sql_table_select,args=(source,))
                # Get a reference to the blob
                blob_client = blobService.get_blob_client(container=container_name, blob=file_path)
                # Download the blob data
                blob_data = blob_client.download_blob()
                # Read data into pandas DataFrame
                if file_path.split("/").pop().split('.')[1]=='csv':
                    sepe = st.selectbox('Select Delimiter', [',','|',r'\t'],key=key+'Delimiter',on_change=sql_table_select,args=(source,))
                    df = pd.read_csv(io.BytesIO(blob_data.readall()),on_bad_lines = 'skip',sep = sepe)
                    return df,file_path.split("/").pop()
                elif file_path.split("/").pop().split('.')[1]=='parquet':
                    df = pd.read_parquet(io.BytesIO(blob_data.readall()))
                    return df,file_path.split("/").pop()
                elif file_path.split("/").pop().split('.')[1] in ['xls', 'xlsx', 'xlsm', 'xlsb']:
                    wb = pd.ExcelFile(io.BytesIO(blob_data.readall()))
                    sheet = st.selectbox('Pick the Sheet Name', wb.sheet_names, key=key+'Sheet',on_change=sql_table_select,args=(source,))
                    df = wb.parse(sheet)
                    return df,file_path.split("/").pop()
                else:return None,None
        except ResourceNotFoundError as ex:
            st.write("Resource not found, Please select Correct Resource")
            
            return None,None
        except ServiceRequestError as ex:
            st.write("Service request error, Please check the Credentials")
            
            return None,None
        except Exception as ex:
            st.write("An unexpected error occurred, Please check the Credentials")
            
            return None,None
    else:return None,None

def s3_buc(key,source=None):
    """
    Connect to an AWS S3 bucket, authenticate using provided credentials,
    and load a selected file (CSV, Excel, or Parquet) into a pandas DataFrame.

    Parameters:
    key (str): Unique identifier for Streamlit widgets and session state.
    source (str, optional): Source identifier for session state updates.

    Returns:
    tuple: DataFrame and file name, or (None, None) if loading fails.
    """
    #Session Variable to maintain their old state(to stop it from rerunning from the start)
    if key+"Sub_S3" not in st.session_state:
        st.session_state[key+'Sub_S3'] = False

    #creating one form for credentials
    with st.form(key+"my_S3_form"):
        #storing all credentials
        st.subheader("Specify the Credentials:")
        access_id = st.text_input('Access Key ID',placeholder='Enter the Access Key ID', key=key+'AccessID') 
        secret_key = st.text_input('Secret Access Key',placeholder='Enter the Secret Access Key', type="password",key=key+'SecretKey')
        bucket_name = st.text_input('Bucket Name',placeholder='Enter Bucket Name', key=key+'BucketName')
        region = st.text_input('Region Name',placeholder='Enter the Region', key=key+'Region')
        dire = st.text_input('Directory Name',placeholder='Enter Directory Name', key=key+'Directory')
        sub = st.form_submit_button("Submit")
        # sub = st.form_submit_button("Submit", key=key+'S3Submit')
        #checking if all the fields are filled or not
        if all([access_id, secret_key, bucket_name, region, sub,dire]):
            st.session_state[key+'Sub_S3'] = True
        else:pass
     #checking for session state
    if key+"Sub_S3" in st.session_state and st.session_state[key+'Sub_S3'] == True:
        try:
        #creating a session string for S3
            ses = boto3.Session(
            aws_access_key_id = access_id,
            aws_secret_access_key = secret_key,
            region_name = region    
            )
            s3 = ses.resource('s3')
            #entering the bucket name
            my_bucket = s3.Bucket(bucket_name)
            s3_list=[]
            # c=0
            #storing all the files and directory in a list in S3 Bucket
            for my_bucket_object in my_bucket.objects.filter(Prefix = dire+'/'):
                s3_list.append(my_bucket_object.key)
                # c+=1
                # if c==1000:
                #     break
            #checking for only csv files in S3
            # print(s3_list)
            filtered_values_2 = list(filter(lambda v: re.match(r'(.*)\.(csv|parquet|xls|xlsx|xlsm|xlsb)', v), s3_list))
            print(filtered_values_2)
            file_path_2 = st.selectbox("Select File with Path for Profiling", filtered_values_2, key=key+'FilePath',on_change=sql_table_select,args=(source,))
            #Creating one Boto client(S3) for reading a particular object with key(you can say the selected CSV)
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=access_id,
                aws_secret_access_key=secret_key,
                region_name = region,
            )
            #reading the file here
            response = s3_client.get_object(Bucket=bucket_name, Key=file_path_2)
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            #checking for success status
            if status == 200:
                print(f"Successful S3 get_object response. Status - {status}")
                if file_path_2.split("/").pop().split('.')[1]=='csv':
                    sepe = st.selectbox('Select Delimiter', [',','|',r'\t'],key=key+'Delimiters3',on_change=sql_table_select,args=(source,))
                    try:
                        df = pd.read_csv(io.BytesIO(response["Body"].read()),encoding='cp1252',sep=sepe)
                    except Exception as e:
                        st.warning("Couldn't parse the data. Please select different seperator")
                    return df,file_path_2.split("/").pop()
                elif file_path_2.split("/").pop().split('.')[1]=='parquet':
                    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
                    return df,file_path_2.split("/").pop()
                elif file_path_2.split("/").pop().split('.')[1] in ['xls', 'xlsx', 'xlsm', 'xlsb']:
                    wb = pd.ExcelFile(io.BytesIO(response["Body"].read()))
                    sheet = st.selectbox('Pick the Sheet Name', wb.sheet_names, key=key+'Sheet',on_change=sql_table_select,args=(source,))
                    df = wb.parse(sheet)
                    return df,file_path_2.split("/").pop()
            else:
                print(f"Unsuccessful S3 get_object response. Status - {status}"); return None,None
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
                st.write("The specified bucket does not exist, Please check enter the correct Bucket Name")
                return None,None
            elif e.response['Error']['Code'] == 'NoSuchKey':
                print("The specified key does not exist.")
                st.write("The specified key does not exist in the registery, Please enter the correct key")
                return None,None
            else:
                print("An error occurred:", e)
                st.write("An error occurred")
                return None,None
        except Exception as e:
            print("An unexpected error occurred:", e)
            st.write("An unexpected error occurred")
            return None,None
    else:return None,None

def sdb(key,source=None):
     """
     Connect to an Azure SQL Database using either SQL Authentication or Azure SPN,
     and load a selected table into a pandas DataFrame.

     Parameters:
     key (str): Unique identifier for Streamlit widgets and session state.
     source (str, optional): Source identifier for session state updates.

     Returns:
     tuple: DataFrame and table name, or (None, None) if loading fails.
     """
     if key+"Sub_sql" not in st.session_state:
        st.session_state[key+'Sub_sql'] = False
     st.write('**Select Authentication Method**')
     check = st.selectbox('', ['Azure SQL Authentication', 'Azure SPN'],key=key+'azuresqlauth')
    #  check = st.checkbox('Azure AD MFA', key=key+'checkbox')
     if check =='Azure SPN':
          with st.form(key+"my_admfa_form"):
            #storing all credentials
            st.subheader("Specify the Credentials:")
            st.write(f"**Please add this SPN ID to your Azure Resource:- {spn_id}**")
            st.write(f"**Please add this IP range and IP address in your ADLS:- {ip_range}**")
            server_name = st.text_input("Enter Server Name", key=key+'AzureServerName')
            database_name = st.text_input("Enter Database Name",key=key+'AzureDatabaseName')
            # username = st.text_input("Enter Username",key=key+'AzureUserName')
            # authentication = 'ActiveDirectoryInteractive'
            sub = st.form_submit_button("Connect")
            # sub = st.form_submit_button("Connect", key= key+'SDBsubmit')
            #checking if all the fields are filled or not
            if all([server_name, database_name,sub]):
                st.session_state[key+'Sub_sql'] = True
            else:
                pass
    #creating one form for credentials
     else:
        with st.form(key+"my_sql_form"):
            #storing all credentials
            st.subheader("Specify the Credentials:")
            server_sql = st.text_input('Server Name',placeholder='Enter Server Name', key=key+'ServerName') 
            db = st.text_input('Database Name',placeholder='Enter the Database Name', key=key+'DBName')
            ui = st.text_input('Username',placeholder='Enter User Name',key=key+'UserName')
            pw = st.text_input('Password',placeholder='Enter your Password', type="password",key=key+'Pwd')
            port = st.text_input('Port Number',placeholder='Enter Server Port Number', key=key+'PortNumber')
            sub = st.form_submit_button("Submit")
            # sub = st.form_submit_button("Submit", key=key+'SQLSubmit')
            #checking if all the fields are filled or not
            if all([server_sql, db, ui, pw, sub]):
                st.session_state[key+'Sub_sql'] = True
                server_sql=f'tcp:{server_sql},{port}'
            else:pass
     #checking for session state
     if key+"Sub_sql" in st.session_state and st.session_state[key+'Sub_sql'] == True and check=='Azure SQL Authentication':
        #creating a session string for S3
        try:
            driver= '{ODBC Driver 17 for SQL Server}'
            conn_str = f'Driver={driver};SERVER={server_sql};DATABASE={db};Uid={ui};Pwd={pw};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Auto_Commit=true;'
            engine = sqal.create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}', fast_executemany=True)
            conn = engine.connect()
            if conn:
                sql_table = list(set([j[0] for j in conn.execute(sqal.text("select TABLE_NAME from INFORMATION_SCHEMA.TABLES")).fetchall()]))
                tab = st.selectbox("Select Table", sql_table,on_change=sql_table_select,args=(source,))
                try:
                    data = pd.read_sql(f"SELECT * FROM {tab}",conn)
                    return data,tab
                except Exception as e:
                    st.write("Current Table have no access, Change your table")
                    return None,None
            else:return None,None
        except SQLAlchemyError as e:
            print("SQLAlchemy Error:", e)
            st.write("SQLAlchemy Error, Please check the credentials and user access")
            return None,None
        except Exception as e:
            print("An unexpected error occurred:", e)
            st.write("An unexpected error occurred:, Please check the credentials and user access")
            return None,None
     elif key+"Sub_sql" in st.session_state and st.session_state[key+'Sub_sql'] == True and check=='Azure SPN':
           try:
                Client_id = client_id
                Client_secret = client_secret
                Tenant_id = tenant_id
                connection_string = (
                        f'DRIVER={{ODBC Driver 17 for SQL Server}};'\
                        f'SERVER={server_name};'\
                        f'DATABASE={database_name};'\
                        f'UID={Client_id};'\
                        f'PWD={Client_secret};'\
                        f'Authentication=ActiveDirectoryServicePrincipal;'\
                        f'Trusted_Connection=no;'\
                        f'Connection Timeout=60;'
                )
                        # f'MARS_Connection=yes;'\
                        # f'TLS_VERSION=TLSv1.2;'
                        # )
                conn = pyodbc.connect(connection_string)
                if conn:
                    st.success("Successfully connected to Azure SQL Database.")

                    # Get list of schemas
                    schemas_query = "SELECT schema_name FROM information_schema.schemata"
                    schemas = [row[0] for row in conn.execute(schemas_query)]

                    # Select schema
                    selected_schema = st.selectbox("Select Schema", schemas,on_change=sql_table_select,args=(source,))

                    # Get list of tables in the selected schema
                    tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{selected_schema}'"
                    tables = [row[0] for row in conn.execute(tables_query)]

                    # Select table
                    selected_table = st.selectbox("Select Table", tables,on_change=sql_table_select,args=(source,))

                    # Load table data into DataFrame
                    query = f"SELECT * FROM [{selected_schema}].[{selected_table}]"

                    try:
                        data = pd.read_sql_query(query, conn)
                        return data,selected_table
                    except Exception as e:
                        st.write("Current Table have no access, Change your table")
                        return None,None
                else:return None,None
           except pyodbc.Error as e:
                print("PyODBC Error:",e)
                st.write("PyODBC Error, Check the credentials and access",e)
                return None,None
           except Exception as e:
                print("An unexpected error occurred:", e) 
                st.write("An unexpected error occurred:",e) 
                return None,None
     else:return None,None

def databr(key,source=None):
    """
    Connect to Azure Databricks using SPN credentials and load a selected file
    (CSV or Excel) from DBFS into a pandas DataFrame.

    Parameters:
    key (str): Unique identifier for Streamlit widgets and session state.
    source (str, optional): Source identifier for session state updates.

    Returns:
    tuple: DataFrame and file name, or (None, None) if loading fails.
    """
    if key+"Sub_databricks" not in st.session_state:
        st.session_state[key+'Sub_databricks'] = False
    # Set up your Databricks workspace details
    with st.form(key+"my_databricks_form"):
        #storing all credentials
        st.write(f"**Please add this SPN ID to your Azure Resource:- {spn_id}**")
        st.write(f"**Please add this IP range and IP address in your ADLS:- {ip_range}**")
        st.header("Specify the Credentials:")
        st.write("**URL should look like this:-https://<databricks resource>.azuredatabricks.net**")
        db_url = st.text_input("Enter your Databricks URL", key=key+'DatabricksName')
        resource_id = st.text_input("Enter Workspace Resource ID",key=key+'ResourceName')
        st.write("**File Path should look like this:-/FileStore/file**")
        directory_path = st.text_input("Enter Directory Path",key=key+'DirectoryName')
        # authentication = 'ActiveDirectoryInteractive'
        sub = st.form_submit_button("Connect")
        # sub = st.form_submit_button("Connect", key= key+'SDBsubmit')
        #checking if all the fields are filled or not
        if all([db_url, resource_id,sub,directory_path]):
            st.session_state[key+'Sub_databricks'] = True
        else:pass
    # workspace_url = "https://<YOUR_WORKSPACE_URL>"
    # resource_id = "<RESOURCE_ID_OF_YOUR_WORKSPACE>"
    Client_id = client_id
    Client_secret = client_secret
    Tenant_id = tenant_id
    if key+"Sub_databricks" in st.session_state and st.session_state[key+'Sub_databricks'] == True:
        token_url = f"https://login.microsoftonline.com/{Tenant_id}/oauth2/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': Client_id,
            'client_secret': Client_secret,
            'resource':  resource_id#'2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'  # Databricks resource ID
        }
        access_token = requests.post(token_url, headers=headers, data=data).json().get('access_token')
        # filestore_path = "/FileStore/tables/"
        base_url = f"{db_url}/api/2.0/dbfs/list?path={directory_path}"
        headers = {'Authorization': f'Bearer {access_token}'}
        files_info = requests.get(base_url, headers=headers).json()
        if files_info:
            files = [file['path'] for file in files_info['files']]
            selected_file_path = st.selectbox("Select a file:", files,key=key+'dtbrfile',on_change=sql_table_select,args=(source,))
            base_url_2 = f"{db_url}/api/2.0/dbfs/read?path={selected_file_path}"
            st.subheader("Selected file:")
            if str(selected_file_path).split('.')[1]=='csv':
                file_location = requests.get(base_url_2, headers=headers).json()['data']#['files'][0]['path']
                decoded_data = base64.b64decode(file_location).decode('utf-8')
                csv_file = io.StringIO(decoded_data)
                df = pd.read_csv(csv_file)
                return df,selected_file_path.split("/").pop()
            elif str(selected_file_path).split('.')[1]=='xlsx':
                file_location = requests.get(base_url_2, headers=headers).json()['data']#['files'][0]['path']
                decoded_data = base64.b64decode(file_location)
                try:
                    xl_file = io.BytesIO(decoded_data)
                    wb = pd.ExcelFile(xl_file)
                    sheet = st.selectbox('Pick the Sheet Name', wb.sheet_names, key=key+'Sheet',on_change=sql_table_select,args=(source,))
                    df=pd.read_excel(xl_file,sheet_name=sheet)
                    return df,selected_file_path.split("/").pop()
                except Exception as e:
                    print("Error:", e)
    else:return None, None

def stsq(key,source=None):
    """
    Connect to a standard SQL Server database using ODBC and load a selected table
    into a pandas DataFrame.

    Parameters:
    key (str): Unique identifier for Streamlit widgets and session state.
    source (str, optional): Source identifier for session state updates.

    Returns:
    tuple: DataFrame and table name, or (None, None) if loading fails.
    """
    if "Sub_sql_s" not in st.session_state:
        st.session_state['Sub_sql_s'] = False
    # Connect to SQL Server
    with st.form(key+"my_st_sql_form"):
        hostname = st.text_input("Enter your hostname", key=key+'hostname') # Replace with your SQL Server hostname
        database = st.text_input("Enter your database", key=key+'database')
        username = st.text_input("Enter your username", key=key+'username')
        password = st.text_input("Enter your password", type="password",key=key+'password')
        sub = st.form_submit_button("Connect")
        # sub = st.form_submit_button("Connect", key= key+'SDBsubmit')
        #checking if all the fields are filled or not
        if all([hostname, database,sub,username,password]):
            st.session_state['Sub_sql_s'] = True
        else:st.write('Please Enter the Credentials')
    if "Sub_sql_s" in st.session_state and st.session_state['Sub_sql_s'] == True:
        # Construct connection string
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + hostname + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password

        # Establish connection
        sql_server_connection = pyodbc.connect(connection_string)

        try:
            # Execute SQL queries
            cursor = sql_server_connection.cursor()
            
            # Get list of schemas
            schemas_query = "SELECT schema_name FROM information_schema.schemata"
            schemas = [row[0] for row in cursor.execute(schemas_query)]

            # Select schema
            selected_schema = st.selectbox("Select Schema", schemas,key=key+'schemaselect')

            # Get list of tables in the selected schema
            tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{selected_schema}'"
            tables = [row[0] for row in cursor.execute(tables_query)]

            # Select table
            selected_table = st.selectbox("Select Table", tables,key=key+'selecttable')

            # Load table data into DataFrame
            query = f"SELECT * FROM [{selected_schema}].[{selected_table}]"
            data = pd.read_sql_query(query, cursor)
            return data,selected_table
        finally:
            # Close cursor and connection
            if 'cursor' in locals():
                cursor.close()
            if 'sql_server_connection' in locals():
                sql_server_connection.close()
    else:return None,None

def apicall(key):
    """
    Connect to a REST API, fetch JSON data, and convert it into a pandas DataFrame.
    Supports optional Basic Authentication and nested JSON normalization.

    Parameters:
    key (str): Unique identifier for Streamlit widgets and session state.

    Returns:
    tuple: DataFrame and source label ('API'), or (None, None) if loading fails.
    """
    st.title("Connect to API and Read Data into DataFrame")
 
    # API URL input
    api_url = st.text_input("Enter API URL",on_change=set_text_input, key=key+'apiurl')
 
    # Button to fetch data
    def get_keys(dl,keys_list):
       """
       Recursively collect keys from nested JSON structure.
       """
       print()
       if isinstance(dl, dict):
        keys_list += dl.keys()
        map(lambda x: get_keys(x, keys_list), dl.values())
       elif isinstance(dl, list):
            map(lambda x: get_keys(x, keys_list), dl)
     
    def has_child_table(data,key):
        """
        Check if a key in a dictionary contains a nested structure.
        """
        if key in data:
            value=data[key]
            if isinstance(value,dict) or isinstance(value,list):
                return True
        return False

    authentication = st.selectbox('Select Authentication type:', ['No Authentication', 'Basic Auth'])

    if authentication == 'Basic Auth':
        apiUsername = st.text_input('Username', key=key+'apiUsername')
        apiPassword = st.text_input('Password', type='password', key=key+'apiPassword')
       
    if st.button("Verify API",on_click=set_verify_api) or 'verify_api' in st.session_state:
        if api_url:
            try:
                # Send GET request to API
                response = requests.get(api_url.format(endpoint='stat_params_players'), params={'login': apiUsername, 'pass': apiPassword}) if authentication == 'Basic Auth' else requests.get(api_url, verify = False)
                response.raise_for_status()  # Raise error if response status is not 200 OK
                if response.json():
                    data=response.json()
                    keys=[]
                    inner_keys=[]
                    ann=[]
                    child=''
                    get_keys(data,keys)
                    # all_keys=get_keys(data)
                    key_list=keys
                    
                    key_list_new=[]
                    try:
                        for i in key_list:
                            try:
                                df=pd.json_normalize(data[i])
                                key_list_new.append(i)
                               
                            except:
                                pass
                                   
                    except:
                        pass
                    if key_list_new:            
                        selected_key=st.selectbox('Select a key from the JSON file to convert it into a table:',key_list_new)
                        if st.button("Convert into table according to the key selected and display inner keys",on_click=set_normalize_json) or 'normalize_json' in st.session_state:
                            for i in data[selected_key]:
                                get_keys(i,inner_keys)
                                get_keys(i[inner_keys[0]],ann)
                                if ann:
                                    for j in ann:
                                        if has_child_table(i[inner_keys[0]],j):
                                            child=j
                                            break
                                    normalized_data=pd.json_normalize(data[selected_key])
                                    df=pd.DataFrame(normalized_data)
                                    columns_to_delete=[col for col in df.columns if child in col]
                                    df.drop(columns=columns_to_delete,inplace=True)
                                    # st.write(df)
                                    return df,'API'
                                
                                else:
                                    normalized_data=pd.json_normalize(data[selected_key])
                                    df=pd.DataFrame(normalized_data)
                                    # st.write(df)
                                    return df,'API'
                                # break    
                        else: return None,None  
                    else:
                        normalized_data=pd.json_normalize(data)
                        df=pd.DataFrame(normalized_data)
                        return df,'API'
               
            except requests.RequestException as e:
                st.error(f"Error fetching data from API: {e}")
        else:
            st.warning("Please enter the API URL.")
    else:return None,None

def oracle_db(key):
    """
    Connect to an Oracle SQL database using user-provided credentials and load a selected table
    into a pandas DataFrame.

    Parameters:
    key (str): Unique identifier for Streamlit widgets and session state.

    Returns:
    tuple: DataFrame and table name, or (None, None) if loading fails.
    """
    if key+"Sub_orsql" not in st.session_state:
        st.session_state[key+'Sub_orsql'] = False

    with st.form(key+"my_orsql_form"):
    #storing all credentials
        st.subheader("Specify the Credentials:")
        host = st.text_input('Host Name',placeholder='Enter the Host IP Address', key=key+'ServerName') 
        service_name = st.text_input('Service Name',placeholder='Enter the Service Name', key=key+'DBName')
        user = st.text_input('Username',placeholder='Enter User Name',key=key+'UserName')
        password = st.text_input('Password',placeholder='Enter your Password', type="password",key=key+'Pwd')
        port = st.text_input('Port Number',placeholder='Enter Server Port Number', value="1521",key=key+'PortNumber')
        sub = st.form_submit_button("Submit")

    #checking if all the fields are filled or not
    if all([host, service_name, password, sub,user,port]):
        st.session_state[key+'Sub_orsql'] = True
        # server_sql=f'tcp:{server_sql},{port}'
    else:pass
    if key+"Sub_orsql" in st.session_state and st.session_state[key+'Sub_orsql'] == True:
        try:
            # Construct DSN
            dsn = f"{host}:{port}/{service_name}"
            # Establish connection

            connection = oracledb.connect(user=user, password=password, dsn=dsn)
            cursor = connection.cursor()
            st.success("Successfully connected to Oracle SQL Database.")

            # Get list of schemas
            schemas_query = "SELECT * FROM all_users"
            cursor.execute(schemas_query)
            schemas = [row[0] for row in cursor.fetchall()]

            # Select schema
            selected_schema = st.selectbox("Select Schema", schemas)

            # Get list of tables in the selected schema
            tables_query = f"SELECT table_name FROM all_tables WHERE owner = '{selected_schema}'"
            cursor.execute(tables_query)
            tables = [row[0] for row in cursor.fetchall()]

            # Select table
            selected_table = st.selectbox("Select Table", tables)

            # Load table data into DataFrame
            query = f"SELECT * FROM {selected_schema}.{selected_table}"
            # Example query
            cursor.execute(query)
            rows = cursor.fetchall()
            # Get column names

            col_names = [col[0] for col in cursor.description]

            # Create a DataFrame
            df = pd.DataFrame(rows, columns=col_names)

            # Display data in Streamlit
            return df,selected_table

        except Exception as e:

            st.error(f"Error: {e}")
            return None,None
    else:return None,None
