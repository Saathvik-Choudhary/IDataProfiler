import pandas as pd
import statistics
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st
from PIL import Image
import re
from statistics import multimode
from scipy.stats import mode
import math
import sqlalchemy
import plotly.graph_objects as go
from SourceData import *
from itertools import groupby
import os
from datetime import datetime
import urllib.parse
#-----------------------------------------------

#this function is used to calculate the frequency distribution which will be published to database table.
server_name = os.getenv("server_name")
db_name = os.getenv("db_name")
db_user = os.getenv("db_user")
db_pass = os.getenv("db_pass")
@st.cache_data
def create_publish_frequency_df(frequency_df,tdf):
                """
                Create a frequency distribution DataFrame for publishing to a database.

                Parameters:
                frequency_df (DataFrame): Initial frequency DataFrame.
                tdf (DataFrame): Target DataFrame (unused here but kept for compatibility).

                Returns:
                DataFrame: Frequency distribution with top 10 values per column.
                """
                for column in st.session_state.filtered_df.columns:
                    df=pd.DataFrame(st.session_state.filtered_df[column].value_counts().head(10)).reset_index()
                    df1=pd.DataFrame()
                    # print(column)
                    
                    df1['pattern']=df[column]
                    df1['values']=df['count'].values
                    df1['column']= column
                    frequency_df=pd.concat([frequency_df,df1])

                frequency_df['Column'] = frequency_df['column']
                frequency_df['Data'] = frequency_df['pattern']
                frequency_df['Count'] = frequency_df['values']
                frequency_df.drop(['column','pattern','values'],axis=1,inplace=True)
                
                return frequency_df
 
@st.cache_data
def create_pattern_dataframe(pattern_df,tdf):
                """
                Generate pattern strings for each column in the filtered DataFrame.

                Parameters:
                pattern_df (DataFrame): DataFrame to store patterns.
                tdf (DataFrame): Target DataFrame (unused here but kept for compatibility).

                Returns:
                DataFrame: Pattern DataFrame.
                """
                # st.dataframe(st.session_state.filtered_df)
                for column in st.session_state.filtered_df.columns:
                    pattern_df[column]=generate_pattern(st.session_state.filtered_df[:],column) 

                return pattern_df

@st.cache_data
def create_pattern_dataframe_statistics(pattern_df,tdf):
                """
                Create pattern frequency statistics for each column.

                Parameters:
                pattern_df (DataFrame): Pattern DataFrame.
                tdf (DataFrame): Target DataFrame (unused here but kept for compatibility).

                Returns:
                DataFrame: Pattern frequency statistics.
                """
                p_df = pd.DataFrame()
                for column in pattern_df.columns:
                    df=pd.DataFrame(pattern_df[column].value_counts()).reset_index()
                    df1=pd.DataFrame()
                    df1['pattern']=list(map(string_pattern,df[column].values))
                    df1['values']=df['count'].values
                    df1['column']= column
                    p_df=pd.concat([p_df,df1])
                
                p_df['Column'] = p_df['column']
                p_df['Pattern'] = p_df['pattern']
                p_df['Values'] = p_df['values']
                p_df.drop(['column','pattern','values'],axis=1,inplace=True)
                return p_df

def create_publish_pattern_dataframe(pattern_df,tdf):
                """
                Create a pattern DataFrame with top 10 patterns per column for publishing.

                Parameters:
                pattern_df (DataFrame): Pattern DataFrame.
                tdf (DataFrame): Target DataFrame (unused here but kept for compatibility).

                Returns:
                DataFrame: Top pattern distribution.
                """
                p_df = pd.DataFrame()
                for column in pattern_df.columns:
                    df=pd.DataFrame(pattern_df[column].value_counts().head(10)).reset_index()
                    df1=pd.DataFrame()
                    
                    df1['pattern']=list(map(string_pattern,df[column].values))
                    df1['values']=df['count'].values
                    df1['column']= column
                    p_df=pd.concat([p_df,df1])

                p_df['Column'] = p_df['column']
                p_df['Pattern'] = p_df['pattern']
                p_df['Values'] = p_df['values']
                p_df.drop(['column','pattern','values'],axis=1,inplace=True)
                
                return p_df

#------------------------------------------------------------------------

#this function is used to convert the float to int datatype.
def convert_to_int_if_whole(x):
    """
    Convert float to int datatype.

    Parameters:
    x (float or int): Input number.

    Returns:
    int, float, or pd.NA: Converted value.
    """
    if x is None or pd.isna(x):
        return pd.NA
    if x==int(x):
        return int(x)
    return float(x)
    
def Length_of_numeric_column(df,column,parameter):
     """
     Calculate the length of the min or max numeric value in a column.

     Parameters:
     df (DataFrame): Input DataFrame.
     column (str): Column name.
     parameter (str): 'min' or 'max'.

     Returns:
     str or int: Length of the numeric value as string.
     """
     if parameter == 'min':
        if df[column].min(skipna=True) is not None:
            
            x = df[column].min(skipna=True)
            
            if x.__class__.__name__ in ['int','float']:
                
                if not math.isnan(x):
                    return str(len(str(x)))
                else:
                    return 0
            else:
                if not math.isnan(x):
                    return str(len(str(x)))
                else:
                    return 0
        else:
            return 0
     else:
        if df[column].max(skipna=True) is not None:
            
            x = df[column].max(skipna=True)
            
            if x.__class__.__name__ in ['int','float']:
                
                if not math.isnan(x):
                    return str(len(str(x)))
                else:
                    return 0
            else:
                if not math.isnan(x):
                    return str(len(str(x)))
                else:
                    return 0
        else:
            return 0
 
def min_max_val_num(df,column):
    """
    Get min and max values of a numeric column as strings.

    Parameters:
    df (DataFrame): Input DataFrame.
    column (str): Column name.

    Returns:
    tuple: (min_value, max_value) as strings or None.
    """
    x = df[column].min(skipna=True)
    y = df[column].max(skipna=True)

    if x is not None:
        if x.__class__.__name__ in ['int','float']:
                if not math.isnan(x):
                    x = str(x)
                else:
                    x = None
        else:

                if not pd.isna(x):
                    x =  str(x)
                else:
                    x = None  
    if y is not None:
        if y.__class__.__name__ in ['int','float']:
                if not math.isnan(x):
                    y = str(y)
                else:
                    y = None
        else:

                if not pd.isna(y):
                    y =  str(y)
                else:
                    y = None  
    return x,y

    # if parameter == 'min':
    #     if df[column].min(skipna=True) is not None:
            
    #         x = df[column].min(skipna=True)
            
    #         if x.__class__.__name__ in ['int','float']:
    #             if not math.isnan(x):
    #                 return str(x)
    #             else:
    #                 return None
    #         else:

    #             if not pd.isna(x):
    #                 return str(x)
    #             else:
    #                 return None      
    #     else:

    #         return None
            
    # else:
    #     if df[column].max(skipna=True) is not None:
            
    #         x = df[column].max(skipna=True)
            
    #         if x.__class__.__name__ in ['int','float']:
    #             if not math.isnan(x):
    #                 return str(x)
    #             else:
    #                 return None
    #         else:
    #             if not pd.isna(x):
    #                 return str(x)
    #             else:
    #                 return None  
    #     else:
    #         return None

#function returns mean, median and standard deviation for the numeric datatype column.
def statistics_for_numeric_column(df,column):
    """
    Calculate and return the mean, median, and standard deviation of a numeric column in a DataFrame.
    The values are returned as strings if valid, otherwise None.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    column (str): Column name to compute statistics for.

    Returns:
    tuple: (mean_value, median_value, std_value) as strings or None.
    """
    mean_value = df[column].mean(skipna=True)
    median_value = df[column].median(skipna=True)
    std_value = df[column].std(skipna=True)
    
    if mean_value is not None:
        if mean_value.__class__.__name__ in ['int','float']:
            if not math.isnan(mean_value):
                mean_value = str(mean_value)
            else:
                mean_value =  None
        else:
            if not pd.isna(mean_value):
                mean_value =  str(mean_value)
            else:
                mean_value =  None

    if median_value is not None:
        if median_value.__class__.__name__ in ['int','float']:
            if not math.isnan(median_value):
                median_value = str(median_value)
            else:
                median_value =  None
        else:
            if not pd.isna(median_value):
                median_value =  str(median_value)
            else:
                median_value =  None
    
    if std_value is not None:
        if std_value.__class__.__name__ in ['int','float']:
            if not math.isnan(std_value):
                std_value = str(std_value)
            else:
                std_value =  None
        else:
            if not pd.isna(std_value):
                std_value =  str(std_value)
            else:
                std_value =  None
    return mean_value,median_value,std_value 
         
    # if parameter == 'mean':

    #     if df[column].mean(skipna=True) is not None:

    #         x = df[column].mean(skipna=True)
                
    #         if x.__class__.__name__ in ['int','float']:
    #             if not math.isnan(x):
    #                 return str(x)
    #             else:
    #                 return None
    #         else:
    #             if not pd.isna(x):
    #                 return str(x)
    #             else:
    #                 return None  
    #     else:
    #         return None
    # if parameter == 'median':

    #     if df[column].median(skipna=True) is not None:

    #         x = df[column].median(skipna=True)
                
    #         if x.__class__.__name__ in ['int','float']:
    #             if not math.isnan(x):
    #                 return str(x)
    #             else:
    #                 return None
    #         else:
    #             if not pd.isna(x):
    #                 return str(x)
    #             else:
    #                 return None  
    #     else:
    #         return None
    # if parameter == 'std':

    #     if df[column].std(skipna=True) is not None:

    #         x = df[column].std(skipna=True)
                
    #         if x.__class__.__name__ in ('int','float'):
    #             if not math.isnan(x):
    #                 x= round(x,2)
    #                 return str(x)
    #             else:
    #                 return None
    #         else:
    #             if not pd.isna(x):
    #                 x= round(x,2)
    #                 return str(x)
    #             else:
    #                 return None

    #     else:
    #         return None


#function used to represent the pattern in summary format which will be very easy to understand.
def string_pattern(x):
        """
        Generate a string pattern summarizing consecutive character repetitions.

        Parameters:
        x (str): Input string.

        Returns:
        str: Pattern like 'a(2)b(1)c(3)'.
        """
        groups = groupby((x))
        result = [(label, sum(1 for _ in group)) for label, group in groups]
        pattern = ''.join([f'{label}({count})' for label,count in result])
        return pattern

#function returns the length distribution of dataframe
def find_length_of_all_columns(df,tdf):
    """
    Compute the length of each value in all columns of a DataFrame.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    tdf (pd.DataFrame): Template DataFrame (unused in logic).

    Returns:
    pd.DataFrame: DataFrame with length of each value.
    """
    lengthdf = pd.DataFrame()

    def generate_length(x):

        if x is None:
            return 0

        if x.__class__.__name__ in ['int','float']:

            if math.isnan(x):
                return 0
            return len(str(x))
        
        return len(str(x))
    
    for column in df.columns:
            df[column] = df[column].replace({pd.NA:None})
            if df[column].dtype != 'object' and not str(df[column].dtype).startswith('date') and not str(df[column].dtype).startswith('time') :
                lengthdf[column] = df[column].apply(lambda x: generate_length(x) if x is not None and not math.isnan(x) else  0)
            
            elif df[column].dtype =='object':
                df[column] = df[column].dropna()
                lengthdf[column] = df[column].apply(lambda x: generate_length(x) if x is not None else  0)
            
            elif str(df[column].dtype).startswith('date') or str(df[column].dtype).startswith('time'):
                df[column]=df[column].astype('str')
                df[column] = df[column].dropna()
                lengthdf[column] = df[column].apply(lambda x: generate_length(x) if x is not None  else  0)
            else:
                df[column] = df[column].dropna()
                lengthdf[column] = df[column].apply(lambda x: generate_length(x) if x is not None and not math.isnan(x)  else 0)

    return lengthdf

def scroll_up_button(page_link):
    """
    Render a scroll-up button in a Streamlit app.

    Parameters:
    page_link (str): Anchor link to scroll to.
    """
    st.markdown("<a href='#" + page_link + "' style= 'display: block; bottom: 0; right: 10; postition: sticky; width: 100px; height: 45px; background: #4E9CAF; padding: 10px; text-align: center; border-radius: 5px; color: white; font-weight: bold; text-decoration: none; line-height: 25px; float: right'>Scroll Up</a>", unsafe_allow_html=True)

#this function is used to publish profiling report to database table.
def write_to_sql_trend(df, tableName):
    """
    Write profiling report data to a SQL Server table.

    Parameters:
    df (pd.DataFrame): DataFrame containing profiling report.
    tableName (str): Target SQL table name.
    """
    file_name = df['TableName'].head(1).values[0]
    # st.write(file_name)
        
    execution_time = df['Execution_time'].head(1).values[0]
    rule_ids = list(df['RuleID'].unique())
    df1 = df.drop_duplicates(subset=['ColumnName','RuleID'])
    
    server_sql = server_name
    db = db_name
    ui = db_user
    pw = db_pass
    port = '1433'
    server_sql=f'tcp:{server_sql},{port}'
    driver= '{ODBC Driver 17 for SQL Server}'
    conn_str = f'Driver={driver};SERVER={server_sql};DATABASE={db};Uid={ui};Pwd={pw};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Auto_Commit=true;sendStringParametersAsUnicode=true;'
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}', fast_executemany=True)
    
    with engine.connect() as conn:
        
        
            for columnname, rule_id in zip(df1['ColumnName'].values,df1['RuleID'].values):
                
                insert_into_current = df.query(f" ColumnName == '{columnname}' and RuleID == '{rule_id}'").to_sql(name=tableName, schema='[dbo]',if_exists='append',con=conn,index=False)
                conn.commit()

def write_to_sql(df, tableName):
    """
    Write a DataFrame to a SQL Server table.

    Parameters:
    df (pd.DataFrame): DataFrame to write.
    tableName (str): Target SQL table name.
    """
    server_sql = server_name
    db = db_name
    ui = db_user
    pw = db_pass
    
    port = '1433'
    server_sql=f'tcp:{server_sql},{port}'
    driver= '{ODBC Driver 17 for SQL Server}'
    conn_str = f'Driver={driver};SERVER={server_sql};DATABASE={db};Uid={ui};Pwd={pw};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Auto_Commit=true;sendStringParametersAsUnicode=true;'
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}')
  
    
    with engine.connect() as conn:
        
        df.to_sql(name=tableName, schema='[dbo]',if_exists='append',con=conn,index=False)

#this function is used to publish profiling report to database table.
def write_to_sql_summary(df, tableName):
    """
    Write profiling summary data from a DataFrame to a SQL Server table.

    Parameters:
    df (pd.DataFrame): DataFrame containing profiling summary.
    tableName (str): Name of the target SQL table.
    """
    server_sql = server_name
    db = db_name
    ui = db_user
    pw = db_pass
    
    port = '1433'
    server_sql=f'tcp:{server_sql},{port}'
    driver= '{ODBC Driver 17 for SQL Server}'
    conn_str = f'Driver={driver};SERVER={server_sql};DATABASE={db};Uid={ui};Pwd={pw};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Auto_Commit=true;sendStringParametersAsUnicode=true;'
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()
  
    for index, row in df.iterrows():
        cursor.execute(f"INSERT INTO {tableName}  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )",row['Execution_time'], row['TableName'],row['ColumnName'],row['DataType'], row['Total_Count'],row['Null_Count'],row['Blank_Count'], row['Non_Null_Count'],row['Completeness%'],row['Distinct_Count'], row['Unique_Count'],row['Uniqueness%'],row['Min_Length'], row['Max_Length'],row['Min_Value'],row['Max_Value'], row['Most_frequent_value'],row['Most_Frequent_pattern'],row['Mean'], row['Median'],row['Standard_Deviation'],row['Leading_Trailing_space_count'] )

    # cursor.executemany("INSERT INTO unicodetest (col1, col2, col3) VALUES (N?, ?, ?)", data)
    connection.commit()
    cursor.close()
    connection.close()

#function used to read the table information from database 
def read_from_sql(tableName, fileName):
    """
    Read the latest profiling data for a specific table from SQL Server.

    Parameters:
    tableName (str): Name of the SQL table to read from.
    fileName (str): Table name to filter the data.

    Returns:
    pd.DataFrame or None: DataFrame with the latest profiling data or None if an error occurs.
    """
    server_sql = server_name
    db = db_name
    ui = db_user
    pw = db_pass
    port = '1433'
    server_sql=f'tcp:{server_sql},{port}'
    driver= '{ODBC Driver 17 for SQL Server}'
    conn_str = urllib.parse.quote_plus(f'Driver={driver};SERVER={server_sql};DATABASE={db};Uid={ui};Pwd={pw};'
                                       'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Auto_Commit=true;')
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={conn_str}', fast_executemany=True)
    try:
        with engine.connect() as conn:
            query = f"select * from {tableName} where TableName = '{fileName}' and Execution_time = (Select max(Execution_time) from {tableName} where TableName = '{fileName}')"
            df = pd.read_sql_query(query, con=conn)
            return df
    except Exception as e:
        st.write(f"Error while accessing the data: ")
        return None
    
#function display the dataframe and chart to show the result for each executed business rule 
def ShowDFAndChart(resultDF):
    """
    Display a sample of rule validation results and a pie chart summary in Streamlit.

    Parameters:
    resultDF (pd.DataFrame): DataFrame containing rule validation results.
    """
    cols = st.columns([1.2, 0.7])
    dftest = resultDF
    unique_statuses = dftest['RuleStatus'].unique()
    sampled_records = pd.DataFrame()
    for status in ['PASS', 'FAIL', 'NULL']:
        if status in unique_statuses:
            sampled_status = dftest[dftest['RuleStatus'] == status].sample(n=min(10, len(dftest[dftest['RuleStatus'] == status])), random_state=1)
            sampled_records = pd.concat([sampled_records, sampled_status])
    with cols[0]:
        cols[0]=st.dataframe(sampled_records, hide_index=True)

    with cols[1]:
        resultStatus = resultDF['RuleStatus'].value_counts()
        totalCount = len(resultDF['RuleStatus'])
        chartLabel = []
        chartValues = []
        chartColors = []
        chartPercent = []
        chartHover = []
        pullFail = []
        for key in resultStatus.keys():
            chartLabel.append(key)
            chartValues.append(resultStatus[key])
            if key == 'PASS':
                 chartColors.append("green")
                 pullFail.append(0)
                 chartPercent.append(str(round(resultStatus[key]*100/totalCount,2))+"%")
                 chartHover.append('label+value')
            elif key == 'FAIL':
                 chartColors.append("red")
                 pullFail.append(0.05)
                 chartPercent.append(str(round(resultStatus[key]*100/totalCount,2))+"%")
                 chartHover.append('label+value')
            elif key == 'NULL':
                 chartColors.append("yellow")
                 pullFail.append(0)
                 chartPercent.append(str(round(resultStatus[key]*100/totalCount,2))+"%")
                 chartHover.append('label+value')
            else:
                 chartColors.append("Black")
                 pullFail.append(0)
                 chartPercent.append(str(round(resultStatus[key]*100/totalCount,2))+"%")
                 chartHover.append('label+value')


        fig = go.Figure(data=[go.Pie(labels=chartLabel,
                                        values=chartValues,texttemplate=chartPercent,
                                        marker=dict(colors=chartColors, line=dict(color='white', width=1)),
                                        pull=pullFail,hoverinfo=chartHover,
                                        title='<b>Validation Results</b>',
                                        titlefont=dict(size=20), titleposition=('bottom center')
                                    )])
        fig.update_layout(legend=dict(y=0.9,x=0),
                          paper_bgcolor='whitesmoke',
                          height=400,width=380)    
        cols[1].plotly_chart(fig)

#Generate pattern 
def generate_pattern(df,column):
    """
    Generate a generalized pattern for each value in a column.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    column (str): Column name to generate patterns for.

    Returns:
    pd.Series: Series of pattern strings.
    """
    df[column] = df[column].replace({pd.NA:None})
    #function that generates pattern for data value
    def pattern_generator(x):

        if x is None:
            return None

        if x.__class__.__name__ in ['int','float']:

            if math.isnan(x):
                return None
        pattern = re.sub('[0-9]','9',str(x))
        pattern = re.sub('[A-Za-z]','X',pattern)
        pattern = re.sub('[\s]','B',pattern)
        pattern = re.sub('[^0-9A-Za-z]','S',pattern)
        
        return pattern

    if df[column].dtype != 'object' and not str(df[column].dtype).startswith('date') and not str(df[column].dtype).startswith('time') :

        return df[column].apply(lambda x: pattern_generator(x) if x is not None and not math.isnan(x) else  None)
    
    if df[column].dtype =='object':
        df[column] = df[column].dropna()
        
        return df[column].apply(lambda x: pattern_generator(x) if x is not None else  None)
    
    if str(df[column].dtype).startswith('date') or str(df[column].dtype).startswith('time'):
        df[column]=df[column].astype('str')
        df[column] = df[column].dropna()
        return df[column].apply(lambda x: pattern_generator(x) if x is not None  else  None)

    df[column] = df[column].dropna()
   
    return df[column].apply(lambda x: pattern_generator(x) if x is not None and not math.isnan(x)  else None)

#function used to calculate the number of blank counts.
def blank_values(df,column):
    """
    Count the number of blank (empty string) values in a column.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    column (str): Column name to check for blanks.

    Returns:
    int: Count of blank values.
    """
    partial_df = pd.DataFrame()
    if df[column].dtype == 'object':
        partial_df[column] = np.where(df[column].isna(),df[column],df[column].astype(str))
        partial_df[column] = partial_df[column].str.strip()
        
        partial_df[column] = partial_df[column].replace({pd.NA:None})
        return sum(list(map(lambda x: 1 if x=='' else 0,list(partial_df[column].values))))
    return 0

#function used to calculate the leading trailing space count.
def prefix_suffix_check(df,column):
    """
    Check for leading or trailing spaces in a column.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    column (str): Column name to check.

    Returns:
    tuple: (bool indicating presence, count of such values)
    """
    partial_df = pd.DataFrame()
    if df[column].dtype == 'object':
        
        total_count = sum(list(df[column].map(lambda x:  1 if str(x).startswith(' ') or str(x).endswith(' ') else 0)))
       
        return total_count !=0 , total_count
    return None,0

def calculate_length(x):
    """
    Calculate the length of a value after converting to string.

    Parameters:
    x: Any value.

    Returns:
    int or None: Length of the value or None if invalid.
    """
    if x is None:
        return 0
    elif x.__class__.__name__ in ['int','float']:
        if math.isnan(x):
            return  None
        else:
            return len(str(x))
    elif pd.isna(x):
        return None
    else:
        return len(str(x))

def min_max_length(df1,column):
    """
    Calculate minimum and maximum length of values in a column.

    Parameters:
    df1 (pd.DataFrame): Input DataFrame.
    column (str): Column name.

    Returns:
    tuple: (min_length, max_length)
    """
    length_df = pd.DataFrame()
    length_df[column]=(df1[column].map(lambda x: calculate_length(x) if x is not None else None).values)
    return (length_df[column].min(skipna=True),length_df[column].max(skipna=True))

#function used to calculate the minimum value.
def find_minimum_maximum(df,column):
    """
    Find the minimum and maximum values in a column.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    column (str): Column name.

    Returns:
    tuple: (min_value, max_value) as strings or None.
    """
    if df[column].notna().sum() == 0:
        return None,None
    df1=pd.DataFrame()
    df1[column]=df[column].dropna()
    return min([str(x) for x in df1[column].values]), max([str(x) for x in df1[column].values])

#function to calculate most frequent value
def most_frequent_value(df,column):
    """
    Find the most frequent value in a column.

    Parameters:
    df (pd.DataFrame): Input DataFrame.
    column (str): Column name.

    Returns:
    object or None: Most frequent value or None if not available.
    """
    if df[column].notna().sum() == 0:
        return None
    df1=pd.DataFrame()
    df1[column]=df[column].dropna()
    return statistics.mode(df1[column].values)

#function used to calculate profiling result
def help_me_profile(tab,pattern_df):
    """
    Generate profiling metrics and insights for each column in a DataFrame.

    Parameters:
    tab (pd.DataFrame): Input DataFrame to profile.
    pattern_df (pd.DataFrame): DataFrame containing pattern information.

    Returns:
    list: List of insights dictionaries.
    """
    df1 = tab
    l1=[]
    insights = []
    #creating the list

    for column in df1.columns:
        #creating the dictionary to store the calculation result.
        d=dict()
        insight_dict = dict()
        d['Column_Name'] = column
        d['DataType'] = str(df1[column].dtype)
        d['Total_Count'] = str(df1.shape[0])

        if df1[column].dtype in ['object']:
            minimum_length,maximum_length = min_max_length(df1,column)
            # maximum_length = max_length(df1,column)
            blank_count = blank_values(tab,column)
            d['Null_Count'] = str(df1[column].isnull().sum())
            
            d['Blank_Count'] = blank_count
            total_data_counts = df1[column].count()
            total_unique_count = df1[column].nunique()
            d['Non_Null_Count'] = str(total_data_counts - blank_count)
            d['Distinct_Count'] = str(total_unique_count)
            d['Unique_Count'] = str(df1[column].value_counts()[df1[column].value_counts() == 1].count())
            d['Min_Length'] = str(int(minimum_length)) if minimum_length is not None and not math.isnan(minimum_length) else 0
            d['Max_Length'] = str(int(maximum_length)) if maximum_length is not None and not math.isnan(maximum_length) else 0
            minimum,maximum = find_minimum_maximum(df1,column)
            d['Min_Value'] = minimum         
            d['Max_Value'] = maximum
            frequent_value = most_frequent_value(df1,column)
            d['Most_frequent_value'] = frequent_value if frequent_value is not None else None
            frequent_pattern = None if df1[column].notna().sum() == 0 else pattern_df[column].value_counts().reset_index().head(1).iloc[0,0]
            d['Most_Frequent_pattern']=string_pattern(frequent_pattern) if frequent_pattern is not None else None
            d['Mean']= None
            d['Median']=None
            d['Standard_Deviation']= None
            lead_trail_space_presence , lead_trail_space_count = prefix_suffix_check(tab,column)
            d['Leading_Trailing_space_presence'] = str(lead_trail_space_presence)
            d['Leading_Trailing_space_count'] = lead_trail_space_count - blank_count if lead_trail_space_count - blank_count > 0 else 0
            if lead_trail_space_count - blank_count > 0:
                 insight_dict['Description'] = f'Column contains {lead_trail_space_count - blank_count} leading or trailing spaces'
                 insight_dict['Dimension'] = 'Validity'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict)
                 insight_dict = dict()
            
            special_char_pattern = r'[^a-zA-Z0-9\s]'
            count_special_chars_df = df1[column].map(lambda x: isinstance(x,str) and re.search(r'[^a-zA-Z0-9\s]', x))
            
            count_special_chars = count_special_chars_df.apply(lambda x: 0 if pd.isna(x) or x is None or not x else 1).sum()
            
            if count_special_chars > 0:
                 
                 insight_dict['Description'] = f"{ count_special_chars } records contain special characters(excluding white spaces) for this column"
                 insight_dict['Dimension'] = "Special characters"
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict)
                 insight_dict = dict()
                 
            # token_df = (df1[column].str.split().str.len())
            token_df = df1[column].map(lambda x: None if x is np.nan or x is None else 1 if not isinstance(x,str) else None if x.isspace() else len(x.split()))
            
            # token_df = df1[column].map(lambda x: x if x is np.nan else 1)
            max_token = token_df.max()
            min_token = token_df.min()
            if (min_token is not np.nan or min_token is not None) and min_token > 0 and max_token <= 10:
                insight_dict['Description'] = f'Column contains minimum {min_token} and maximum {max_token} tokens'
                insight_dict['Dimension'] = 'Data Format'
                insight_dict['ColumnName'] = column
                insight_dict['TableName'] =  st.session_state.file_name
                insight_dict['Execution_date'] =  datetime.now().date()
                insights.append(insight_dict)
                insight_dict = dict()
            

        elif str(df1[column].dtype).startswith('date') or str(df1[column].dtype).startswith('time'):
            # minimum_length = min_length(df1,column)
            # maximum_length = max_length(df1,column)
            blank_count = 0
            minimum_length,maximum_length = min_max_length(df1,column)
            d['Null_Count'] = str(df1[column].isnull().sum())
            d['Blank_Count'] = blank_count
            total_data_counts = df1[column].count()
            total_unique_count = df1[column].nunique()
            d['Non_Null_Count'] = str(total_data_counts)
            d['Distinct_Count'] = str(total_unique_count)
            d['Unique_Count'] = str(df1[column].value_counts()[df1[column].value_counts() == 1].count())
            d['Min_Length'] = str(int(minimum_length))  if minimum_length is not None and not math.isnan(minimum_length) else 0
            d['Max_Length'] = str(int(maximum_length)) if maximum_length is not None and not math.isnan(maximum_length) else 0
            minimum,maximum = find_minimum_maximum(df1,column)
            d['Min_Value'] = minimum         
            d['Max_Value'] = maximum
            frequent_value = most_frequent_value(df1,column)
            d['Most_frequent_value'] = str(frequent_value) if frequent_value is not None else None
            frequent_pattern = None if df1[column].notna().sum() == 0 else pattern_df[column].value_counts().reset_index().head(1).iloc[0,0]
            d['Most_Frequent_pattern']=string_pattern(frequent_pattern) if frequent_pattern is not None else None
            d['Mean']= None
            d['Median']=None
            d['Standard_Deviation']= None
            d['Leading_Trailing_space_presence'] = None
            d['Leading_Trailing_space_count'] = 0

        else:
            minimum_length,maximum_length = min_max_length(df1,column)
            d['Null_Count'] = str(df1[column].isnull().sum())
            blank_count = 0
            d['Blank_Count'] = 0
            total_data_counts = df1[column].count()
            total_unique_count = df1[column].nunique()
            d['Non_Null_Count'] = str(total_data_counts)

            d['Distinct_Count'] = str(total_unique_count)
            # d['Non_Distinct_Count'] = str(int(total_data_counts)-total_unique_count)
            d['Unique_Count'] = str(df1[column].value_counts()[df1[column].value_counts() == 1].count())

            d['Min_Length'] =  str(int(minimum_length))  if minimum_length is not None and not math.isnan(minimum_length) else 0
            d['Max_Length'] =  str(int(maximum_length)) if maximum_length is not None and not math.isnan(maximum_length) else 0
            min_val,max_val = min_max_val_num(df1,column)
            d['Min_Value'] = min_val
            d['Max_Value'] = max_val
            # d['Most_frequent_value'] = str(statistics.mode(df1[column].values))
            frequent_value = statistics.mode([x for x in df1[column].values if x is not None and not pd.isna(x)]) if df1[column].isnull().sum()!=df1[column].shape[0] else None
            d['Most_frequent_value'] = str(statistics.mode([x for x in df1[column].values if x is not None and not pd.isna(x)])) if df1[column].isnull().sum()!=df1[column].shape[0] else None

            
            if d['Most_frequent_value'] is not None:
                
                d['Most_Frequent_pattern']=None if df1[column].notna().sum() == 0 else string_pattern(pattern_df[column].value_counts().reset_index().head(1).iloc[0,0])
            else:
                d['Most_Frequent_pattern']= None
            mean_value,median_value,std_value = statistics_for_numeric_column(df1,column)
            d['Mean'] = mean_value
            d['Median'] = median_value
            d['Standard_Deviation'] = std_value
            if mean_value is not None and std_value is not None:
               stddev_outliers_right = df1[(df1[column] > (float(mean_value) + 3*float(std_value)))].shape[0]
               
               stddev_outliers_left = df1[(df1[column] < (float(mean_value) - 3*float(std_value)))].shape[0]
               
               no_of_outliers = stddev_outliers_left + stddev_outliers_right
               if no_of_outliers> 0:
                 insight_dict['Description'] = f'Column contains {no_of_outliers} data points fall outside of 3 standard deviations'
                 insight_dict['Dimension'] = 'Outliers'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict)
                 insight_dict = dict()
                 
            d['Leading_Trailing_space_presence'] = None
            d['Leading_Trailing_space_count'] = 0
        
        #check if all patterns are not None
        if pattern_df[column].value_counts().shape[0] > 0:
            frequent_pattern_count = pattern_df[column].value_counts().reset_index().head(1).iloc[0,1]
            frequent_pattern_percentage = round(frequent_pattern_count * 100 / df1.shape[0],2)
            if frequent_pattern_percentage > 1:
                insight_dict['Description'] = f"{d['Most_Frequent_pattern']} pattern occurs {frequent_pattern_percentage}% of the time"
                insight_dict['Dimension'] = 'Data Format'
                insight_dict['ColumnName'] = column
                insight_dict['TableName'] =  st.session_state.file_name
                insight_dict['Execution_date'] =  datetime.now().date()
                insights.append(insight_dict)
                insight_dict = dict()
             
             
        # if d['Most_Frequent_pattern'] 
        
        if int(blank_count)==  df1.shape[0]:
                 insight_dict['Description'] = 'Column contains 100% blank values'
                 insight_dict['Dimension'] = 'Completeness'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict)
                 insight_dict = dict()
        elif int(blank_count)>0:
                 insight_dict['Description'] = f'Column contains {round(int(blank_count)*100/df1.shape[0],2)}% blank values'
                 insight_dict['Dimension'] = 'Completeness'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict)
                 insight_dict = dict()

        if int(d['Null_Count'])==df1.shape[0]:
                 insight_dict['Description'] = 'Column contains 100% null values'
                 insight_dict['Dimension'] = 'Completeness'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict) 
                 insight_dict = dict()

        elif  int(d['Null_Count']) / df1.shape[0] > 0.5:
                 insight_dict['Description'] = 'Column has more than 50% of null values'
                 insight_dict['Dimension'] = 'Completeness'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict) 
                 insight_dict = dict()
        
        #Done
        if int(total_unique_count)==1:
                 insight_dict['Description'] = 'Column contains only one distinct value'
                 insight_dict['Dimension'] = 'Value Distribution'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict)
                 insight_dict = dict()

        if int(d['Unique_Count'])==df1.shape[0]:
                 insight_dict['Description'] = 'Column contains 100% unique values'
                 insight_dict['Dimension'] = 'Uniqueness'
                 insight_dict['ColumnName'] = column
                 insight_dict['TableName'] =  st.session_state.file_name 
                 insight_dict['Execution_date'] =  datetime.now().date()
                 insights.append(insight_dict)
                 insight_dict = dict()
        
        if d['Most_frequent_value'] is not None:
            most_frequent_value_count = df1[df1[column]==frequent_value][column].shape[0]
            most_frequent_value_percentage = most_frequent_value_count*100/df1.shape[0]
            
            if most_frequent_value_percentage > 1:
                    insight_dict['Description'] = f"{d['Most_frequent_value']} Occurs {round(most_frequent_value_percentage,2)}% of the time"
                    insight_dict['Dimension'] = 'Value Distribution'
                    insight_dict['ColumnName'] = column
                    insight_dict['TableName'] =  st.session_state.file_name
                    insight_dict['Execution_date'] =  datetime.now().date() 
                    insights.append(insight_dict)
                    insight_dict = dict()
        
             
             
        
        l1.append(d)

    df = pd.DataFrame(l1)
    insight_df = pd.DataFrame(insights) #create the dataframe using list.
    df.set_index(keys='Column_Name',drop=True,inplace=True)
    
    return df,insight_df