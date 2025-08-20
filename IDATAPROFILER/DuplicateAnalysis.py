import streamlit as st
import pandas as pd
from utility import write_to_sql,convert_to_int_if_whole
import numpy as np
from SourceData import *

def set_duplicate():
    """
    Initialize the 'duplicate_analysis' key in Streamlit session state if it doesn't exist.
    This is used to track whether duplicate analysis has been set.
    """
    if 'duplicate_analysis' not in st.session_state:
        st.session_state.duplicate_analysis = 1

def connect_to_source_data_duplicate():
    """
    Display a Streamlit UI to select and connect to a source data option.
    Based on the user's selection, it loads the corresponding dataset and file name.

    Returns:
        tuple: A tuple containing the loaded DataFrame and the file name.
    """
    st.subheader('Connect to Source Data')

    # Radio button for selecting the data source
    ques = st.radio(
        "",
        ('Flat File','ADLS Blob','S3 Bucket','Azure SQL DB','Databricks DBFS'),
        on_change=reset_dataframe
    )  #tsv,psv,parquet

    # Load data based on the selected source
    if ques == 'Flat File':
        df,file_name = flat('RuleProfilingFlat')

    if ques == 'ADLS Blob':
        print("Entry")
        df,file_name=adl('RuleProfilingadls')
                
    if ques == 'S3 Bucket':
        print("S3 bucket")
        df,file_name = s3_buc('RuleProfilings3')
                
    if ques == 'Azure SQL DB':
        print("Sql DB")
        df,file_name = sdb('RuleProfilingsql')
     
    if ques == 'Databricks DBFS':
          df,file_name = databr('RuleProfilingdatabricks')

    return df, file_name

@st.cache_data
def duplicate_analysis_data(d_df,duplicate_columns):
        """
        Analyze potential duplicate records in a DataFrame based on selected columns.

        Args:
                d_df (pd.DataFrame): The input DataFrame to analyze.
                duplicate_columns (list): List of column names to check for duplicates.

        Returns:
                tuple: Contains the following:
                        - duplicate_count_high (int): Count of high-confidence duplicates.
                        - duplicate_count_low (int): Count of low-confidence duplicates.
                        - duplicate_columns (list): Columns used for duplicate detection.
                        - d_df_high (pd.DataFrame): DataFrame of high-confidence duplicates.
                        - d_df_low (pd.DataFrame): DataFrame of low-confidence duplicates.
                        - col_null_count (int): Count of records with all nulls in duplicate columns.
        """
        d_df_high = pd.DataFrame()
        d_df_low = pd.DataFrame()

        # Use all columns if none are specified
        if len(duplicate_columns)==0:
                duplicate_columns = d_df.columns

        # Identify duplicate records
        duplicate_records = d_df.duplicated(subset=duplicate_columns,keep=False)
        
        # Count high-confidence duplicates (non-null in key columns)
        duplicate_count_high = d_df[duplicate_records].dropna(subset=duplicate_columns).shape[0]
        
        # Count low-confidence duplicates (some nulls in key columns)
        duplicate_count_low = d_df[duplicate_records].dropna(subset=duplicate_columns,how='all')[d_df[duplicate_columns].isnull().any(axis=1)].shape[0]

        # Count records with all nulls in duplicate columns
        col_null_count = d_df[duplicate_records][d_df[duplicate_columns].isnull().all(axis=1)].shape[0]

        # Display summary statistics
        st.markdown(
        f"""<b>High-Confidence Potential Duplicate Count</b> 
        <span style='font-size: 0.80em; font-style: italic;'>(None of the specified match columns contain null values)</span>: 
        <b>{duplicate_count_high} ({round(duplicate_count_high * 100 / d_df.shape[0], 2)}%)</b>""",
        unsafe_allow_html=True
        )

        st.markdown(
        f"""<b>Low-Confidence Potential Duplicate Count</b> 
        <span style='font-size: 0.80em; font-style: italic;'>(At least one of the specified match columns contains null values)</span>: 
        <b>{duplicate_count_low} ({round(duplicate_count_low * 100 / d_df.shape[0], 2)}%)</b>""",
        unsafe_allow_html=True
        )
        
        No_unique_records = d_df.shape[0]  - duplicate_count_high - duplicate_count_low  

        st.write(f"**No of unique records: {No_unique_records} ({round(No_unique_records*100/d_df.shape[0],2)}%)**")
        
        # Prepare high-confidence dupicate DataFrame
        if duplicate_count_high!=0:
    
            if len(list(duplicate_columns))!=0:
                # st.subheader("High Confidence Potential Duplicate Records")
                d_df_high = d_df[duplicate_records].sort_values(by=list(duplicate_columns)).dropna(subset=duplicate_columns)
                d_df_high["Cluster_Id"]=d_df_high.groupby(list(duplicate_columns),dropna=False).ngroup() +1
                d_df_high['Confidence(%)'] = 100
                # st.dataframe(d_df_high,hide_index=True)
        
        # Prepare low-confidence duplicate DataFrame
        if duplicate_count_low!=0:
        
            if len(list(duplicate_columns))!=0:
                # st.subheader("Low Confidence Potential Duplicate Records")
                d_df_low = d_df[duplicate_records].sort_values(by=list(duplicate_columns)).dropna(subset=duplicate_columns,how='all')[d_df[duplicate_columns].isnull().any(axis=1)]
                d_df_low["Cluster_Id"]=d_df_low.groupby(list(duplicate_columns),dropna=False).ngroup() +1
                d_df_low['Confidence(%)'] = round(d_df_low[duplicate_columns].notnull().sum(axis=1) / len(duplicate_columns) * 100,2)
                # st.dataframe(d_df_low,hide_index=True)
        
        return (
                duplicate_count_high,
                duplicate_count_low,
                duplicate_columns,
                d_df_high,
                d_df_low,
                col_null_count
        )

def new_duplicate_analysis_src():
        """
        Initialize the 'new_dup_src' key in Streamlit session state if it doesn't exist.
        Used to track the state of a new duplicate analysis source.
        """
        if 'new_dup_src' not in st.session_state:
               st.session_state.new_dup_src = 1
      
def DuplicateAnalysis():
    """
    Perform duplicate analysis on a dataset using exact match logic.
    Allows users to select a new data source, choose columns for matching,
    and view high/low confidence duplicate records along with summary statistics.
    """
    try:
        st.subheader("Duplicate Analysis")

        # Load filtered DataFrame from session state
        if 'filtered_df' in st.session_state:
                d_df = st.session_state.filtered_df[:]
                file_name = st.session_state.file_name

                # Option to load a new data source
                if st.toggle("New data source",on_change=new_duplicate_analysis_src):
                      d_df,file_name =  connect_to_source_data_duplicate()
                       
        # Proceed if data is available
        if d_df is not None:
                st.subheader(f"**File Name: {file_name}**")
                st.write("**_Sample view of dataset_**")
                st.dataframe(d_df.head(5))

                # Toggle for fuzzy match (not implemented here)
                if not st.toggle("**Fuzzy Match Analysis**",on_change=new_duplicate_analysis_src):
                        st.subheader("Exact Match Analysis")

                        # Strip whitespace from string columns
                        for column in d_df.columns:
                                if d_df[column].dtype in ['object']:
                                        d_df[column] = list(map(lambda x:x if x is None else x.strip() if x.__class__.__name__=='str' else x,d_df[column].values))

                        # Column selection for duplicate detection
                        columns = list(d_df.columns.values)
                        duplicate_columns = st.multiselect("",options=columns,on_change=set_duplicate,placeholder='Select one or more columns to identify duplicate records. (Multiple columns are recommended)')

                        # Run duplicate analysis
                        duplicate_count_high,duplicate_count_low,duplicate_columns,d_df_high,d_df_low,col_null_count= duplicate_analysis_data(d_df,duplicate_columns)

                        # Initialize database for logging duplicate stats
                        duplicate_database = pd.DataFrame(columns=['Execution_time','TableName','MatchKey','ConfidenceLevel','percentage'])

                        def push_duplicate_database(confidence,percentage):                                
                                """ Append a new duplicate analysis record to the database."""
                                nonlocal duplicate_database
                                new_record = {
                                        'Execution_time':datetime.now(),
                                        'TableName':file_name,
                                        'MatchKey':'|'.join(duplicate_columns),
                                        'ConfidenceLevel':confidence,
                                        'percentage':percentage
                                }
                                duplicate_database = duplicate_database._append(new_record,ignore_index=True)
                        
                        # Log high and low confidence duplicates
                        if duplicate_count_high!=0:
                                push_duplicate_database('high',round(duplicate_count_high*100/d_df.shape[0],2))
                        if duplicate_count_low!=0:
                                push_duplicate_database('low',round(duplicate_count_low*100/d_df.shape[0],2))

                        # Confidence level breakdown
                        confidence_100 = duplicate_count_high
                        if d_df_low.shape[0]!=0:
                                confidence_70_to_100 = d_df_low.query("`Confidence(%)` >= 70 and `Confidence(%)` < 100").shape[0]
                                confidence_50_to_70 = d_df_low.query("`Confidence(%)` >= 50 and `Confidence(%)` < 70").shape[0]
                                confidence_30_to_50 = d_df_low.query("`Confidence(%)` >= 30 and `Confidence(%)` < 50").shape[0]
                                confidence_0_to_30 = d_df_low.query("`Confidence(%)` > 0 and `Confidence(%)` < 30").shape[0]
                        else:
                               confidence_70_to_100 = 0
                               confidence_50_to_70 = 0
                               confidence_30_to_50 = 0
                               confidence_0_to_30 = 0

                        total_records = d_df.shape[0]
                        
                        # Summary table
                        confidence_dataframe = {
                               
                               "Confidence_Level" : ['100%','Between 70% and 100%','Between 50% and 70%','Between 30% and 50%','Between 0% and 30%','0%'],
                               "No of records":[confidence_100,confidence_70_to_100,confidence_50_to_70,confidence_30_to_50,confidence_0_to_30,col_null_count]
                        }
                        Summary_table = pd.DataFrame(confidence_dataframe)
                        
                        st.subheader("Duplicate Distribution Summary")
                        st.write(f"**Total Number of Records: {d_df.shape[0]}**")
                        st.dataframe(Summary_table,hide_index=True)
                        st.write("**0% Refers to there is no data in the columns those are selected for Match Analysis.**")

                        # Detailed distribution
                        if duplicate_count_low != 0 or duplicate_count_high !=0 or col_null_count!=0:
                                st.subheader("Detailed Duplicate Distribution ")

                                detailed_report = pd.DataFrame(columns=['Confidence(%)','count'])
                                if duplicate_count_low != 0:
                                   data = d_df_low['Confidence(%)'].value_counts().reset_index()
                                   detailed_report = detailed_report._append(data)

                                if duplicate_count_high !=0:
                                #    detailed_report.loc[100] = duplicate_count_high
                                     detailed_report = detailed_report._append({'Confidence(%)':100,'count':duplicate_count_high},ignore_index = True)
                                if col_null_count!=0:
                                #    detailed_report.loc[0] = col_null_count
                                   detailed_report = detailed_report._append({'Confidence(%)':0,'count':col_null_count},ignore_index = True)
                                st.dataframe(detailed_report.sort_values(by='Confidence(%)',ascending=False),hide_index=True)

                        # Display sample duplication records
                        if d_df_high.shape[0]!=0:
                                st.subheader("High Confidence Potential Duplicate Records")
                                st.dataframe(d_df_high.head(20),hide_index=True)
                        if d_df_low.shape[0]!=0:
                                st.subheader("Low Confidence Potential Duplicate Records")
                                st.dataframe(d_df_low.head(20),hide_index=True)

                        # Create three columns for layout
                        col1,col2,col3  = st.columns(3)
                        try:
                                # Export High Confidence Duplicate Records
                                if duplicate_count_high!=0:
                                        d_df_high.insert(0,'Execution_time',str(st.session_state.profile_time))
                                        if d_df_high.shape[0]!=0:
                                                col1.download_button("Export High Confidence Potential Duplicate Records",data=d_df_high.to_csv(index=False).encode('utf-8'),file_name='High_confidence_duplicate.csv',on_click=set_duplicate,use_container_width=True,type='primary')

                                # Export Low Confidence Duplicate Records
                                if duplicate_count_low!=0:
                                        d_df_low.insert(0,'Execution_time',str(st.session_state.profile_time))
                                        col1.download_button("Export Low Confidence Potential Duplicate Records",data=d_df_low.to_csv(index=False).encode('utf-8'),file_name='low_confidence_duplicate.csv',on_click=set_duplicate,use_container_width=True,type='primary')

                        except Exception as e:
                                st.error(e)

                        try:
                                # Publish statistics to database
                                if (duplicate_count_high!=0 or duplicate_count_low!=0) and col3.button(label='Publish Duplicate Statistics to Database', key='exportHighLowConfDupliToSQL', on_click=set_duplicate, use_container_width=True):
                                                                
                                        try:
                                                write_to_sql(duplicate_database,'IDATAPROFILE_DUPLICATE_STATS')
                                        except Exception as e:
                                                st.error(e)
                                        else:
                                            st.success("Successfully Published Duplicate Statistics to Database")
                        except Exception as e:
                                st.error(e)
                else:
                       st.subheader("Fuzzy Match Analysis Under Development")
    except Exception as e:
            st.error(e)
           
    # Clean up session state
    if 'duplicate_analysis' in st.session_state:
        del st.session_state.duplicate_analysis