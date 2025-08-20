import streamlit as st
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib_venn import venn2,venn2_circles
from matplotlib.patches import Circle
from SourceData import *
from Filter import *
import numpy as np
from utility import convert_to_int_if_whole

st.set_option('deprecation.showPyplotGlobalUse', False)

def apply_filter4():
    """Set a flag in session state to apply filter 4."""
    if 'apply_filter4' not in st.session_state:
        st.session_state.apply_filter4=1


def filter_state4():
    """Activate filter 4 and remove apply flag if present."""
    st.session_state.filter4=1
    if 'apply_filter4' in st.session_state:
        del st.session_state.apply_filter4

def increase_rule_count4():
    """Add a new rule for filter 4 and reset apply flag."""
    if 'rule_count4' in st.session_state:
        st.session_state.rule_count4+=1
    st.session_state.col_name_dict4['ColName'+str(st.session_state.rule_count4)] = None
    st.session_state.col_operator_dict4['ColOp'+str(st.session_state.rule_count4)] = None 
    st.session_state.col_value_dict4['ColValue'+str(st.session_state.rule_count4)] = None

    if 'apply_filter4' in st.session_state:
        del st.session_state.apply_filter4

def add_filter_conditions4():
    """Initialize dictionaries and counters for filter 4 if not already set."""
    if 'col_name_dict4' not in st.session_state:
        st.session_state.col_name_dict4 = {}

    if 'col_operator_dict4' not in st.session_state:
        st.session_state.col_operator_dict4 = {}

    if 'col_value_dict4' not in st.session_state:
        st.session_state.col_value_dict4 = {}

    if 'rule_count4' not in st.session_state:
        st.session_state.rule_count4 = 0
    
    if 'filter4' not in st.session_state:

        st.session_state.filter4 = 1

def apply_filter5():
    """Set a flag in session state to apply filter 5."""
    if 'apply_filter5' not in st.session_state:
        st.session_state.apply_filter5=1

def filter_state5():
    """Activate filter 5 and remove apply flag if present."""
    st.session_state.filter5=1
    if 'apply_filter5' in st.session_state:
        del st.session_state.apply_filter5

def increase_rule_count5():
    """Add a new rule for filter 5 and reset apply flag."""
    if 'rule_count5' in st.session_state:
        st.session_state.rule_count5+=1
    st.session_state.col_name_dict5['ColName'+str(st.session_state.rule_count5)] = None
    st.session_state.col_operator_dict5['ColOp'+str(st.session_state.rule_count5)] = None 
    st.session_state.col_value_dict5['ColValue'+str(st.session_state.rule_count5)] = None

    if 'apply_filter5' in st.session_state:
        del st.session_state.apply_filter5

def add_filter_conditions5():
    """Initialize dictionaries and counters for filter 5 if not already set."""
    if 'col_name_dict5' not in st.session_state:
        st.session_state.col_name_dict5 = {}

    if 'col_operator_dict5' not in st.session_state:
        st.session_state.col_operator_dict5 = {}

    if 'col_value_dict5' not in st.session_state:
        st.session_state.col_value_dict5 = {}

    if 'rule_count5' not in st.session_state:
        st.session_state.rule_count5 = 0
    
    if 'filter5' not in st.session_state:

        st.session_state.filter5 = 1

def set_table_prev():
    """Set flag to indicate previous table selection."""
    st.session_state.table_prev=1

def set_select_table():
    """Set flag to indicate current table selection."""
    st.session_state.select_table=1  

def set_select_table_dropdown():
    """Reset table selection flags."""
    if 'table_prev' in st.session_state:
        del st.session_state.table_prev   
    if 'select_table' in st.session_state:
        del st.session_state.select_table   
   
def table_list():
    """Initialize list of table names in session state."""
    st.session_state['table_list']=['Left Table','Right Table','Intersection Table']  
 
def change_table():
    if 'table_prev' in st.session_state:
        del st.session_state.table_prev
    """Set flag to indicate table change only if selection changed."""
    if 'previous_selection' in st.session_state:
        if st.session_state.previous_selection != st.session_state.selected_table:
            st.session_state.table_change = True
    else:
        st.session_state.table_change = True
    st.session_state.previous_selection = st.session_state.selected_table

def remove_table_change():
    """Remove table change flag if present."""
    if 'table_change' in st.session_state:
        del st.session_state.table_change
     
def read_file(file):
    """
    Read uploaded file into a DataFrame.
    
    Parameters:
        file (UploadedFile): Uploaded file object.
    
    Returns:
        pd.DataFrame or None: Parsed DataFrame or None if error occurs.
    """
    if file:
        try:
            if file.name.endswith('.csv'):
                df = pd.read_csv(file)
            elif file.name.endswith('.xlsx'):
                df = pd.read_excel(file)
            else:
                st.error("Unsupported file format. Please upload CSV or Excel files.")
                return None
            return df
        except Exception as e:
            st.error(f"Error reading file: {e}")
            return None
        
def rename_columns(df):
    """
    Rename all columns in the DataFrame by appending '_1'.
    
    Parameters:
        df (pd.DataFrame): Original DataFrame.
    
    Returns:
        pd.DataFrame: Renamed DataFrame.
    """
    renamed_df = df.copy()
    for col in renamed_df.columns:
        renamed_df.rename(columns={col: col + "_1"}, inplace=True)
    return renamed_df

def venn_diagram(left_only, right_only, both,leftt,rightt):
    """
    Display a Venn diagram using matplotlib and Streamlit.
    
    Parameters:
        left_only (int): Count of elements only in the left set.
        right_only (int): Count of elements only in the right set.
        both (int): Count of elements in both sets.
        left_label (str): Label for the left set.
        right_label (str): Label for the right set.
    """
    fig, ax = plt.subplots(figsize=(8, 6))

    # Draw circles for left-only and right-only elements
    ax.add_patch(Circle((-0.25, 0), 0.5, color='blue', alpha=0.2))
    ax.add_patch(Circle((0.25, 0), 0.5, color='red', alpha=0.2))

    # Display overlap region
    ax.text(-0.5,0,f"{leftt}: {left_only}", ha='center', va='center', fontsize=8)
    ax.text(0.5,0,f"{rightt}: {right_only}", ha='center', va='center',fontsize=8)
    ax.text(0, 0, f"Both: {both}", ha='center', va='center',fontsize=8)

    # Set axis limits and remove ticks
    ax.set_xlim(-1, 1)
    ax.set_ylim(-1, 1)
    ax.axis('off')

    st.pyplot(fig)  # Display the Venn diagram

def connect_source_data_join(source):
    """
    Prompt the user to select a data source and load the corresponding dataset.

    Parameters:
        source (str): Identifier or configuration for the data source.

    Returns:
        tuple: A tuple containing the loaded DataFrame and the file name.
    """
    ques = st.radio("Upload First Data Source",('Flat File','ADLS Blob','S3 Bucket','Azure SQL DB','Databricks DBFS'))  #tsv,psv,parquet

    if ques == 'Flat File':
       df,file_name = flat('a',source)
       
    if ques == 'ADLS Blob':
        df,file_name=adl('b',source)
                 
    if ques == 'S3 Bucket':
        df,file_name = s3_buc('c',source)
        
    if ques == 'Azure SQL DB':
        df,file_name = sdb('d',source)

    if ques == 'Databricks DBFS': 
        df,file_name = databr('databricks1',source)
  
    if ques == 'Standard SQL Server': 
        df,file_name = stsq('stsql1',source)
        
    if ques == 'API':
        df,file_name = apicall('apicall1',source)

    return df,file_name

def clear_particular_filter_join1(key1,key2,key3):
    """
    Remove a specific filter rule from filter 4 dictionaries in session state.

    Parameters:
        key1 (str): Key for column name.
        key2 (str): Key for column operator.
        key3 (str): Key for column value.
    """
    if key1 in st.session_state.col_name_dict4:
        del st.session_state.col_name_dict4[key1] 
        del st.session_state.col_operator_dict4[key2] 
        del st.session_state.col_value_dict4[key3]
    
    if len(st.session_state.col_name_dict4) == 0:
        st.session_state.rule_count4 = 0

def clear_particular_filter_join2(key1,key2,key3):
    """
    Remove a specific filter rule from filter 5 dictionaries in session state.

    Parameters:
        key1 (str): Key for column name.
        key2 (str): Key for column operator.
        key3 (str): Key for column value.
    """
    if key1 in st.session_state.col_name_dict5:
        del st.session_state.col_name_dict5[key1] 
        del st.session_state.col_operator_dict5[key2] 
        del st.session_state.col_value_dict5[key3]
    
    if len(st.session_state.col_name_dict5) == 0:
        st.session_state.rule_count5 = 0

def JoinProfiling():
    """
    Streamlit interface for uploading and profiling two datasets for join operations.
    Allows filtering of the first dataset and displays sample views of both datasets.
    """
    st.title("Join Profiling")

    # Load first source data
    if 'original_dataframe' not in st.session_state or 'src_select_widget_change_source_JOIN' in st.session_state:
        df,file_name = connect_source_data_join('join_source_data')
        if df is not None: 
                    numeric_columns = df.select_dtypes(include=np.number).columns.to_list()                    
                    for column1 in numeric_columns:
                        df[column1] = df[column1].apply(convert_to_int_if_whole)
                        try:
                            df[column1] = df[column1].astype('Int64')
                        except:
                            df[column1] = df[column1].astype('Float64')
                    
                    st.session_state.original_dataframe = df
                    st.session_state.file_name = file_name
    else:
        df =  st.session_state.original_dataframe
        file_name = st.session_state.file_name
        if st.toggle("New Source",key='newjoinsource'):
            df = None
            df,file_name = connect_source_data_join('join_source_data')
            
            if df is not None: 
                numeric_columns = df.select_dtypes(include=np.number).columns.to_list()                    
                for column1 in numeric_columns:
                    df[column1] = df[column1].apply(convert_to_int_if_whole)
                    try:
                        df[column1] = df[column1].astype('Int64')
                    except:
                        df[column1] = df[column1].astype('Float64')
                
                st.session_state.original_dataframe = df
                st.session_state.file_name = file_name
                df =  st.session_state.original_dataframe
                file_name = st.session_state.file_name

    # Display and filter first dataset
    if df is not None:
        numeric_columns = df.select_dtypes(include=np.number).columns.to_list()                    
        for column1 in numeric_columns:
            df[column1] = df[column1].apply(convert_to_int_if_whole)
            try:
                df[column1] = df[column1].astype('Int64')
            except:
                df[column1] = df[column1].astype('Float64')
        st.subheader(f"**File Name: {file_name}**")
        st.write("**_Sample view of dataset_**")
        st.dataframe(df.head(5), hide_index=True)
        if st.button("Include/Exclude Source Data",on_click=add_filter_conditions4) or 'filter4' in st.session_state:
                                
                                if 'rule_count4' in st.session_state and st.session_state.rule_count4>0:
                                    for key1,key2,key3 in zip(st.session_state.col_name_dict4.keys(),st.session_state.col_operator_dict4.keys(),st.session_state.col_value_dict4.keys()):
                                        col1,col2,col3,col4=st.columns([1,1,1,0.5])
                                        col1.write("**Select Column**")
                                        column1=col1.selectbox("",options=df.columns,on_change = filter_state4,key = 'ColName4'+str(key1))
                                        col2.write("**Select the operator**")
                                        operator = col2.selectbox("",options=['==','!=','<','<=','>','>=','Like','Not Like','In','Not In','Is None','Is Not None'],on_change = filter_state4,key = 'ColOp4'+str(key2),index = None)
                                        col3.write("**Enter the compare value**")
                                        compare_value = col3.text_input("",on_change = filter_state4,key= 'ColValue4'+str(key3))
                                        col4.write("")
                                        col4.write("")
                                        col4.write("")
                                        col4.write("")
                                        col4.button("❌",key= 'Clear_filter'+str(key1),on_click=clear_particular_filter_join1,args=(key1,key2,key3))
                                        st.session_state.col_name_dict4[key1] = st.session_state['ColName4'+str(key1)]
                                        st.session_state.col_operator_dict4[key2] = st.session_state['ColOp4'+str(key2)]
                                        st.session_state.col_value_dict4[key3] = st.session_state['ColValue4'+str(key3)]
                                
                                col1,col2,col3 = st.columns([1.3,1,4])
                                if 'rule_count4' in st.session_state and col1.button("Add Filter condition",on_click=increase_rule_count4):

                                    pass
                                
                                if 'rule_count4' in st.session_state and st.session_state.rule_count4 > 0:
                                    
                                    if col2.button("Clear All Filters",on_click = clear_filters4):
                                        pass
                                if 'rule_count4' in st.session_state and st.session_state.rule_count4>0:
                                        if len(st.session_state.col_name_dict4) == len(st.session_state.col_operator_dict4) == len(st.session_state.col_value_dict4):  
                                            if st.button("Apply Filter",on_click=apply_filter4) or 'apply_filter4' in st.session_state:
                                                try:    
                                                    df = filter_dataframe(df,st.session_state.col_name_dict4.values(),st.session_state.col_operator_dict4.values(),st.session_state.col_value_dict4.values())    
                                                except:
                                                    st.error("Please select correct filter condition")
                                                else:
                                                    st.write("Number of records:",df.shape[0])
    
    # Store filtered data
    if df is not None:
            st.session_state.common_data = df[:]
            st.session_state.file_name = file_name
    ques1 = st.radio("Upload Second Source Data",('Flat File','ADLS Blob','S3 Bucket','Azure SQL DB','Databricks DBFS'))  #tsv,psv,parquet

    # Load second source data
    if ques1 == 'Flat File':
            df2,file_name2 = flat('e','join_reference_data')

    if ques1 == 'ADLS Blob':
            df2,file_name2=adl('f','join_reference_data')
                    
    if ques1 == 'S3 Bucket':
            df2,file_name2 = s3_buc('g','join_reference_data')
                    
    if ques1 == 'Azure SQL DB':
            df2,file_name2 = sdb('h','join_reference_data')
    if ques1 == 'Databricks DBFS':
            df2,file_name2 = databr('databricks2','join_reference_data')

    if ques1 == 'Standard SQL Server':
            df2,file_name2 = stsq('stsql2','join_reference_data')
            
    if ques1 == 'API':
            df2,file_name2 = apicall('apicall2','join_reference_data')
            
    # Display second dataset
    if df2 is not None:
            numeric_columns = df2.select_dtypes(include=np.number).columns.to_list()                    
            for column1 in numeric_columns:
                    df2[column1] = df2[column1].apply(convert_to_int_if_whole)
                    try:
                        df2[column1] = df2[column1].astype('Int64')
                    except:
                        df2[column1] = df2[column1].astype('Float64')
            st.write("**_Sample view of dataset_**")
            st.dataframe(df2.head(5), hide_index=True)     

    # Perform join and display Venn diagram
    if df2 is not None:
            if st.button("Include/Exclude Source Data",on_click=add_filter_conditions5,key='join2include') or 'filter5' in st.session_state:
                                
                                if 'rule_count5' in st.session_state and st.session_state.rule_count5>0:
                                    for key1,key2,key3 in zip(st.session_state.col_name_dict5.keys(),st.session_state.col_operator_dict5.keys(),st.session_state.col_value_dict5.keys()):
                                            col1,col2,col3,col4=st.columns([1,1,1,0.5])
                                            col1.write("**Select Column**")
                                            column1=col1.selectbox("",options=df2.columns,on_change = filter_state5,key = 'ColName5'+str(key1))
                                            col2.write("**Select the operator**")
                                            operator = col2.selectbox("",options=['==','!=','<','<=','>','>=','Like','Not Like','In','Not In','Is None','Is Not None'],on_change = filter_state5,key = 'ColOp5'+str(key2),index = None)
                                            col3.write("**Enter the compare value**")
                                            compare_value = col3.text_input("",on_change = filter_state5,key= 'ColValue5'+str(key3))
                                            col4.write("")
                                            col4.write("")
                                            col4.write("")
                                            col4.write("")
                                            col4.button("❌",key= 'Clear_filter5'+str(key1),on_click=clear_particular_filter_join2,args=(key1,key2,key3))

                                            st.session_state.col_name_dict5[key1] = st.session_state['ColName5'+str(key1)]
                                            st.session_state.col_operator_dict5[key2] = st.session_state['ColOp5'+str(key2)]
                                            st.session_state.col_value_dict5[key3] = st.session_state['ColValue5'+str(key3)]
                                
                                col1,col2,col3 = st.columns([1.3,1,4])
                                if 'rule_count5' in st.session_state and col1.button("Add Filter condition",on_click=increase_rule_count5,key='join2addfilter'):

                                    pass
                                
                                if 'rule_count5' in st.session_state and st.session_state.rule_count5 > 0:
                                    
                                    if col2.button("Clear Filter",on_click = clear_filters5,key='join2clearfilter'):
                                        pass
                                
                                if 'rule_count5' in st.session_state and st.session_state.rule_count5>0:
                                    if len(st.session_state.col_name_dict5) == len(st.session_state.col_operator_dict5) == len(st.session_state.col_value_dict5):
                                        

                                        if st.button("Apply Filter",on_click=apply_filter5,key='join2applyfilter') or 'apply_filter5' in st.session_state:

                                            try:    
                                                df2 = filter_dataframe(df2,st.session_state.col_name_dict5.values(),st.session_state.col_operator_dict5.values(),st.session_state.col_value_dict5.values())    
                                            except:
                                                st.error("Please select correct filter condition")
                                            else:
                                                # st.dataframe(df2.head(10))
                                                st.write("Number of records:",df2.shape[0])
    if df is not None and df2 is not None:

            renamed_second_df = rename_columns(df2)

            st.subheader("Select Join Keys")
            col1, col2 = st.columns(2)
            with col1:
                join_key_first = st.selectbox("Join Key from First Table", df.columns,on_change=remove_table_change)
            with col2:
                join_key_second = st.selectbox("Join Key from Second Table", renamed_second_df.columns,on_change=remove_table_change)

            first_df_unique = df.drop_duplicates(subset=[join_key_first])
            second_df_unique = renamed_second_df.drop_duplicates(subset=[join_key_second])

            if st.button("Join Profile") or 'table_prev' in st.session_state or 'table_change' in st.session_state:
                # Perform join operation
                try:
                    joined_df = pd.merge(df, renamed_second_df, left_on=join_key_first, right_on=join_key_second, how='outer', indicator=True)
                    joined_df_unique = pd.merge(first_df_unique, second_df_unique, left_on=join_key_first, right_on=join_key_second, how='outer', indicator=True)
                except KeyError as e:
                    st.error(f"KeyError: {e}")
                    return

                df_left=joined_df_unique[joined_df_unique['_merge'] == 'left_only']
                # st.write(df_left)
                df_right=joined_df_unique[joined_df_unique['_merge'] == 'right_only']
                # st.write(df_right)
                df_both=joined_df_unique[joined_df_unique['_merge'] == 'both']

                left_only_unique = (set([str(val).strip() for val in df_left[join_key_first].dropna()]))
                right_only_unique = (set([str(val).strip() for val in df_right[join_key_second].dropna()]))
                both_unique = (set([str(val).strip() for val in df_both[join_key_first].dropna()]))

                left_only_count_unique = len([val for val in left_only_unique if val not in both_unique])
                right_only_count_unique = len([val for val in right_only_unique if val not in both_unique])
                both_count_unique = len(both_unique)

                file_name_wo_ext = file_name.split('.')[0]
                file_name2_wo_ext = file_name2.split('.')[0]
                # Display Venn diagram
                st.subheader("Join Profile Results")
                columns_selcted=[join_key_first,join_key_second]
                inner_joined_df = pd.merge(df, renamed_second_df, left_on=join_key_first, right_on=join_key_second, how='left', indicator=True)

                
                venn_diagram(left_only_count_unique,right_only_count_unique,both_count_unique,file_name_wo_ext,file_name2_wo_ext)
                st.write('***Note: Any leading/trailing spaces in specified join key columns are ignored while performing Join Profiling.***')
                # st.write(joined_df_unique)
                inner_joined_df['cluster_id']=-1

                inner_joined_df['cluster_id']=inner_joined_df.groupby(inner_joined_df[join_key_first].tolist()).ngroup()

                df_left=joined_df[joined_df['_merge'] == 'left_only']
                # st.write(df_left)
                df_right=joined_df[joined_df['_merge'] == 'right_only']
                # st.write(df_right)
                df_both=joined_df[joined_df['_merge'] == 'both']
                # st.write(df_both)
                # Initialize session state variables


                if 'selected_table' not in st.session_state:
                    st.session_state.selected_table = 'Left Table'
                if 'previous_selection' not in st.session_state:
                    st.session_state.previous_selection = 'Left Table'
                if 'table_change' not in st.session_state:
                    st.session_state.table_change = False

                selected_table = st.selectbox(
                    'Select a table to preview',
                    ['Left Table', 'Right Table', 'Intersection Table'],
                    key='selected_table',
                    on_change=change_table
                )


                if st.button("Show",on_click=set_table_prev) or 'table_prev' in st.session_state:
                    if selected_table:
                        if selected_table == 'Left Table':
                            st.write(df_left.drop(columns=renamed_second_df.columns).head(30))
                        elif selected_table == 'Right Table':
                            st.write(df_right.drop(columns=df.columns).head(30))    
                        elif selected_table == 'Intersection Table':
                            st.write(joined_df_unique[joined_df_unique['_merge'] == 'both'].head(30))    