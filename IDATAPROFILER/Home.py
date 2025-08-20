import streamlit as st
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go 
from utility import *
import plotly.express as px
import time
from module import *
from SourceData import *
from datetime import datetime
import uuid
from streamlit_modal import Modal
from Filter import *
from DuplicateAnalysis import DuplicateAnalysis

def clear_filters():
    """
    Clears all filter-related session state variables.
    """
    if 'col_name_dict'  in st.session_state:
        del st.session_state.col_name_dict

    if 'col_operator_dict' in st.session_state:
        del st.session_state.col_operator_dict 

    if 'col_value_dict'  in st.session_state:
        del st.session_state.col_value_dict

    if 'rule_count' in st.session_state:
        del st.session_state.rule_count
    
    if 'filter'  in st.session_state:

        del st.session_state.filter
    
    filter_state()

def increase_rule_count():
    """
    Increments the rule count and initializes new filter rule entries.
    """
    if 'rule_count' in st.session_state:
        st.session_state.rule_count+=1
    st.session_state.col_name_dict['ColName'+str(st.session_state.rule_count)] = None
    st.session_state.col_operator_dict['ColOp'+str(st.session_state.rule_count)] = None 
    st.session_state.col_value_dict['ColValue'+str(st.session_state.rule_count)] = None
    if 'apply_filter' in st.session_state:
        del st.session_state.apply_filter
    filter_state()

def add_filter_conditions():
    """
    Initializes filter-related session state variables if not already present.
    """
    if 'col_name_dict' not in st.session_state:
        st.session_state.col_name_dict = {}

    if 'col_operator_dict' not in st.session_state:
        st.session_state.col_operator_dict = {}

    if 'col_value_dict' not in st.session_state:
        st.session_state.col_value_dict = {}

    if 'rule_count' not in st.session_state:
        st.session_state.rule_count = 0
    
    if 'filter' not in st.session_state:

        st.session_state.filter = 1
    if 'src_select_widget_change' in st.session_state:
        del st.session_state.src_select_widget_change

def modal_set():
    """
    Initializes modal-related session state variables.
    """
    if 'dict' not in st.session_state:
         st.session_state.dict={}

    if 'dicttype' not in st.session_state:
         st.session_state.dicttype = {}
    
    if 'close_modal' in st.session_state:
        del st.session_state.close_modal
    if 'src_select_widget_change' in st.session_state:
        del st.session_state.src_select_widget_change

def modal_session():
    """
    Sets the modal session state and clears the close modal flag.
    """
    st.session_state.modal = 1

    if 'close_modal' in st.session_state:
        del st.session_state.close_modal
  
def column_select_box():
    """
    Sets the column selection state.
    """
    st.session_state.column = 1

def dataset_select_box():
    """
    Sets the dataset selection state.
    """
    st.session_state.dataset = 1

def table_level_stat_download():
    """
    Sets the table-level statistics download state.
    """
    st.session_state.table = 1

def upload_level_state():
    """
    Initializes or resets session state variables related to data upload.
    """
    st.session_state.upload = 1
    if 'run_profile' in st.session_state:
        del st.session_state.run_profile

    if 'previous_selection' in st.session_state:
        st.session_state.previous_selection = None

    if 'enable_button' in st.session_state:
        del st.session_state.enable_button

    if 'time_taken' in st.session_state:
        del st.session_state.time_taken

def run_profile():
    """
    Sets the session state to indicate that profiling should run.
    """
    st.session_state.ran_profiling_summary_statistics = True
    st.session_state.run_profile = 1
    if 'src_select_widget_change' in st.session_state:
        del st.session_state.src_select_widget_change

def file_change_state():
    """
    Updates session state to reflect a file change and resets related variables.
    """
    st.session_state.file_change = 1

    if 'selected_columns' in st.session_state:
        del st.session_state.selected_columns
        print("column deleted")

    if 'previous_selection' in st.session_state:
        del st.session_state.previous_selection

    if 'run_profile' in st.session_state:
        del st.session_state.run_profile

    print("file changed")

def check_file_change():
    """
    Checks if the file change state is set to 2.

    Returns:
        bool: True if file_change is 2, else False.
    """
    if 'file_change' in st.session_state:
        return st.session_state.file_change == 2
 
def filter_state():
    """
    Sets the filter state and clears related session variables.
    """
    st.session_state.filter=1
    if 'run_profile' in st.session_state:
        del st.session_state.run_profile

    if 'apply_filter' in st.session_state:
        del st.session_state.apply_filter

    if 'enable_button' in st.session_state:
        del st.session_state.enable_button

    if 'time_taken' in st.session_state:
        del st.session_state.time_taken

def clear_source():
    """
    Clears the source data and related session state variables.
    """
    if 'dataframe' in st.session_state:
        del st.session_state.dataframe
    
    if 'run_profile' in st.session_state:
        del st.session_state.run_profile

    if 'filter' in st.session_state:
         del st.session_state.filter

    st.session_state.source_change = 1

def apply_filter():
    """
    Sets the apply_filter flag in session state.
    """
    if 'apply_filter' not in st.session_state:
        st.session_state.apply_filter=1

def close_modal():
    """
    Sets the close_modal flag in session state.
    """
    if 'close_model' not in st.session_state:
        st.session_state.close_modal = 1

def delete_dataset():
    """
    Deletes the dataset key from session state if it exists.
    """
    if 'dataset' in st.session_state:
        del st.session_state.dataset

def set_map_count():
    """
    Initializes mapping-related session state variables.
    """
    if 'map' not in st.session_state:
        st.session_state.map={}
    if 'datatype_map' not in st.session_state:
         st.session_state.datatype_map = {}
    st.session_state.add_map = 1

    if 'dict' not in st.session_state:
         st.session_state.dict={}

    if 'dicttype' not in st.session_state:
         st.session_state.dicttype = {}

    if 'execute_map' in st.session_state:
         del st.session_state.execute_map

    if 'show_datatype_view' in st.session_state:
         del st.session_state.show_datatype_view

def del_map_count():
    """
    Sets the del_map flag in session state.
    """
    st.session_state.del_map =1

def set_execute_mapping():
    """
    Sets the execute_map flag in session state.
    """
    st.session_state.execute_map =1
    
def datatype_change(numeric_columns,df):
    """
    Converts numeric columns to Int64 if possible, otherwise to Float64.

    Args:
        numeric_columns (list): List of numeric column names.
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: The modified DataFrame with updated data types.
    """
    def convert_to_int_if_whole(x):
        if x is None or pd.isna(x):
            return pd.NA
        if x==int(x):
            return int(x)
        return float(x)
    for column1 in numeric_columns:
        df[column1] = df[column1].apply(convert_to_int_if_whole)
        try:
            df[column1] = df[column1].astype('Int64')
        except:
            df[column1] = df[column1].astype('Float64')
    return df

def connect_to_source_data_new():
    """
    Connects to a selected data source and returns the DataFrame and file name.

    Returns:
        tuple: A tuple containing the DataFrame and the file name.
    """
    st.subheader('Connect to Source Data')

    ques = st.radio("",('Flat File','ADLS Blob','S3 Bucket','Azure SQL DB','Databricks DBFS'),on_change=reset,key='newsourcefile')  #tsv,psv,parquet
    
    if ques == 'Flat File':
        df,file_name = flat('flatfile')
        
    if ques == 'ADLS Blob':
        df,file_name=adl('adlsfile')

    if ques == 'S3 Bucket':
        df,file_name = s3_buc('s3bucketfile')
        
    if ques == 'Azure SQL DB':
        df,file_name = sdb('sqldbfile')

    if ques == 'Databricks DBFS': 
        df,file_name = databr('databricksfile')
            
    if ques == 'Standard SQL Server':
        df,file_name = stsq('stsqlfile')
        
    if ques == 'API':
        df,file_name = apicall('apicallfile')
    
    return df,file_name

def connect_to_source_data(source):
    """
    Connects to a selected data source and returns the DataFrame and file name.

    Args:
        source (str): Identifier for the source context.

    Returns:
        tuple: A tuple containing the DataFrame and the file name.
    """
    st.subheader('Connect to Source Data')

    ques = st.radio("",('Flat File','ADLS Blob','S3 Bucket','Azure SQL DB','Databricks DBFS'),on_change=reset)  #tsv,psv,parquet
    
    if ques == 'Flat File':
        df,file_name = flat('flat',source)
        
    if ques == 'ADLS Blob':
        df,file_name=adl('adls',source)

    if ques == 'S3 Bucket':
        df,file_name = s3_buc('s3bucket',source)
        
    if ques == 'Azure SQL DB':
        df,file_name = sdb('sqldb',source)

    if ques == 'Databricks DBFS': 
        df,file_name = databr('databricks',source)
            
    # if ques == 'Standard SQL Server':
    #     df,file_name = stsq('stsql')
        
    # if ques == 'API':
    #     df,file_name = apicall('apicall')
    
    return df,file_name

def clear_particular_filter(key1,key2,key3):
    """
    Clears a specific filter rule from session state.

    Args:
        key1 (str): Key for column name.
        key2 (str): Key for operator.
        key3 (str): Key for value.
    """
    del st.session_state.col_name_dict[key1] 
    del st.session_state.col_operator_dict[key2] 
    del st.session_state.col_value_dict[key3]
    
    if len(st.session_state.col_name_dict) == 0: 
        st.session_state.rule_count = 0
    filter_state()

def Home():
        """
        Main function to render the home page and manage data profiling workflow.
        """
        col1,col2,col3 = st.columns(3)
        '''Buttons to view the specific report for profiling'''
        table_button = None
        column_button = None
        upload_file = None
        duplicate_button = None

        if 'dataframe' not in st.session_state and 'file_name' not in st.session_state:
                st.session_state.dataframe = None
                st.session_state.file_name=None
                
        if 'run_profile' in st.session_state and 'enable_button' in st.session_state:
                table_button = st.sidebar.button("Summary Profile",use_container_width=True, disabled= True if 'run_profile' not in st.session_state else False,on_click=delete_dataset )
                column_button = st.sidebar.button("Column Profile",use_container_width=True, disabled= True if 'run_profile' not in st.session_state else False,on_click=delete_dataset)
                duplicate_button = st.sidebar.button("Match Analysis",use_container_width=True, disabled= True if 'run_profile' not in st.session_state else False,on_click=delete_dataset)
                upload_file = st.sidebar.button("Start Over",use_container_width=True, disabled= True if 'run_profile' not in st.session_state else False,on_click=reset)
       
        '''Check if dataframe is already available if not connect to new data source '''
        
        
        if upload_file is None or upload_file or 'source_change' in st.session_state or 'dataframe' in st.session_state:
                
                if (not table_button and not column_button and not 'column' in st.session_state and not 'table' in st.session_state and not duplicate_button and 'duplicate_analysis' not in st.session_state and 'new_dup_src' not in st.session_state):
                        
                        st.title("Comprehensive Column Profiling")
                        
                        if 'original_dataframe' not in st.session_state or 'src_select_widget_change_CCP' in st.session_state :
                            df,file_name = connect_to_source_data("ccp_source_data")
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
                            if st.toggle("New Source",key='newccpsource'):
                                df = None
                                file_name = None
                                df,file_name = connect_to_source_data("ccp_source_data")
                                
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

                        if df is not None and file_name is not None:  
                            st.session_state.dataframe = df
                            st.session_state.file_name = file_name
                            st.session_state.original_dataframe = df
            
                        if 'dataframe' in st.session_state  and st.session_state.dataframe is not None and df is not None:
                                    
                                    st.subheader(f"**File Name: {st.session_state.file_name}**")
                                    st.write("_**Sample view of dataset**_")

                                    if 'dataframe' in st.session_state:
                                        
                                        st.dataframe(st.session_state.dataframe.head(10).style.format(thousands=''),hide_index=True)
                                        
                                        try:
                                            modal = Modal(key="ListOfRules", title='Review and Adjust Datatype', max_width=800)
                                            open_modal = st.button(label='View Dataset Columns and DataTypes', key="listOfRules" , on_click = modal_set)
                                            if open_modal or 'modal' in st.session_state:
                                                with modal.container():
                                                    col1,col2,col3 = st.columns([1.5,3,1])

                                                    data_type_mapping = {
                                                        "str": "object",
                                                        "int": pd.Int64Dtype(),
                                                        "float": "float",
                                                        "datetime": "datetime64"                                
                                                    }
                                                    selected_columns = []
                                                    selected_data_types = []

                                                    col1,col2,col3 = st.columns(3)

                                                    col1.write("**Column Name**")
                                                    col2.write("**Data type**")
                                                    col3.write("**Overwrite Data type**")
                                                    if 'update_dataframe' in st.session_state:
                                                        st.session_state.dataframe = st.session_state.update_dataframe[:]
                                                    
                                                    #Display default datatype of column and allow to select the datatype to convert the datatype of column.

                                                    for col in st.session_state.dataframe.columns:
                                                        st.markdown("""<hr style="height:1px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

                                                        def assign_default_datatype(col):
                                                            if str(st.session_state.dataframe[col].dtype).lower().startswith('object'):
                                                                return 0
                                                            elif str(st.session_state.dataframe[col].dtype).lower().startswith('int'):
                                                                return 1
                                                            elif str(st.session_state.dataframe[col].dtype).lower().startswith('float'):
                                                                return 2
                                                            elif str(st.session_state.dataframe[col].dtype).lower().startswith('date'):
                                                                return 3
                                                            else:
                                                                return 0

                                                        col1,col2,col3 = st.columns(3)
                                                        original_data_type = str( st.session_state.dataframe[col].dtype)
                                                        user_selected_data_type = col3.selectbox(f"", list(data_type_mapping.keys()), index=assign_default_datatype(col),key = str(col) + "dtype",on_change=modal_session)
                                                        selected_columns.append(col)
                                                        selected_data_types.append(data_type_mapping[user_selected_data_type])
                                                        st.session_state.dict[str(col)] = col
                                                        st.session_state.dicttype[str(col)+'dtype'] = data_type_mapping[user_selected_data_type]


                                                
                                                        col1.write("")
                                                        col1.write("")
                                                        col1.write(col)
                                                        col2.write("")
                                                        col2.write("")
                                                        col2.write(original_data_type)
                                                        st.write("")

                                                        
                                                    col1.write("")   
                                                    col1.write("")  
                                                    st.markdown("""<hr style="height:1px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True) 

                                                    #Adjust the datatype of columns for which adjustment is needed
                                                    if st.button("Adjust datatype",on_click = close_modal) or 'close_modal' in st.session_state:
                                                        
                                                        modal_df = pd.DataFrame(st.session_state.dataframe.dtypes).reset_index()
                                                        for column1, data_type in zip(st.session_state.dict.values(),st.session_state.dicttype.values()):
                                                                
                                                                if 'int' in str(data_type).lower():
                                                                    try: 
                                                                        st.session_state.dataframe[column1] = np.floor(pd.to_numeric(st.session_state.dataframe[column1], errors='coerce')).astype(pd.Int64Dtype())
                                                                        
                                                                    except Exception as e:
                                                                        st.error(e)
                                                                elif 'float' in str(data_type).lower():
                                                                    try:
                                                                        st.session_state.dataframe[column1] = pd.to_numeric(st.session_state.dataframe[column1], errors='coerce')
                                                                    except Exception as e:
                                                                        st.error(e)
                                                                elif str(data_type).lower().startswith('object'):
                                                                    try:
                                                                        st.session_state.dataframe[column1] = np.where(pd.isnull(st.session_state.dataframe[column1]),st.session_state.dataframe[column1],st.session_state.dataframe[column1].astype(str))     
                                                                    except Exception as e:
                                                                        st.error(e)
                                                                elif str(data_type).lower().startswith('datetime'):
                                                                    try:
                                                                        st.session_state.dataframe[column1] = pd.to_datetime(st.session_state.dataframe[column1],format='mixed')
                                                                    except Exception as e:
                                                                        st.error(e)
                                                        else:
                                                            st.session_state.update_dataframe = st.session_state.dataframe[:]
                                                        
                                                            modal_df.rename(columns={'index':'Column Name',0:'Original Datatype'},inplace=True)
                                                            modal_df['Adjusted DataType'] = st.session_state.update_dataframe.dtypes.values
                                                            col1,col2,col3 = st.columns([1,2,1])

                                                            #display the adjusted datatype
                                                            col2.subheader("Adjusted Datatypes")

                                                            col2.data_editor(modal_df,hide_index=True,column_config=None,disabled=True)

                                                            if 'modal' in st.session_state:
                                                                del st.session_state.modal

                                                                st.session_state.modal_set1 = 1
                                        except Exception as e:
                                            st.error(e)
            
                                        st.markdown("""<hr style="height:1px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)


                                    col1,col2,col3=st.columns(3)

                                #----------------------------------------------------------------------------------

                                    if 'dataframe' in st.session_state and st.session_state.dataframe is not None:

                                        #Assign the dataframe with adjusted datatype if user has adjusted datatype of any of the columns.
                                        #else select the original dataframe without any adjustment.
                                        if 'modal_set1' in st.session_state:            
                                            df = st.session_state.update_dataframe
                                        else:
                                            df = st.session_state.dataframe
                                        df = st.session_state.update_dataframe if 'modal_set1' in st.session_state else st.session_state.dataframe
                                        st.write("**Select Primary Key Columns:**")
                                        st.session_state.primary_columns = st.multiselect('',options=df.columns,default=None,key = 'Primarykey')
                                        st.text("")


                                        #Select the filter condition to apply it to dataframe.
                                        if st.button("Include/Exclude Source Data",on_click=add_filter_conditions) or 'filter' in st.session_state:
                                            if 'rule_count' in st.session_state and st.session_state.rule_count>0:
                                                for key1,key2,key3 in zip(st.session_state.col_name_dict.keys(),st.session_state.col_operator_dict.keys(),st.session_state.col_value_dict.keys()):
                                                    col1,col2,col3,col4=st.columns([1,1,1,0.5])
                                                    col1.write("**Select Column**")
                                                    column1=col1.selectbox("",options=df.columns,on_change = filter_state,key = 'ColName'+str(key1))
                                                    col2.write("**Select the operator**")
                                                    operator = col2.selectbox("",options=['==','!=','<','<=','>','>=','Like','Not Like','In','Not In','Is None','Is Not None'],on_change = filter_state,key = 'ColOp'+str(key2),index = None)
                                                    col3.write("**Enter the compare value**")
                                                    compare_value = col3.text_input("",on_change = filter_state,key= 'ColValue'+str(key3))
                                                    col4.write("")
                                                    col4.write("")
                                                    col4.write("")
                                                    col4.text("")
                                                    
                                                    
                                                    col4.button("❌",key= 'Clear_filter'+str(key1),on_click=clear_particular_filter,args=(key1,key2,key3))

                                                    #storing the column, operator and comparison value for respective column selected for filtering the dataframe
                                                    st.session_state.col_name_dict[key1] = st.session_state['ColName'+str(key1)]
                                                    st.session_state.col_operator_dict[key2] = st.session_state['ColOp'+str(key2)]
                                                    st.session_state.col_value_dict[key3] = st.session_state['ColValue'+str(key3)]
                                            
                                            col1,col2,col3 = st.columns([1.3,1,4])
                                            if 'rule_count' in st.session_state and col1.button("Add Filter condition",on_click=increase_rule_count):
                                                pass
                                            
                                            if 'rule_count' in st.session_state and st.session_state.rule_count > 0:  
                                                if col2.button("Clear All Filters",on_click = clear_filters):
                                                    pass

                                            if 'rule_count' in st.session_state and st.session_state.rule_count > 0:
                                                    
                                                    #Check if all the filter fields are populated with data.
                                                    if len(st.session_state.col_name_dict) == len(st.session_state.col_operator_dict) == len(st.session_state.col_value_dict):
                                                        if st.button("Apply Filter",on_click=apply_filter) or 'apply_filter' in st.session_state:

                                                            try:
                                                                #Filter the dataframe
                                                                df = filter_dataframe(df,st.session_state.col_name_dict.values(),st.session_state.col_operator_dict.values(),st.session_state.col_value_dict.values())
                                                                
                                                            except:
                                                                st.error("Please select correct filter condition")
                    
                                                            else:
                                                                st.write(f"Number of Source records considered for Profiling : {df.shape[0]} ({round(df.shape[0]*100/st.session_state.dataframe.shape[0],2)}%)")
                                    if 'dataframe' in st.session_state and st.session_state.dataframe is not None:
                                                
                                                #select the critical data elements for profiling.
                                                print("Entered selected column")

                                                st.text("")
                                                st.text("")

                                                st.write("**Select Critical Data Elements. (Optional)**")
                                                
                                                st.session_state.selected_columns = st.multiselect("", st.session_state.dataframe.columns, on_change=upload_level_state)

                                                if len(st.session_state.selected_columns)!=0:
                                                    st.session_state.filtered_df = df[st.session_state.selected_columns]
                                                                                     
                                                else:
                                                    st.session_state.filtered_df = df

                                                if st.button("Run Profile",on_click=run_profile):
                                                    st.session_state.file_change = 2
                                                    progress_text = "Operation in progress. Please wait."
                                                    my_bar = st.progress(0, text=progress_text)
                                                    for percent_complete in range(100):
                                                        time.sleep(0.01)
                                                        my_bar.progress(percent_complete + 1, text=progress_text)
                                                    else:

                                                        #----------------------------------------------------------------
                                                        start_time = time.time()
                                                        
                                                        try:
                                                            #profiling the dataframe and storing the results in session state variables to reuse the results in other modules.
                                                            pattern_df =pd.DataFrame()
                                                            st.session_state.pattern_df = create_pattern_dataframe(pattern_df,st.session_state.filtered_df[:])
                                                            
                                                            st.session_state.profile_df,st.session_state.insight_df = help_me_profile(st.session_state.filtered_df[:],st.session_state.pattern_df)
                                                            st.session_state.completeness_df = compute_data_completeness(st.session_state.profile_df)
                                                            st.session_state.uniqueness_df = compute_data_uniqueness(st.session_state.profile_df)
                                                            
                                                            st.session_state.length_df = find_length_of_all_columns(st.session_state.filtered_df[:],st.session_state.profile_df)
                                                            
                                                        except Exception as e:
                                                            st.error(e)
                                                        else:
                                                            st.session_state.profile_time = datetime.now().date()
                                                            end_time = time.time()
                                                            
                                                            st.session_state.enable_button = 1
                                                            st.session_state.time_taken = end_time-start_time
                                                            st.rerun()
                                                    time.sleep(1)
                                                    my_bar.empty()

                                                if 'time_taken' in st.session_state:
                                                    st.success("Completed Successfully")
                                                    st.success(f"Time Elapsed: {round(st.session_state.time_taken,3)} Seconds")
                                                    
        #Execute the below if condition block when user clicks on summary profile results.
        if table_button or 'table' in st.session_state:
            if 'run_profile' in st.session_state and 'profile_df' in st.session_state:
 
                try:
                    table()
                except Exception as e:
                    st.error(e)
            else:
                st.error("Please select the file OR Run profiling")

        #Execute the below if condition block when user clicks on column summary.
        if column_button or 'column' in st.session_state :

            if 'run_profile' in st.session_state and 'profile_df' in st.session_state:
                try:
                    column()
                except Exception as e:
                    st.error(e)
            else:
                st.error("Please select the file OR Run profiling")

        #Execute below if condition block when user clicks on match analysis botton.
        if duplicate_button or 'duplicate_analysis' in st.session_state or 'new_dup_src' in st.session_state:

             if 'run_profile' in st.session_state and 'profile_df' in st.session_state :                
                try:
                    DuplicateAnalysis()
                except Exception as e:
                   st.error(e)
             
             else:
                st.error("Please select the file OR Run profiling")
