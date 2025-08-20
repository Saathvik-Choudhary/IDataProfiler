# CrossTable.py handles the cross table filters used in the application.

import uuid
import streamlit as st
import pandas as pd
from ValidationRules.ListofCustomRules import *
from SourceData import *
from utility import scroll_up_button, write_to_sql, ShowDFAndChart, write_to_sql_trend,convert_to_int_if_whole
from streamlit_modal import Modal
import matplotlib.pyplot as plt
import plotly.express as px 
from Filter import *
from  datetime import date

# Initialize an empty DataFrame
resultDF2=pd.DataFrame 
Composite_Condition = ''

def apply_filter2():
    """
    Sets the 'apply_filter2' flag in the session state to 1.
    """
    if 'apply_filter2' not in st.session_state:
          st.session_state.apply_filter2=1

def filter_state2():
    """
    Sets the 'filter2' flag in the session state to 1 and removes 'apply_filter2' if it exists.
    """
    st.session_state.filter2=1
    if 'apply_filter2' in st.session_state:
        del st.session_state.apply_filter2

def clear_filters2():
    """
    Clears all filter-related session state variables for filter 2.
    """
    if 'col_name_dict2'  in st.session_state:
        del st.session_state.col_name_dict2

    if 'col_operator_dict2' in st.session_state:
        del st.session_state.col_operator_dict2

    if 'col_value_dict2'  in st.session_state:
        del st.session_state.col_value_dict2

    if 'rule_count2' in st.session_state:
        del st.session_state.rule_count2
        
    if 'filter2'  in st.session_state:
        del st.session_state.filter2

def increase_rule_count2():
    """
    Increases the rule count for filter 2 and initializes corresponding dictionaries in the session state.
    """
    if 'rule_count2' in st.session_state:
        st.session_state.rule_count2+=1
    st.session_state.col_name_dict2['ColName'+str(st.session_state.rule_count2)] = None
    st.session_state.col_operator_dict2['ColOp'+str(st.session_state.rule_count2)] = None 
    st.session_state.col_value_dict2['ColValue'+str(st.session_state.rule_count2)] = None

    if 'apply_filter2' in st.session_state:
        del st.session_state.apply_filter2

def publish_collibra_result_cross_table():
    """
    Sets the 'collibra_crosstable' flag in the session state to 1 and removes 'publish_collibra_cross_table' if it exists.
    """
    if 'collibra_crosstable' not in st.session_state:
        st.session_state.collibra_crosstable = 1

    if 'publish_collibra_cross_table' in st.session_state:
        del st.session_state.publish_collibra_cross_table

def remove_collibra_state_cross_table():
    """
    Sets the 'publish_collibra_cross_table' flag in the session state to 1.
    """
    if 'publish_collibra_cross_table' not in st.session_state:
        st.session_state.publish_collibra_cross_table = 1

def add_filter_conditions2():
    """
    Initializes filter-related dictionaries and sets the 'filter2' flag in the session state.
    """
    if 'col_name_dict2' not in st.session_state:
        st.session_state.col_name_dict2 = {}

    if 'col_operator_dict2' not in st.session_state:
        st.session_state.col_operator_dict2 = {}

    if 'col_value_dict2' not in st.session_state:
        st.session_state.col_value_dict2 = {}

    if 'rule_count2' not in st.session_state:
        st.session_state.rule_count2 = 0
    
    if 'filter2' not in st.session_state:

        st.session_state.filter2 = 1

def apply_filter3():
    """
    Sets the 'apply_filter3' flag in the session state to 1.
    """
    if 'apply_filter3' not in st.session_state:
        st.session_state.apply_filter3=1

def filter_state3():
    """
    Sets the 'filter3' flag in the session state to 1 and removes 'apply_filter3' if it exists.
    """
    st.session_state.filter3=1
    if 'apply_filter3' in st.session_state:
        del st.session_state.apply_filter3

def clear_filters3():
    """
    Clears all filter-related session state variables for filter 3.
    """
    if 'col_name_dict3'  in st.session_state:
        del st.session_state.col_name_dict3

    if 'col_operator_dict3' in st.session_state:
        del st.session_state.col_operator_dict3

    if 'col_value_dict3'  in st.session_state:
        del st.session_state.col_value_dict3

    if 'rule_count3' in st.session_state:
        del st.session_state.rule_count3
    
    if 'filter3'  in st.session_state:

        del st.session_state.filter3

def increase_rule_count3():
    """
    Increases the rule count for filter 3 and updates the corresponding dictionaries in the session state.
    """
    if 'rule_count3' in st.session_state:
        st.session_state.rule_count3+=1
    st.session_state.col_name_dict3['ColName'+str(st.session_state.rule_count3)] = None
    st.session_state.col_operator_dict3['ColOp'+str(st.session_state.rule_count3)] = None 
    st.session_state.col_value_dict3['ColValue'+str(st.session_state.rule_count3)] = None

    if 'apply_filter3' in st.session_state:
        del st.session_state.apply_filter3


def add_filter_conditions3():
    """
    Initializes the dictionaries and rule count for filter 3 in the session state if they do not already exist.
    """
    if 'col_name_dict3' not in st.session_state:
        st.session_state.col_name_dict3 = {}

    if 'col_operator_dict3' not in st.session_state:
        st.session_state.col_operator_dict3 = {}

    if 'col_value_dict3' not in st.session_state:
        st.session_state.col_value_dict3 = {}

    if 'rule_count3' not in st.session_state:
        st.session_state.rule_count3 = 0

    if 'filter3' not in st.session_state:
        st.session_state.filter3 = 1

def session_setting():
    """
    Sets initial values for cross profiling table and CTDQ modal in the session state.
    """
    st.session_state.crossProfilingTable = 1
    st.session_state.CTDQModal = 1

def session_setting_rule():
    """
    Initializes the cross table rule dictionary in the session state if it does not already exist.
    """
    if 'crossTableRule' not in st.session_state:
         st.session_state.crossTableRule = {}

def connect_to_source_crosstable(source):
    """
    Connects to the source data based on the selected option and returns the dataframe and file name.
    """
    st.subheader('Connect to Source Data')
    ques = st.radio("", ('Flat File', 'ADLS Blob', 'S3 Bucket', 'Azure SQL DB', 'Databricks DBFS'), key='SourceTableRadio', on_change=reset_crosscolumn_df1)

    if ques == 'Flat File':
        df1, file_name1 = flat('SourceTableFile', source)
    if ques == 'ADLS Blob':
        df1, file_name1 = adl('SourceTableADLS', source)
    if ques == 'S3 Bucket':
        df1, file_name1 = s3_buc('SourceTableS3', source)
    if ques == 'Azure SQL DB':
        df1, file_name1 = sdb('SourceTableSQL', source)
    if ques == 'Databricks DBFS':
        df1, file_name1 = databr('SourceTableSQLdatabricks', source)
    if ques == 'Standard SQL Server':
        df1, file_name1 = stsq('SourceTableSQLstsql', source)
    if ques == 'API':
        df1, file_name1 = apicall('SourceTableSQLapicall', source)
    
    return df1, file_name1

def funcAnd(a, b):
    """
    Returns 'PASS' if both inputs are 'PASS', otherwise returns 'FAIL'.
    """
    if a == 'PASS' and b == 'PASS':
        return 'PASS'
    elif a == 'FAIL' or b == 'FAIL':
        return 'FAIL'
    return None

def funcOr(a, b):
    """
    Returns 'PASS' if either input is 'PASS', otherwise returns 'FAIL'.
    """
    if a == 'PASS' or b == 'PASS':
        return 'PASS'
    elif a == 'FAIL' and b == 'FAIL':
        return 'FAIL'
    return None

def clear_particular_filter_crs_tbl(key1, key2, key3):
    """
    Clears specific filter criteria from the session state for cross table 2.
    """
    if key1 in st.session_state.col_name_dict2:
        del st.session_state.col_name_dict2[key1]
        del st.session_state.col_operator_dict2[key2]
        del st.session_state.col_value_dict2[key3]

    if len(st.session_state.col_name_dict2) == 0:
        st.session_state.rule_count2 = 0

def clear_particular_filter_crs_tbl2(key1, key2, key3):
    """
    Clears specific filter criteria from the session state for cross table 3.
    """
    if key1 in st.session_state.col_name_dict3:
        del st.session_state.col_name_dict3[key1]
        del st.session_state.col_operator_dict3[key2]
        del st.session_state.col_value_dict3[key3]

    if len(st.session_state.col_name_dict3) == 0:
        st.session_state.rule_count3 = 0

def CrossTableProfiling():
    """
    Function to perform Cross Table Profiling
    """
    #Get source data 1
    st.title("Cross Table Profiling")
    
    if 'original_dataframe' not in st.session_state or 'src_select_widget_change_source_CTP' in st.session_state:
                df1,file_name1 = connect_to_source_crosstable('source_data')
                if df1 is not None: 
                            numeric_columns = df1.select_dtypes(include=np.number).columns.to_list()                    
                            for column1 in numeric_columns:
                                df1[column1] = df1[column1].apply(convert_to_int_if_whole)
                                try:
                                    df1[column1] = df1[column1].astype('Int64')
                                except:
                                    df1[column1] = df1[column1].astype('Float64')
                            
                            st.session_state.original_dataframe = df1
                            st.session_state.file_name = file_name1
    else:
        df1 =  st.session_state.original_dataframe
        file_name1 = st.session_state.file_name
        if st.toggle("New Source",key='newcrosssource'):
            df1 = None
            file_name1 = None
            df1,file_name1 = connect_to_source_crosstable('source_data')
            
            if df1 is not None: 
                numeric_columns = df1.select_dtypes(include=np.number).columns.to_list()                    
                for column1 in numeric_columns:
                    df1[column1] = df1[column1].apply(convert_to_int_if_whole)
                    try:
                        df1[column1] = df1[column1].astype('Int64')
                    except:
                        df1[column1] = df1[column1].astype('Float64')
                
                st.session_state.original_dataframe = df1
                st.session_state.file_name = file_name1
                df1 =  st.session_state.original_dataframe
                file_name1 = st.session_state.file_name
         

    if df1 is not None and file_name1 is not None:
                   
 
        st.subheader(f"**File Name: {file_name1}**")
        st.write("**_Sample view of dataset_**")
        st.dataframe(df1.head(10), hide_index=True)
        
        ruleList, summaryStats = st.columns([1.5, 0.5])
        modal = Modal(key="ListOfRules", title='', max_width=1000)
        open_modal = ruleList.button(label='List of Supported Rules', key="listOfRulesCrossTable", on_click=session_setting())
        if open_modal:
                with modal.container():
                    central_dq_repo_df = pd.read_csv('IDATAPROFILER/static/CrossTableDQRepository.csv',encoding_errors= 'replace')
                    st.data_editor(central_dq_repo_df, disabled=set(central_dq_repo_df.columns) - set(['Category', 'Priority']), hide_index=True, key='tableOfListOfRulesCrossTable')
        
        st.write("**Select Primary Key Columns:**")
        st.session_state.primary_columns = st.multiselect('',options=df1.columns,default=None,key = 'PrimarykeyRuleProfilingnew')
        st.text("")
        if 'primary_columns' in st.session_state and len(st.session_state.primary_columns) > 0:
               st.write(f"**Primary Key Columns: {st.session_state.primary_columns}**")
        else:
               st.write(f"**Primary Key is not selected**")
        st.session_state.source_system = st.text_input("**Enter Source System Name: (Mandatory to Publish Cross Table Summary Results to Database)**",key='Sourcesystem_cross_table_profile')
        if st.button("Include/Exclude Source Data",on_click=add_filter_conditions2,key=1) or 'filter2' in st.session_state:
                                
                                if 'rule_count2' in st.session_state and st.session_state.rule_count2>0:
                                    for key1,key2,key3 in zip(st.session_state.col_name_dict2.keys(),st.session_state.col_operator_dict2.keys(),st.session_state.col_value_dict2.keys()):
                                            col1,col2,col3,col4=st.columns([1,1,1,0.3])
                                            col1.write("**Select Column**")
                                            column1=col1.selectbox("",options=df1.columns,on_change = filter_state2,key = 'ColName'+str(key1))
                                            col2.write("**Select the operator**")
                                            operator = col2.selectbox("",options=['==','!=','<','<=','>','>=','Like','Not Like','In','Not In','Is None','Is Not None'],on_change = filter_state2,key = 'ColOp'+str(key2),index = None)
                                            col3.write("**Enter the compare value**")
                                            compare_value = col3.text_input("",on_change = filter_state2,key= 'ColValue'+str(key3))
                                            col4.write("")
                                            col4.write("")
                                            col4.write("")
                                            col4.write("")
                                            col4.button("❌",key= 'Clear_filter2'+str(key1),on_click=clear_particular_filter_crs_tbl,args=(key1,key2,key3))
                                            st.session_state.col_name_dict2[key1] = st.session_state['ColName'+str(key1)]
                                            st.session_state.col_operator_dict2[key2] = st.session_state['ColOp'+str(key2)]
                                            st.session_state.col_value_dict2[key3] = st.session_state['ColValue'+str(key3)]
                                
                                col1,col2,col3 = st.columns([1.3,1,4])
                                if 'rule_count2' in st.session_state and col1.button("Add Filter condition",on_click=increase_rule_count2,key='AddFilter1'):

                                    # st.rerun()
                                    pass
                                
                                if 'rule_count2' in st.session_state and st.session_state.rule_count2 > 0:
                                    
                                    if col2.button("Clear All Filters",on_click = clear_filters2,key='ClearFilter1'):
                                        pass
                                
                                if 'rule_count2' in st.session_state and st.session_state.rule_count2>0:
                                        if len(st.session_state.col_name_dict2) == len(st.session_state.col_operator_dict2) == len(st.session_state.col_value_dict2):
                                            

                                            if st.button("Apply Filter",on_click=apply_filter2,key='ApplyFilter1') or 'apply_filter2' in st.session_state:
                                                try:
                                                    df1 = filter_dataframe(df1,st.session_state.col_name_dict2.values(),st.session_state.col_operator_dict2.values(),st.session_state.col_value_dict2.values())
                                                except:
                                                    st.error("Please select correct filter condition")
                                                else: 
                                                   
                                                    st.write(f"Number of Source records : {df1.shape[0]} ")
                                                    

        
    st.write('')  

    #Get source data 2
    if df1 is not None:
          st.session_state.common_data = df1[:]
          st.session_state.file_name = file_name1
    st.subheader('Connect to Reference Source')
    ques = st.radio("",('Flat File','ADLS Blob','S3 Bucket','Azure SQL DB','Databricks DBFS'), key='ReferenceTableRadio',on_change=reset_crosscolumn_df2)  #tsv,psv,parquet

    if ques == 'Flat File':
        df2,file_name2 = flat('ReferenceTableFile','reference_source')

    if ques == 'ADLS Blob':
        print("Entry")
        df2,file_name2=adl('ReferenceTableADLS','reference_source')
                
    if ques == 'S3 Bucket':
        print("S3 bucket")
        df2,file_name2 = s3_buc('ReferenceTableS3','reference_source')
                
    if ques == 'Azure SQL DB':
        print("Sql DB")
        df2,file_name2 = sdb('ReferenceTableSQL','reference_source')

    if ques == 'Databricks DBFS':
        df2,file_name2 = databr('ReferenceTabledatabricks','reference_source')
        
    if ques == 'Standard SQL Server':
          df2,file_name2 = stsq('ReferenceTableSQLstsql','reference_source')

    if ques == 'API':
          df2,file_name2 = apicall('ReferenceTableSQLapicall','reference_source')
    
    if df2 is not None:
        st.write("**_Sample view of dataset_**")
        st.dataframe(df2.head(10), hide_index=True)

    st.write('')

    if df2 is not None:
        df = df2
        numeric_columns = df.select_dtypes(include=np.number).columns.to_list()                    
        for column1 in numeric_columns:
            df[column1] = df[column1].apply(convert_to_int_if_whole)
            try:
                df[column1] = df[column1].astype('Int64')
            except:
                df[column1] = df[column1].astype('Float64')
       
        if st.button("Include/Exclude Source Data",on_click=add_filter_conditions3,key=2) or 'filter3' in st.session_state:
                                
                                if 'rule_count3' in st.session_state and st.session_state.rule_count3>0:
                                    for key1,key2,key3 in zip(st.session_state.col_name_dict3.keys(),st.session_state.col_operator_dict3.keys(),st.session_state.col_value_dict3.keys()):
                                        col1,col2,col3,col4=st.columns([1,1,1,0.3])
                                        col1.write("**Select Column**")
                                        column1=col1.selectbox("",options=df.columns,on_change = filter_state3,key = 'ColName1'+str(key1))
                                        col2.write("**Select the operator**")
                                        operator = col2.selectbox("",options=['==','!=','<','<=','>','>=','Like','Not Like','In','Not In','Is None','Is Not None'],on_change = filter_state3,key = 'ColOp1'+str(key2),index = None)
                                        col3.write("**Enter the compare value**")
                                        compare_value = col3.text_input("",on_change = filter_state3,key= 'ColValue1'+str(key3))
                                        col4.write("")
                                        col4.write("")
                                        col4.write("")
                                        col4.write("")
                                        col4.button("❌",key= 'Clear_filter'+str(key1),on_click=clear_particular_filter_crs_tbl2,args=(key1,key2,key3))
                                        st.session_state.col_name_dict3[key1] = st.session_state['ColName1'+str(key1)]
                                        st.session_state.col_operator_dict3[key2] = st.session_state['ColOp1'+str(key2)]
                                        st.session_state.col_value_dict3[key3] = st.session_state['ColValue1'+str(key3)]
                                
                                col1,col2,col3 = st.columns([1.3,1,4])
                                if  'rule_count3' in st.session_state and col1.button("Add Filter condition",on_click=increase_rule_count3,key='AddFilter2'):

                                    pass
                                # st.rerun()
                                
                                if 'rule_count3' in st.session_state and st.session_state.rule_count3 > 0:
                                    
                                    if col2.button("Clear Filter",on_click = clear_filters3,key='ClearFilter2'):
                                        pass

                                if 'rule_count3' in st.session_state and st.session_state.rule_count3>0:
                                    if len(st.session_state.col_name_dict3) == len(st.session_state.col_operator_dict3) == len(st.session_state.col_value_dict3):
                                        if st.button("Apply Filter",on_click=apply_filter3,key='ApplyFilter2') or 'apply_filter3' in st.session_state:

                                            try:    
                                                df = filter_dataframe(df,st.session_state.col_name_dict3.values(),st.session_state.col_operator_dict3.values(),st.session_state.col_value_dict3.values())
                                            except:
                                                st.error("Please select correct filter condition")
                                            else:   
                                               
                                                st.write(f"Number of Source records : {df.shape[0]} ")
                                                st.session_state.cross_column_df2 = df

        df2 = st.session_state.cross_column_df2 if 'cross_column_df2' in st.session_state else df

    if df1 is not None and df2 is not None:
               
            st.header('')

            st.divider()
            st.subheader('Step2: Select one or more rules from the sidebar menu')

            resultantDF = pd.DataFrame()
            publishPrimaryData = pd.DataFrame()
            PublishSummary = pd.DataFrame()
            st.sidebar.subheader("Menu")
            list_of_checks = ['Lookup Column Compare', 'Lookup Substring Check', 'Join Key Column Compare']
          
            if 'crossTableRule' in st.session_state:
                
                for key, value in st.session_state.crossTableRule.items():
                    validationRule = st.sidebar.selectbox(label="Select rule", options=list_of_checks, index=None, key='rule' + str(key),on_change=cancel_collibra_integration)
                
                    if validationRule:
                        if 'Lookup Column Compare' in validationRule:
                            st.sidebar.caption('Lookup Column Compare')
                            column1 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol1' + str(key))
                            operator = st.sidebar.selectbox('Operation', ['matching', 'non-matching'], index = None, placeholder='Select operator', key='crossTableColumnCompareCheckOp' + str(key))
                            column2 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol2' + str(key))
                            try:
                                if column1 and operator and column2:
                                    st.subheader("Lookup Column Compare Validation Results - " + column1 + ', ' + column2)
                                    resultDF, publishDF, publishDF2, publishPrimary = crossTableColumnCompare(file_name1, df1, df2[[column2]], column1, column2, operator)
                                    resultantDF = resultantDF._append(resultDF)
                                    colss=resultantDF.columns
                                    rows=resultDF.shape[0]
                                    global resultDF2
                                    global Composite_Condition
                                    resultDF2=pd.DataFrame(columns=colss)
                                    publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                    PublishSummary= PublishSummary._append(publishDF2)
                                    ShowDFAndChart(resultDF)
                                    st.divider()
                            except Exception as e:
                                 st.error(e)
                            Composite_Condition=st.sidebar.selectbox("Select Optional Logical Operator",['','AND','OR'],on_change=cancel_collibra_integration)
                            
                            
                            if Composite_Condition == 'AND':
            
                                column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key)) #first column selected after choosing a composite rule(and/or)
                                operator1 = st.sidebar.selectbox('Operation', ['matching', 'non-matching'], index = None, placeholder='Select operator', key='crossTableColumnCompareCheckOpp' + str(key))
                                column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key)) #second column selected after choosing a composite rule(and/or)
                               
                                try:
                                    if column3 and operator1 and column4:
                                        st.subheader("Lookup Column Compare Validation Results - " + column3 + ', ' + column4)
                                        resultDF1, publishDF, publishDF2, publishPrimary = crossTableColumnCompare(file_name2, df1, df2[[column4]], column3, column4, operator1)
                                        resultantDF = resultantDF._append(resultDF1)
                                        resultDF2['TableName']=resultDF['TableName']
                                        resultDF2['ColumnName']="{},{} | {},{}".format(column1,column2,column3,column4)
                                        resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                        resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                        
                                        resultDF2['Execution_time']=resultDF['Execution_time']
                                        for i in range(0,rows):
                                            resultDF2.iloc[i,4]=funcAnd(resultDF.iloc[i,4],resultDF1.iloc[i,4]) 
                                        
                                        publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                        PublishSummary= PublishSummary._append(publishDF2)
                                        ShowDFAndChart(resultDF1)
                                        
                                        st.subheader(operator+ ' ' + column1 +' from '+ resultDF2.iloc[1,0]+' with ' + column2+' from '+file_name2)
                                        st.subheader('-AND-')
                                        st.subheader(operator1+ ' ' + column3+' from '+resultDF2.iloc[1,0]+' with ' + column4+' from '+file_name2  )
                                        ShowDFAndChart(resultDF2)
                                        st.divider()
                                except Exception as e:
                                    st.error(e)
                            elif Composite_Condition == 'OR':
                                    column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key))
                                    operator2 = st.sidebar.selectbox('Operation', ['matching', 'non-matching'], index = None, placeholder='Select operator', key='crossTableColumnCompareCheckOpp' + str(key))
                                    column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key))
                                    try:
                                        if column3 and operator2 and column4:
                                            st.subheader("Lookup Column Compare Validation Results - " + column3 + ', ' + column4)
                                            resultDF1, publishDF, publishDF2, publishPrimary = crossTableColumnCompare(file_name1, df1, df2[[column4]], column3, column4, operator2)
                                            resultantDF = resultantDF._append(resultDF1)
                                            resultDF2['TableName']=resultDF['TableName']
                                            resultDF2['ColumnName']=resultDF['ColumnName'].astype(str) + ' | ' + resultDF1['ColumnName'].astype(str)
                                            resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                            resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                         
                                            resultDF2['Execution_time']=resultDF['Execution_time']
                                            for i in range(0,rows):
                                                resultDF2.iloc[i,4]=funcOr(resultDF.iloc[i,4],resultDF1.iloc[i,4]) 
                                            
                                            publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                            PublishSummary= PublishSummary._append(publishDF2)
                                            ShowDFAndChart(resultDF1)
                                            st.subheader(operator+ ' ' + column1 +' from '+ resultDF2.iloc[1,0]+' with ' + column2+' from '+file_name2)
                                            st.subheader('-OR-')
                                            st.subheader(operator2+ ' ' + column3+' from '+resultDF2.iloc[1,0]+' with ' + column4+' from '+file_name2  )
                                            ShowDFAndChart(resultDF2)
                                            st.divider()
                                    except Exception as e:
                                        st.error(e)
                            else:
                                    column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key))
                                    operator2 = st.sidebar.selectbox('Operation', ['matching', 'non-matching'], index = None, placeholder='Select operator', key='crossTableColumnCompareCheckOpp' + str(key))
                                    column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key))
                                    try:
                                        if column3 and operator2 and column4:
                                            st.subheader("Lookup Column Compare Validation Results - " + column3 + ', ' + column4)
                                            resultDF1, publishDF, publishDF2, publishPrimary = crossTableColumnCompare(file_name1, df1, df2[[column4]], column3, column4, operator2)
                                            resultantDF = resultantDF._append(resultDF1)
                                            resultDF2['TableName']=resultDF['TableName']
                                            resultDF2['ColumnName']=resultDF['ColumnName'].astype(str) + ' | ' + resultDF1['ColumnName'].astype(str)
                                            resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                            resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                         
                                            resultDF2['Execution_time']=resultDF['Execution_time']
                                            publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                            PublishSummary= PublishSummary._append(publishDF2)
                                            ShowDFAndChart(resultDF1)
                                    except Exception as e:
                                        st.error(e)

                        # Code left her for future scope
                        # if 'Lookup Substring Check' in validationRule:
                        #         st.sidebar.caption('Lookup Substring Check')
                        #         column1 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='ctSubCol1' + str(key))
                        #         column2 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='ctSubCol2' + str(key))
                        #         try:
                        #             if column1 and column2:
                        #                 st.subheader("Lookup Substring Check Validation Results - " + column1 + ', ' + column2)
                        #                 resultDF, publishDF, publishDF2, publishPrimary = crossTableSubstringCheck(file_name1, df1, df2[[column2]], column1, column2)
                        #                 resultantDF = resultantDF._append(resultDF)
                        #                 publishPrimaryData = publishPrimaryData._append(publishPrimary)
                        #                 PublishSummary= PublishSummary._append(publishDF2)
                        #                 ShowDFAndChart(resultDF)
                        #                 st.divider()
                        #         except Exception as e:
                        #              st.error(e)
                        if 'Lookup Substring Check' in validationRule:
                            st.sidebar.caption('Lookup Substring Check')
                            column1 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol1' + str(key))

                            column2 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol2' + str(key))
                            try:
                                if column1 and column2:
                                    st.subheader("Lookup Substring Check Validation Results - " + column1 + ', ' + column2)
                                    resultDF, publishDF, publishDF2, publishPrimary = crossTableSubstringCheck(file_name1, df1, df2[[column2]], column1, column2)
                                    resultantDF = resultantDF._append(resultDF)
                                    colss=resultantDF.columns
                                    rows=resultDF.shape[0]
                        
                                    
                                    resultDF2=pd.DataFrame(columns=colss)
                                    publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                    PublishSummary= PublishSummary._append(publishDF2)
                                    ShowDFAndChart(resultDF)
                                    st.divider()
                            except Exception as e:
                                 st.error(e)
                            Composite_Condition=st.sidebar.selectbox("Select Optional Logical Operator",['','AND','OR'],on_change=cancel_collibra_integration)
                            
                            
                            if Composite_Condition == 'AND':
            
                                column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key)) #first column selected after choosing a composite rule(and/or)
                            
                                column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key)) #second column selected after choosing a composite rule(and/or)
                               
                                try:
                                    if column3 and column4:
                                        st.subheader("Lookup Substring Check Validation Results - " + column3 + ', ' + column4)
                                        resultDF1, publishDF, publishDF2, publishPrimary = crossTableSubstringCheck(file_name2, df1, df2[[column4]], column3, column4)
                                        resultantDF = resultantDF._append(resultDF1)
                                        resultDF2['TableName']=resultDF['TableName']
                                        resultDF2['ColumnName']="{},{} | {},{}".format(column1,column2,column3,column4)
                                        resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                        resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                        
                                        resultDF2['Execution_time']=resultDF['Execution_time']
                                        for i in range(0,rows):
                                            resultDF2.iloc[i,4]=funcAnd(resultDF.iloc[i,4],resultDF1.iloc[i,4]) 
                                        
                                        publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                        PublishSummary= PublishSummary._append(publishDF2)
                                        ShowDFAndChart(resultDF1)
                                        
                                        st.subheader(column1 +' from '+ resultDF2.iloc[1,0]+' with ' + column2+' from '+file_name2)
                                        st.subheader('-AND-')
                                        st.subheader(column3+' from '+resultDF2.iloc[1,0]+' with ' + column4+' from '+file_name2  )
                                        ShowDFAndChart(resultDF2)
                                        st.divider()
                                except Exception as e:
                                    st.error(e)
                            elif Composite_Condition == "OR":
                                    column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key))
                                    
                                    column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key))
                                    try:
                                        if column3 and column4:
                                            st.subheader("Lookup Substring Check Validation Results - " + column3 + ', ' + column4)
                                            resultDF1, publishDF, publishDF2, publishPrimary = crossTableSubstringCheck(file_name1, df1, df2[[column4]], column3, column4)
                                            resultantDF = resultantDF._append(resultDF1)
                                            resultDF2['TableName']=resultDF['TableName']
                                            resultDF2['ColumnName']=resultDF['ColumnName'].astype(str) + ' | ' + resultDF1['ColumnName'].astype(str)
                                            resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                            resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                         
                                            resultDF2['Execution_time']=resultDF['Execution_time']
                                            for i in range(0,rows):
                                                resultDF2.iloc[i,4]=funcOr(resultDF.iloc[i,4],resultDF1.iloc[i,4]) 
                                            
                                            publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                            PublishSummary= PublishSummary._append(publishDF2)
                                            ShowDFAndChart(resultDF1)
                                            st.subheader(column1 +' from '+ resultDF2.iloc[1,0]+' with ' + column2+' from '+file_name2)
                                            st.subheader('-OR-')
                                            st.subheader(column3+' from '+resultDF2.iloc[1,0]+' with ' + column4+' from '+file_name2  )
                                            ShowDFAndChart(resultDF2)
                                            st.divider()
                                    except Exception as e:
                                        st.error(e) 
                            else:
                                    column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key))
                                    
                                    column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key))
                                    try:
                                        if column3 and column4:
                                            st.subheader("Lookup Substring Check Validation Results - " + column3 + ', ' + column4)
                                            resultDF1, publishDF, publishDF2, publishPrimary = crossTableSubstringCheck(file_name1, df1, df2[[column4]], column3, column4)
                                            resultantDF = resultantDF._append(resultDF1)
                                            resultDF2['TableName']=resultDF['TableName']
                                            resultDF2['ColumnName']=resultDF['ColumnName'].astype(str) + ' | ' + resultDF1['ColumnName'].astype(str)
                                            resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                            resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                         
                                            resultDF2['Execution_time']=resultDF['Execution_time']
                                            
                                            
                                            publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                            PublishSummary= PublishSummary._append(publishDF2)
                                            ShowDFAndChart(resultDF1)
                                            st.divider()
                                    except Exception as e:
                                        st.error(e)
     

                        if 'Join Key Column Compare' in validationRule:
                            st.sidebar.caption('Join Key Column Compare')
                            key1 = st.sidebar.selectbox('Select key 1', df1.columns, index=None, key='crossTableKey1' + str(key))
                            column1 = st.sidebar.selectbox('Select column 1', df1.columns, index=None, key='ctCol1' + str(key))
                            operator = st.sidebar.selectbox('Operation', ['matching', 'non-matching','is substring of', 'less than', 'less than or equal to', 'greater than', 'greater than or equal to'], index = None, placeholder='Select operator', key='crossTableColumnCompareJoinOp' + str(key))
                            key2 = st.sidebar.selectbox('Select key 2', df2.columns, index=None, key='crossTableKey2' + str(key))
                            column2 = st.sidebar.selectbox('Select column 2', df2.columns, index=None, key='ctCol2' + str(key))
                            try:
                                if column1 and operator and column2 and key1 and key2:
                                    st.subheader("Lookup Column Compare Validation Results - " + column1 + ', ' + column2)
                                    resultDF, publishDF, publishDF2, publishPrimary = crossTableJoinKeyColumnCompare(file_name1, df1, df2[[key2, column2]], column1, column2, key1, key2, operator)
                                    resultantDF = resultantDF._append(resultDF)
                                    colss=resultantDF.columns
                                    rows=resultDF.shape[0]
                
                                    resultDF2=pd.DataFrame(columns=colss)
                                    publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                    PublishSummary= PublishSummary._append(publishDF2)
                                    ShowDFAndChart(resultDF)
                                    st.divider()
                            except Exception as e:
                                 st.error(e)
                            Composite_Condition=st.sidebar.selectbox("Select Optional Logical Operator",['','AND','OR'],on_change=cancel_collibra_integration)
                            
                            
                            if Composite_Condition == 'AND':
                                key3 = st.sidebar.selectbox('Select key 1', df1.columns, index=None, key='crossTableKey11' + str(key))
                                column3 = st.sidebar.selectbox('Select column 1', df1.columns, index=None, key='ctCol11' + str(key))
                                operator1 = st.sidebar.selectbox('Operation', ['matching', 'non-matching','is substring of', 'less than', 'less than or equal to', 'greater than', 'greater than or equal to'], index = None, placeholder='Select operator', key='crossTableColumnCompareJoinOp12' + str(key))
                                key4 = st.sidebar.selectbox('Select key 2', df2.columns, index=None, key='crossTableKey21' + str(key))
                                column4 = st.sidebar.selectbox('Select column 2', df2.columns, index=None, key='ctCol23' + str(key))
                                
                               
                                try:
                                    if column3 and operator1 and column4 and key3 and key4:
                                        st.subheader("Lookup Column Compare Validation Results - " + column3 + ', ' + column4)
                                        resultDF1, publishDF, publishDF2, publishPrimary = crossTableJoinKeyColumnCompare(file_name2, df1, df2[[key4, column4]], column3, column4,key3,key4, operator1)
                                        resultantDF = resultantDF._append(resultDF1)
                                        resultDF2['TableName']=resultDF['TableName']
                                        resultDF2['ColumnName']="{},{} | {},{}".format(column1,column2,column3,column4)
                                        resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                        resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                        
                                        resultDF2['Execution_time']=resultDF['Execution_time']
                                        for i in range(0,rows):
                                            resultDF2.iloc[i,4]=funcAnd(resultDF.iloc[i,4],resultDF1.iloc[i,4]) 
                                        
                                        publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                        PublishSummary= PublishSummary._append(publishDF2)
                                        ShowDFAndChart(resultDF1)
                                        
                                        st.subheader(operator+ ' ' + column1 +' from '+ resultDF2.iloc[1,0]+' with ' + column2+' from '+file_name2)
                                        st.subheader('-AND-')
                                        st.subheader(operator1+ ' ' + column3+' from '+resultDF2.iloc[1,0]+' with ' + column4+' from '+file_name2  )
                                        ShowDFAndChart(resultDF2)
                                        st.divider()
                                except Exception as e:
                                    st.error(e)
                            elif Composite_Condition == "OR":
                                    key3 = st.sidebar.selectbox('Select key 1', df1.columns, index=None, key='crossTableKey3' + str(key))
                                    column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key))
                                    operator2 = st.sidebar.selectbox('Operation', ['matching', 'non-matching','is substring of', 'less than', 'less than or equal to', 'greater than', 'greater than or equal to'], index = None, placeholder='Select operator', key='crossTableColumnCompareJoinOp1' + str(key))
                                    key4 = st.sidebar.selectbox('Select key 2', df2.columns, index=None, key='crossTableKey4' + str(key))
                                    column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key))
                                    try:
                                        if column3 and operator2 and column4 and key3 and key4:
                                            st.subheader("Lookup Column Compare Validation Results - " + column3 + ', ' + column4)
                                            resultDF1, publishDF, publishDF2, publishPrimary = crossTableJoinKeyColumnCompare(file_name2, df1, df2[[key4, column4]], column3, column4,key3,key4, operator2)
                                            resultantDF = resultantDF._append(resultDF1)
                                            resultDF2['TableName']=resultDF['TableName']
                                            resultDF2['ColumnName']=resultDF['ColumnName'].astype(str) + ' | ' + resultDF1['ColumnName'].astype(str)
                                            resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                            resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                         
                                            resultDF2['Execution_time']=resultDF['Execution_time']
                                            for i in range(0,rows):
                                                resultDF2.iloc[i,4]=funcOr(resultDF.iloc[i,4],resultDF1.iloc[i,4]) 
                                            
                                            publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                            PublishSummary= PublishSummary._append(publishDF2)
                                            ShowDFAndChart(resultDF1)
                                            st.subheader(operator+ ' ' + column1 +' from '+ resultDF2.iloc[1,0]+' with ' + column2+' from '+file_name2)
                                            st.subheader('-OR-')
                                            st.subheader(operator2+ ' ' + column3+' from '+resultDF2.iloc[1,0]+' with ' + column4+' from '+file_name2  )
                                            ShowDFAndChart(resultDF2)
                                            st.divider()
                                    except Exception as e:
                                        st.error(e) 
                            else:
                                    key3 = st.sidebar.selectbox('Select key 1', df1.columns, index=None, key='crossTableKey3' + str(key))
                                    column3 = st.sidebar.selectbox('Select column', df1.columns, index=None, key='crossTableCol11' + str(key))
                                    operator2 = st.sidebar.selectbox('Operation', ['matching', 'non-matching','is substring of', 'less than', 'less than or equal to', 'greater than', 'greater than or equal to'], index = None, placeholder='Select operator', key='crossTableColumnCompareJoinOp1' + str(key))
                                    key4 = st.sidebar.selectbox('Select key 2', df2.columns, index=None, key='crossTableKey4' + str(key))
                                    column4 = st.sidebar.selectbox('Select reference column', df2.columns, index=None, key='crossTableCol22' + str(key))
                                    try:
                                        if column3 and operator2 and column4 and key3 and key4:
                                            st.subheader("Lookup Column Compare Validation Results - " + column3 + ', ' + column4)
                                            resultDF1, publishDF, publishDF2, publishPrimary = crossTableJoinKeyColumnCompare(file_name2, df1, df2[[key4, column4]], column3, column4,key3,key4, operator2)
                                            resultantDF = resultantDF._append(resultDF1)
                                            resultDF2['TableName']=resultDF['TableName']
                                            resultDF2['ColumnName']=resultDF['ColumnName'].astype(str) + ' | ' + resultDF1['ColumnName'].astype(str)
                                            resultDF2['RuleID']=resultDF['RuleID'].astype(str) 
                                            resultDF2['Input']=resultDF['Input'].astype(str) + ' | ' + resultDF1['Input'].astype(str)
                                         
                                            resultDF2['Execution_time']=resultDF['Execution_time']
                                      
                                            
                                            publishPrimaryData = publishPrimaryData._append(publishPrimary)
                                            PublishSummary= PublishSummary._append(publishDF2)
                                            ShowDFAndChart(resultDF1)
                                            st.divider()
                                    except Exception as e:
                                        st.error(e)
                                                                  
                            
                    st.sidebar.divider()
            
            if st.sidebar.button('Add rule', on_click=session_setting_rule):
                new_uuid = uuid.uuid4()
                st.session_state.crossTableRule[new_uuid] = "Rule"
                st.rerun()
                
            try:
                if Composite_Condition == '' :
                    if resultantDF.empty is False:
                        col1, col2, col3, col4 = st.columns(4)
                        col1.download_button(label='Download Results', data= resultantDF.to_csv(index=False).encode('utf-8'), file_name='Business_Rule_Profile.csv', key='downloadData', mime='text/csv', on_click=session_setting())
                        if col4.button("Publish Cross Table Summary Results to Database", key='exportsummary',use_container_width=True):
                            

                            if len(st.session_state.source_system.strip())!=0:
                                PublishSummary['SourceSystem'] = st.session_state.source_system
                                # st.dataframe(PublishSummary)

                                try:
                                    write_to_sql_trend(PublishSummary, 'IDATAPROFILE_BIZ_RULE_PROF_SUMMARY')                   
                                except Exception as e:
                                    st.error(f"Current Table have no access, Change your table. Error - {e}")
                                else:
                                    
                                    modal = Modal(key="CTPSummary", title='', max_width=1000)
                                    with modal.container():
                                         st.success("Successfully Published Cross Table Summary Results to Database")

                            else:
                                
                                modal = Modal(key="CTPSummarySource", title='', max_width=1000)
                                with modal.container():
                                    st.warning("Please Select Source System in Order to Publish Cross Table Summary Results")

                            
                        
                        if col4.button(label='Publish Cross Table Fail Records to Database', key='exportToSQL4CR',use_container_width=True):
                            #publishing failed records.
                            try:
                                write_to_sql(publishPrimaryData[publishPrimaryData['RuleStatus']=='FAIL'],'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')
                            except Exception as e:
                                st.error(e)
                            else:
                                
                                modal = Modal(key="CTPSummaryFail", title='', max_width=1000)
                                with modal.container():
                                         st.success("Successfully Published Cross Table Failed Records to Database")

                        if col4.button(label='Publish Cross Table All Records to Database', key='exportToSQLPrimaryCR',disabled=True,use_container_width=True):
                            write_to_sql(publishPrimaryData, 'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')
                            #publishing all records.

                            
                        # Initialize ribbon state
                        if "ribbon_expanded" not in st.session_state:
                            st.session_state.ribbon_expanded = True
                        
                        if "ribbon_expanded" in st.session_state:
                            st.session_state.ribbon_expanded = False

                        toggle = st.toggle(
                        "Enable Collibra Integration",
                        key='collibra_integration',
                        value=st.session_state.ribbon_expanded
                        )

                        # Update ribbon state based on toggle
                        st.session_state.ribbon_expanded = toggle

                        # Proceed if ribbon is expanded
                        if st.session_state.ribbon_expanded:
                        #if col4.button('Publish Results for Collibra Integration',key='collibra_integration',on_click = publish_collibra_result_cross_table,disabled=False,use_container_width=True) or 'collibra_crosstable' in st.session_state:
                            columns = resultantDF.drop_duplicates(subset = ['ColumnName','RuleID'])[['ColumnName','RuleID']]
                            colibra_columns = ['SourceSystemName','TableName','ColumnName','Name','Full Name','DQ Dimension','Definition','Inclusion Scenario','Exception Scenario','Threshold','Loaded Rows','Rows Passed','Rows Failed','Result','Entity Load Date','Conformity Score','Non Conformity Score','Last Sync Date','SchemaName','DatabaseName','Passing Fraction']
                            colibra_data = pd.DataFrame(columns=colibra_columns)
                            i = 0
                            for index, row in columns.iterrows():
                                i+=1
                                df = resultantDF.query(f" ColumnName == '{row['ColumnName']}' and RuleID == '{row['RuleID']}' ")
                                central_dq_repo_df = pd.read_csv('IDATAPROFILER/static/CrossTableDQRepository.csv',encoding_errors= 'replace')
                                rule_id = df['RuleID'].unique()[0]
                                dq_rule = central_dq_repo_df.query(f" DQ_RuleId == '{rule_id}' ")['DQ_Rule'].values[0]
                                dq_rule_desc = central_dq_repo_df.query(f" DQ_RuleId == '{rule_id}' ")['DQ_Rule_Description'].values[0]
                                DQ_Dimension = central_dq_repo_df.query(f" DQ_RuleId == '{rule_id}' ")['DQ_Dimension'].values[0]
                                st.write(f"**ColumnName: {row['ColumnName']},       Rule: {dq_rule}**")
                                col1,col2 = st.columns(2)
                                source_system = col1.text_input(f"Enter Source System Name ",key=str(index)+str(i)+'source_system')
                                threshold = col2.number_input(f"Enter Threshold ",key=str(index)+str(i)+'threshold',min_value=0,max_value=100)
                                schema_name = col1.text_input(f"Enter Schema Name ",key=str(index)+str(i)+'schema_name')
                                database_name = col2.text_input(f"Enter Database Name ",key=str(index)+str(i)+'database_name')
                                inclusion_scenario = col1.text_input(f"Enter Inclusion Scenario ",key=str(index)+str(i)+'inclusion_scenario')
                                exclusion_scenario = col2.text_input(f"Enter Exclusion Scenario ",key=str(index)+str(i)+'exclusion_scenario')
                                st.markdown("""<hr style="height:2px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
                                loaded_rows = df.shape[0] 
                                rows_pass = df.query("RuleStatus == 'PASS' ").shape[0]
                                rows_fail = loaded_rows - rows_pass
                                column_name = df['ColumnName'].unique()[0]
                                
                                result = 'PASS' if round((rows_pass*100/loaded_rows),2) >= st.session_state[str(index)+str(i)+'threshold'] else 'FAIL'
                                conformity_score = round(rows_pass / loaded_rows,2)
                                non_conformity_score = 1 - conformity_score
                                
                                new_row = {
                                    'SourceSystemName': st.session_state[str(index)+str(i)+'source_system'] ,
                                    'TableName': file_name1 ,
                                    'ColumnName': column_name ,
                                    'Name':  dq_rule ,
                                    'Full Name': dq_rule_desc ,
                                    'DQ Dimension' : DQ_Dimension ,
                                    'Definition': dq_rule_desc ,
                                    'Inclusion Scenario': st.session_state[str(index)+str(i)+'inclusion_scenario']  ,
                                    'Exception Scenario': st.session_state[str(index)+str(i)+'exclusion_scenario'],
                                    'Threshold': round(st.session_state[str(index)+str(i)+'threshold']/100,2) ,
                                    'Loaded Rows': loaded_rows ,
                                    'Rows Passed': rows_pass ,
                                    'Rows Failed': rows_fail ,
                                    'Result': result ,
                                    'Entity Load Date': date.today() ,
                                    'Conformity Score':  conformity_score ,
                                    'Non Conformity Score': non_conformity_score ,
                                    'Last Sync Date': date.today(),
                                    'SchemaName': st.session_state[str(index)+str(i)+'schema_name'],
                                    'DatabaseName' : st.session_state[str(index)+str(i)+'database_name'],
                                    'Passing Fraction':round(conformity_score*100,2)
                                }
                    
                                
                                colibra_data = colibra_data._append(new_row,ignore_index=True)
                            col1,col2 = st.columns(2)
                            col2.button("Cancel",key='cancel_button',on_click=cancel_collibra_integration)  
                            with col1:
                                    missing_fields = not source_system or not schema_name or not database_name or not inclusion_scenario or not exclusion_scenario

                                    if missing_fields:
                                        st.error("All fields are required!")

                                    # Initialize success flag in session state if not already set
                                    if 'show_collibra_success' not in st.session_state:
                                        st.session_state.show_collibra_success = False
                                    if st.button("Publish For Collibra Integration",on_click = remove_collibra_state_cross_table, disabled = missing_fields) or 'publish_collibra_cross_table' in st.session_state:
                                        try:
                                        
                                            write_to_sql(colibra_data, 'IDATAPROFILE_CATALOG_DQ_METRICS')
                                        
                                        except Exception as e:
                                            st.error(e)
                                        else:
                                            st.session_state.show_collibra_success = True
                                        finally:
                                            del st.session_state.publish_collibra_cross_table
                                            if 'collibra_crosstable' in st.session_state:
                                                del st.session_state.collibra_crosstable
                                                st.rerun()
                            
                            if st.session_state.show_collibra_success:
                                    modal = Modal(key="BRPSource_system collectSummary2", title='', max_width=1000)
                                    with modal.container():
                                        st.success("Successfully Published Business Rule Summary Results to Database")
                                    st.session_state.show_collibra_success = False

                else:
                    if resultantDF.empty is False:
                        col1, col2, col3, col4 = st.columns(4)
                        col1.download_button(label='Download Results', data= resultDF2.to_csv(index=False).encode('utf-8'), file_name='Business_Rule_Profile.csv', key='downloadData', mime='text/csv', on_click=session_setting())
                        calculatedDF = pd.DataFrame()
                        calculatedDF['Input'] = resultDF2['Input']
                        df_1,df2,df3 = assignData(calculatedDF, resultDF2['ColumnName'].head(1).values[0], resultDF2['RuleID'].head(1).values[0], resultDF2['TableName'].head(1).values[0], resultDF2['Execution_time'].head(1).values[0], resultDF2['RuleStatus'])
                        Fail_dataframe = publishPrimaryDataDependentRule(df1,calculatedDF,resultDF2['ColumnName'], resultDF2['RuleID'], resultDF2['TableName'], resultDF2['Execution_time'], resultDF2['RuleStatus'])
                        
                        if col4.button("Publish Cross Table Summary Results to Database", key='exportsummary',use_container_width=True):
                            

                            if len(st.session_state.source_system.strip())!=0:
                                df3['SourceSystem'] = st.session_state.source_system
                                # st.dataframe(df3.head(3))

                                try:
                                    write_to_sql_trend(df3.head(3), 'IDATAPROFILE_BIZ_RULE_PROF_SUMMARY')                   
                                except Exception as e:
                                    st.error(f"Current Table have no access, Change your table. Error - {e}")
                                else:
                                    
                                    modal = Modal(key="CTPSummarySummary2", title='', max_width=1000)
                                    with modal.container():
                                        st.success("Successfully Published Cross Table Summary Results to Database")

                            else:
                                
                                modal = Modal(key="CTPSummarySummarysource2", title='', max_width=1000)
                                with modal.container():
                                        st.warning("Please Select Source System in Order to Publish Cross Table Summary Results.")

                            
                            
                        if col4.button(label='Publish Cross Table Fail Records to Database', key='exportToSQL4CR',use_container_width=True):
                            #publishing failed records.
                            try:
                                write_to_sql(Fail_dataframe[Fail_dataframe['RuleStatus']=='FAIL'],'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')
                            except Exception as e:
                                st.error(e)
                            else:
                                
                                modal = Modal(key="CTPSummarySummaryFail2", title='', max_width=1000)
                                with modal.container():
                                        st.success("Successfully Published Cross Table Failed Records to Database")
                        if col4.button(label='Publish Cross Table All Records to Database', key='exportToSQLPrimaryCR',disabled=True,use_container_width=True):
                            write_to_sql(Fail_dataframe, 'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')
                            #publishing all records.
       
            except Exception as e:
                st.write(e)                        
            st.header('')   
            st.header('')   
            scroll_up_button('connect-to-source-data')