import uuid
import streamlit as st
import pandas as pd
from ValidationRules.ListofCustomRules import *
from SourceData import *
from utility import scroll_up_button, write_to_sql, ShowDFAndChart, read_from_sql,write_to_sql_trend,convert_to_int_if_whole
from streamlit_modal import Modal
import matplotlib.pyplot as plt
import plotly.express as px
from Filter import *
from  datetime import date,datetime
from SessionStateVar import cancel_collibra_integration

# Global flag
CompFlag=True

def apply_filter1():
     """Set a flag in session state to apply filter 1."""
     if 'apply_filter1' not in st.session_state:
          st.session_state.apply_filter1=1

def filter_state1():
    """Activate filter 1 and remove apply flag if present."""
    st.session_state.filter1=1
    if 'apply_filter1' in st.session_state:
        del st.session_state.apply_filter1
        
def publish_collibra_result():
    """Set flag to publish results to Collibra."""
    if 'collibra' not in st.session_state:
        st.session_state.collibra = 1

    if 'publish_collibra' in st.session_state:
        del st.session_state.publish_collibra

def remove_collibra_state():
    """Set flag to remove Collibra publish state."""
    if 'publish_collibra' not in st.session_state:
        st.session_state.publish_collibra = 1

def increase_rule_count1():
    """
    Increase the rule count and initialize corresponding dictionaries in session state.
    """
    if 'rule_count1' in st.session_state:
        st.session_state.rule_count1+=1
    st.session_state.col_name_dict1['ColName'+str(st.session_state.rule_count1)] = None
    st.session_state.col_operator_dict1['ColOp'+str(st.session_state.rule_count1)] = None 
    st.session_state.col_value_dict1['ColValue'+str(st.session_state.rule_count1)] = None

    if 'apply_filter1' in st.session_state:
        del st.session_state.apply_filter1

def add_filter_conditions1():
    """
    Initialize filter condition dictionaries and rule count in session state if not already present.
    """
    if 'col_name_dict1' not in st.session_state:
        st.session_state.col_name_dict1 = {}

    if 'col_operator_dict1' not in st.session_state:
        st.session_state.col_operator_dict1 = {}

    if 'col_value_dict1' not in st.session_state:
        st.session_state.col_value_dict1 = {}

    if 'rule_count1' not in st.session_state:
        st.session_state.rule_count1 = 0
    
    if 'filter1' not in st.session_state:

        st.session_state.filter1 = 1

def session_setting():
     """
     Set session state variables for rule profiling and modal display.
     """
     st.session_state.ruleProfilingTable = 1
     st.session_state.ruleModal = 1
     
def add_composite_rule1(value): 
     """
     Add a composite rule to session state if applicable.
    
     Parameters:
     value (dict): Dictionary containing rule metadata.
     """
     cancel_collibra_integration()
     if value['RuleType'] == 'Composite Business Rules':
        
          
        if 'rule2' in st.session_state:
            pass 
        if 'rule2' not in st.session_state:
            st.session_state.rule2 = {}
        st.session_state.composite_rule2 = 1
        if len( st.session_state.rule2) <2:
            new_uuid = str(uuid.uuid4())
            st.session_state.rule2[new_uuid] = "Rule"
            new_uuid = str(uuid.uuid4())
            st.session_state.rule2[new_uuid] = "Rule"

def clear_particular_filter_brp(key1,key2,key3):
    """
    Clear a specific filter condition from session state.
    
    Parameters:
    key1 (str): Key for column name.
    key2 (str): Key for operator.
    key3 (str): Key for value.
    """
    if key1 in st.session_state.col_name_dict1:
        del st.session_state.col_name_dict1[key1] 
        del st.session_state.col_operator_dict1[key2] 
        del st.session_state.col_value_dict1[key3]
    
    if len(st.session_state.col_name_dict1) == 0:
        
        st.session_state.rule_count1 = 0

def session_setting_rule():
     """
     Initialize a new rule in session state.
     """
     if 'rule' not in st.session_state:
         st.session_state.rule = {}
     
     new_uuid = uuid.uuid4()
     st.session_state.rule[new_uuid] = "Rule"

def session_setting_rule_indepedent():
     """
     Initialize a new independent rule in session state.
     """
     if 'rule' not in st.session_state:
         st.session_state.rule = {}
     
     new_uuid = uuid.uuid4()
     st.session_state.rule[new_uuid] = "Rule"
     
def session_rule_add():
     """
     Add a rule to session state if not already present.
     """
     if 'addrule' not in st.session_state:
          st.session_state.rule={}         

def set_session_button():
     """
     Set a button flag in session state.
     """
     st.session_state.button = 0

def add_composite_rule():
     """
     Placeholder for adding a composite rule.
     """
     pass

def connect_to_source_data(source):
    """
    Display UI for selecting and connecting to a data source.
    
    Parameters:
    source (str): Identifier for the source context.
    
    Returns:
    tuple: DataFrame and file name from the selected source.
    """
    st.subheader('Connect to Source Data')
    ques = st.radio("",('Flat File','ADLS Blob','S3 Bucket','Azure SQL DB','Databricks DBFS'),on_change=reset_dataframe)  #tsv,psv,parquet

    if ques == 'Flat File':
        df,file_name = flat('RuleProfilingFlat',source)

    if ques == 'ADLS Blob':
        print("Entry")
        df,file_name=adl('RuleProfilingadls',source)
                
    if ques == 'S3 Bucket':
        print("S3 bucket")
        df,file_name = s3_buc('RuleProfilings3',source)
                
    if ques == 'Azure SQL DB':
        print("Sql DB")
        df,file_name = sdb('RuleProfilingsql',source)
     
    if ques == 'Databricks DBFS':
          df,file_name = databr('RuleProfilingdatabricks',source)
             
    if ques == 'Standard SQL Server':
          df,file_name = stsq('RuleProfilingstsql',source)
          
    if ques == 'API':
          df,file_name = apicall('RuleProfilingapicall',source)
          
    st.write('')  
    
    return df, file_name

def funcAnd(a,b):
    """
    Logical AND operation for rule evaluation.

    Parameters:
    a (str): First rule result ('PASS', 'FAIL', or 'NULL').
    b (str): Second rule result ('PASS', 'FAIL', or 'NULL').

    Returns:
    str: Result of logical AND operation.
    """
    if a=='PASS' and b=='PASS':
        return 'PASS'
    elif a == 'NULL' or b == 'NULL':
        return 'NULL'
    elif a == 'FAIL' or b == 'FAIL':
        return 'FAIL'
    
def funcOr(a,b):
    """
    Logical OR operation for rule evaluation.

    Parameters:
    a (str): First rule result ('PASS', 'FAIL', or 'NULL').
    b (str): Second rule result ('PASS', 'FAIL', or 'NULL').

    Returns:
    str: Result of logical OR operation.
    """
    if a=='PASS' or b=='PASS':
        return 'PASS'
    elif a == 'NULL' and b == 'NULL':
        return 'NULL'
    elif a == 'FAIL' and b == 'FAIL':
        return 'FAIL'

resdf3=pd.DataFrame()
expression = ''

def BusinessRuleProfiling():
    """
    Main function to perform Business Rule Profiling using Streamlit.
    Loads source data, processes numeric columns, and stores the result in session state.
    """
    st.title("Business Rule Profiling")

    if 'original_dataframe' not in st.session_state or 'src_select_widget_change_BRP' in st.session_state:
                df,file_name = connect_to_source_data("brp_source_data")
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
        if st.toggle("New Source",key='newbrpsource'):
            df = None
            df,file_name = connect_to_source_data('brp_source_data')
            
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
     
    if df is not None:
            
            st.subheader(f"**File Name: {file_name}**")
            st.write("**_Sample view of dataset_**")
            st.dataframe(df.head(5), hide_index=True)
            st.header('')
            st.write("**Select Primary Key Columns:**")
            st.session_state.primary_columns = st.multiselect('',options=df.columns,default=None,key = 'PrimarykeyRuleProfilingnew')
            st.text("")
            if 'primary_columns' in st.session_state and len(st.session_state.primary_columns) > 0:
               st.write(f"**Primary Key Columns: {st.session_state.primary_columns}**")
            else:
               st.write(f"**Primary Key is not selected**")
            # st.write("**Enter Source System Name:**")
            st.session_state.source_system = st.text_input("**Enter Source System Name:(Mandatory to Publish Business Rule Summary Results to Database)**",key='Sourcesystem_rule_profiling_new')
            ruleList, summaryStats = st.columns([1.5, 0.5])
            modal = Modal(key="ListOfRules", title='', max_width=1000)
            open_modal = ruleList.button(label='List of Supported Rules', key="listOfRules", on_click=session_setting())
            if open_modal:
                with modal.container():
                    central_dq_repo_df = pd.read_csv('IDATAPROFILER/static/CentralDQRepository.csv',encoding_errors= 'replace')
                    st.data_editor(central_dq_repo_df, disabled=set(central_dq_repo_df.columns) - set(['Category', 'Priority']), hide_index=True, key='tableOfListOfRules')

            summaryStatsModal = Modal(key="SummaryStats", title='', max_width=1000)

            # Ensure the session state key exists
            if 'ran_profiling_summary_statistics' not in st.session_state:
                st.session_state.ran_profiling_summary_statistics = False

            summary_stats = False

            if st.session_state.ran_profiling_summary_statistics:
                # Create the button and disable it if the condition is not met
                summary_stats = st.button(
                    'Link to Summary Statistics',
                    type='primary',
                    disabled=not st.session_state.ran_profiling_summary_statistics
                )

            if summary_stats:
                with summaryStatsModal.container():
                    summary_stats_df = read_from_sql('IDATAPROFILE_SUMMARY_STATISTICS', file_name)
                    if summary_stats_df is not None and summary_stats_df.empty is False: 
                        st.dataframe(summary_stats_df)
                    else:
                         st.markdown("<h3 style='text-align: center; color: grey;'>Table not Profiled</h3>", unsafe_allow_html=True)

            st.markdown(
                """
                <style>
                button[kind="primary"] {
                    background: none!important;
                    border: none;
                    padding: 0!important;
                    color: blue !important;
                    text-decoration: none;
                    cursor: pointer;
                    border: none;
                }
                button[kind="primary"]:hover {
                    color: red !important;
                }
                button[kind="primary"]:focus {
                    outline: none !important;
                    box-shadow: none !important;
                    color: red !important;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )

            if st.button("Include/Exclude Source Data",on_click=add_filter_conditions1) or 'filter1' in st.session_state:
                                if 'rule_count1' in st.session_state and st.session_state.rule_count1 > 0:
                                     for key1,key2,key3 in zip(st.session_state.col_name_dict1.keys(),st.session_state.col_operator_dict1.keys(),st.session_state.col_value_dict1.keys()):
                                         col1,col2,col3,col4=st.columns([1,1,1,0.5])
                                         col1.write("**Select Column**")
                                         column1=col1.selectbox("",options=df.columns,on_change = filter_state1,key = 'ColName'+str(key1))
                                         col2.write("**Select the operator**")
                                         operator = col2.selectbox("",options=['==','!=','<','<=','>','>=','Like','Not Like','In','Not In','Is None','Is Not None'],on_change = filter_state1,key = 'ColOp'+str(key2),index = None)
                                         col3.write("**Enter the compare value**")
                                         compare_value = col3.text_input("",on_change = filter_state1,key= 'ColValue'+str(key3))
                                         col4.write("")
                                         col4.write("")
                                         col4.write("")
                                         col4.write("")
                                         col4.button("❌",key= 'Clear_filter'+str(key1),on_click=clear_particular_filter_brp,args=(key1,key2,key3))
                                         st.session_state.col_name_dict1[key1] = st.session_state['ColName'+str(key1)]
                                         st.session_state.col_operator_dict1[key2] = st.session_state['ColOp'+str(key2)]
                                         st.session_state.col_value_dict1[key3] = st.session_state['ColValue'+str(key3)]
                                
                                col1,col2,col3 = st.columns([1.3,1,4])
                                if 'rule_count1' in st.session_state and col1.button("Add Filter condition",on_click=increase_rule_count1):
                                    pass
                                
                                
                                if 'rule_count1' in st.session_state and st.session_state.rule_count1 > 0:
                                    
                                    if col2.button("Clear All Filters",on_click = clear_filters1):
                                        pass

                                
                                # st.write(st.session_state.col_name_dict)
                                # st.write(st.session_state.col_operator_dict)
                                # st.write(st.session_state.col_value_dict)
                                if 'rule_count1' in st.session_state and st.session_state.rule_count1 > 0:

                                          if len(st.session_state.col_name_dict1) == len(st.session_state.col_operator_dict1) == len(st.session_state.col_value_dict1):
                                              
          
                                              if st.button("Apply Filter",on_click=apply_filter1) or 'apply_filter1' in st.session_state:
          
                                                  
                                                  try:
                                                      df = filter_dataframe(df,st.session_state.col_name_dict1.values(),st.session_state.col_operator_dict1.values(),st.session_state.col_value_dict1.values())
                                                      
                                                  except:
                                                      st.error("Please select correct filter condition")
          
                                                  else:
                                                      
                                                      st.write(f"Number of Source records considered for Profiling : {df.shape[0]} ")
                                                      st.session_state.business_rule_data = df

            st.divider()
            st.subheader('Step2: Select one or more rules from the sidebar menu')

            resultantDF = pd.DataFrame()
            finalPublishDF = pd.DataFrame()
            finalPublishDF2 = pd.DataFrame()
            primarySummaryData = pd.DataFrame()

            Rule_type = st.sidebar.selectbox("Rule Type",['Standalone Business Rules','Composite Business Rules'],key = 'RuleType',on_change=add_composite_rule1,args=(st.session_state,))

            st.sidebar.subheader("Menu")

            if Rule_type == 'Composite Business Rules':
                        global expression
                        global resdf3
                        resdf3=pd.DataFrame()
                        expression = ''
                    

                        list_of_checks = ['Unallowable Keywords/ Characters', 'Null/ Blank', 'Value check', 'Length check', 'Email Validity', 'Duplicate using composite key', 'Leading or Trailing characters', 'Cross Column Compare', 'Substring Check','Date Check Weekend','Date Check Future Date','Date Check Weekday','Date Check Non Future Date', 'Allowable Keywords','Pattern Check','Date Validity Check','Cardinality Check']
                    
                        if 'rule2' in st.session_state:

                            for key, value in enumerate(st.session_state.rule2.items()):   
                                if len(st.session_state.rule2) > 1 and key >= 1 :
                                          condition = st.sidebar.selectbox("Select Logical Operator",['','AND','OR'],key = key)
                                          expression_condition = st.session_state[key]
                                          expression += ' ' + expression_condition

                                validationRule = st.sidebar.selectbox(label="Select rule", options=list_of_checks, index=None, key='rule' + str(key))
                                try:
                                   expression +=  ' ' + validationRule
                                except:
                                   pass
                                try:
                                    if validationRule:
                                        if 'Unallowable Keywords/ Characters' in validationRule:
                                            st.sidebar.caption('Unallowable Keywords/ Characters')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key='unallowableCol' + str(key))
                                            keywords = st.sidebar.text_input("Enter unallowable keywords/ characters", placeholder = 'Enter only comma separated keywords', key='unallowableKeyword' + str(key))
                                            caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='unallowableColCase' + str(key))

                                            
                                            
                                            try:
                                                if column and len(keywords) != 0:
                                                    expression += " " + column + " " + '{' + keywords +'}'
                                                    # st.subheader('Unallowable Keywords/ Characters Validation Results - ' + column)
                                                    resultDF, publishDF,publishDF2,primarySummary = unallowableKeywords(file_name, df, column, keywords, caseSensitivity)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    # ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)
                                        
                                        if 'Allowable Keywords' in validationRule:
                                            st.sidebar.caption('Allowable Keywords')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key='allowableCol' + str(key))
                                            keywords = st.sidebar.text_input("Enter allowable keywords", placeholder = 'Enter only comma separated keywords', key='allowableKeyword' + str(key))
                                            caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='allowableColCase' + str(key))

                                            

                                            try:
                                                if column and len(keywords) != 0:
                                                    expression += " " + column + " " + '{' + keywords +'}'
                                                    # st.subheader('Allowable Keywords Validation Results - ' + column)
                                                    resultDF, publishDF,publishDF2,primarySummary = allowableKeywords(file_name, df, column, keywords, caseSensitivity)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    # ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)
                                        
                                        if 'Null/ Blank' in validationRule:
                                            st.sidebar.caption('Null/ Blanks')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key='nullBlankCol' + str(key))
                                            try:
                                                if column:
                                                    expression += " " + column
                                                    # st.subheader("Null/ Blanks Validation Results - " + column)
                                                    NullRuleType=st.sidebar.selectbox("Select Null Rule Type",['Should be NULL','Should not be NULL'],key='nullBlankColRulType' + str(key))
                                                    if NullRuleType == 'Should be NULL':
                                                            resultDF, publishDF,publishDF2,primarySummary = shouldbenullsBlanks(file_name, df, column)
                                                            resultantDF = resultantDF._append(resultDF)
                                                            primarySummaryData = primarySummaryData._append(primarySummary)
                                                            # ShowDFAndChart(resultDF)
                                                            finalPublishDF = finalPublishDF._append(publishDF)
                                                            finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                            st.divider()
                                                    else:
                                                            resultDF, publishDF,publishDF2,primarySummary = nullsBlanks(file_name, df, column)
                                                            resultantDF = resultantDF._append(resultDF)
                                                            primarySummaryData = primarySummaryData._append(primarySummary)
                                                            # ShowDFAndChart(resultDF)
                                                            finalPublishDF = finalPublishDF._append(publishDF)
                                                            finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                            st.divider()
                                            except Exception as e:
                                                st.error(e)

                                        if  'Value check' in validationRule:
                                                st.sidebar.caption('Value check')
                                                column = st.sidebar.selectbox('Select column', df.columns, index=None, key= 'valueCol' + str(key))
                                                operator = st.sidebar.selectbox('Operator',['should be', 'should not be', 'should be greater than', 'should be greater than or equal to', 'should be less than', 'should be less than or equal to', 'should be between','is in'], index = None, placeholder='Select operator', key='valueCheckOperator' + str(key))
                                                
                                                
                                                try:
                                                    if operator == 'should be between':
                                                        value = st.sidebar.text_input('Value1', placeholder = 'Enter value', key='value1CheckInput' + str(key))
                                                        value2 = st.sidebar.text_input('Value2', placeholder = 'Enter value', key='value2CheckInput' + str(key))

                                                        expression += ' ' + str(value) + ' and '+str(value2)
                                                    else:
                                                        value = st.sidebar.text_input('Value', placeholder = 'Enter value', key='valueCheckInput' + str(key))
                                                        value2 = True
                                                        expression += ' ' + str(value) 

                                                    caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='ValueCheckColCase' + str(key))

                                                    if column and operator and value and value2:
                                                        expression += ' ' + column + ' ' + operator
                                                        # st.subheader("Value check Validation Results - " + column)

                                                        if operator == 'should be between':
                                                            resultDF, publishDF,publishDF2,primarySummary = valueCheck(file_name, df, column, operator, value, value2, caseSensitivity)
                                                        else:
                                                            resultDF, publishDF,publishDF2,primarySummary = valueCheck(file_name, df, column, operator, value, '0', caseSensitivity)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Length check' in validationRule:
                                                st.sidebar.caption('Length check')
                                                column = st.sidebar.selectbox('Select column', df.columns, index=None, key='lenCol' + str(key))
                                                operator = st.sidebar.selectbox('Operator',['should be', 'should not be', 'should be greater than', 'should be greater than or equal to', 'should be less than', 'should be less than or equal to', 'should be between'], index = None, placeholder='Select operator', key='lengthCheckOperator' + str(key))
                                                

                                                try:
                                                    if operator == 'should be between':
                                                        length = st.sidebar.number_input('Minimum Length', min_value=0, value=0, step=1, key='minLength' + str(key))
                                                        length2 = st.sidebar.number_input('Maximum Length', min_value=0, value=0, step=1, key='maxLength' + str(key))
                                                        expression += ' ' + str(length) + ' and '+str(length2)

                                                    else:
                                                        length = st.sidebar.number_input('Length', min_value=0, value=0, step=1, key='singleLength' + str(key))
                                                        expression += ' ' + str(length)
    
                                                    if column and operator:
                                                        expression += ' ' + column + ' ' + operator
                                                        # st.subheader("Length check Validation Results - " + column)
                                                        
                                                        if operator == 'should be between':
                                                            resultDF, publishDF,publishDF2,primarySummary = lengthCheck(file_name, df, column, operator, length, length2)
                                                        else:
                                                            resultDF, publishDF,publishDF2,primarySummary = lengthCheck(file_name, df, column, operator, length)
                                                        
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Email Validity' in validationRule:
                                                st.sidebar.caption('Email Validity')
                                                column = st.sidebar.selectbox('Select column', df.columns, index=None, key='emailCol' + str(key))
                                                
                                                try:
                                                    if column:
                                                        expression += ' '+ column + ' '
                                                        # st.subheader("Email Validity Validation Results - " + column)
                                                        resultDF, publishDF,publishDF2,primarySummary = validateEmail(file_name, df, column)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Duplicate using composite key' in validationRule:
                                                st.sidebar.caption('Duplicate using composite key')
                                                columns = st.sidebar.multiselect('Select columns', df.columns, key='duplicateCol' + str(key))
                                                
                                                try:
                                                    if columns:
                                                        expression += ' ' + ' { ' + columns +' } '
                                                        resultDF, publishDF,publishDF2,primarySummary = compositeKeyDuplicates(file_name, df, columns)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)
                                        

                                        if 'Leading or Trailing characters' in validationRule:
                                                st.sidebar.caption('Leading or Trailing characters')
                                                column = st.sidebar.selectbox('Select column', df.columns, index=None, key='leadTrailCol' + str(key))
                                                option = st.sidebar.selectbox('Option', ['starts with', 'ends with', 'starts and ends with', 'starts or ends with'], index = None, placeholder='Select an option', key='leadTrailChars' + str(key))
                                                character = st.sidebar.text_input('Character', placeholder = 'Enter char to check for', key='leadTrailChar' + str(key))
                                                caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='leadTrailColCase' + str(key))
                                                
                                                try:
                                                    if column and option and character:
                                                        expression += ' '+ column+ ' ' + option + '\'' + character + '\''+ ' '
                                                        # st.subheader('Leading or Trailing characters Validation Results - ' + column)
                                                        resultDF, publishDF,publishDF2,primarySummary = leadTrailingCharacters(file_name, df, column, option, character, caseSensitivity)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Cross Column Compare' in validationRule:
                                                st.sidebar.caption('Cross Column Compare')
                                                column1 = st.sidebar.selectbox('Select column 1', df.columns, index=None, key='crossCol1' + str(key))
                                                operator = st.sidebar.selectbox('Operation', ['matching', 'non-matching','is substring of', 'less than', 'less than or equal to', 'greater than', 'greater than or equal to'], index = None, placeholder='Select operator', key='columnCompareCheckOp' + str(key))
                                                column2 = st.sidebar.selectbox('Select column 2', df.columns, index=None, key='crossCol2' + str(key))
                                                caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='ValueCheckColCase' + str(key))
                                                if column1 and operator and column2:
                                                     expression += ' '+ column1 + ' '+ operator + ' ' + column2
                                                try:
                                                    if column1 and operator and column2:
                                                        
                                                        # st.subheader("Cross Column Compare Validation Results - [" + column1 + ', ' + column2 + ']')
                                                        resultDF, publishDF,publishDF2,primarySummary = crossColumnCompare(file_name, df.copy(), column1, column2, operator, caseSensitivity)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)                             
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                        
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Substring Check' in validationRule:
                                                st.sidebar.caption('Substring Check')
                                                column = st.sidebar.selectbox('Select column', df.columns, index=None, key=7)
                                                subString = st.sidebar.text_input('Substring to be searched for', placeholder = 'Enter Substring')
                                                startPos = st.sidebar.number_input('Start position of substring', min_value=1, value='min',placeholder='Enter Start Position')
                                                caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='SubstringColCase' + str(key))
                                                
                                                try:
                                                    if column and subString and startPos:
                                                        expression += ' ' + column + ' '+ subString +' Starting position ' + str(startPos)
                                                        # st.subheader("Substring Check Validation Results - " + column)
                                                        resultDF, publishDF,publishDF2,primarySummary = substringCheck(file_name, df.copy(), column,subString,startPos, caseSensitivity)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)
                                        
                                        if 'Date Check Weekend' in validationRule:
                                                st.sidebar.caption("Date Check")
                                                column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                                # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                               
                                                try:
                                                    if column:
                                                        expression += ' ' + column 
                                                        # st.subheader("Date Check Validation Results")
                                                        resultDF, publishDF,publishDF2,primarySummary = DatecheckWeekend(file_name, df, column)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Date Check Future Date' in validationRule:
                                                st.sidebar.caption("Date Check")
                                                column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                                # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                                
                                                try:
                                                    if column:
                                                        expression += ' ' + column 
                                                        # st.subheader("Date Check Validation Results")
                                                        resultDF, publishDF,publishDF2,primarySummary = DatecheckFutureDate(file_name, df, column)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Date Check Weekday' in validationRule:
                                                st.sidebar.caption("Date Check")
                                                column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                                # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                                
                                                try:
                                                    if column:
                                                        expression += ' ' + column 
                                                        # st.subheader("Date Check Validation Results")
                                                        resultDF, publishDF,publishDF2,primarySummary = DatecheckWeekday(file_name, df, column)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Date Check Non Future Date' in validationRule:
                                                st.sidebar.caption("Date Check")
                                                column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                                # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                                
                                                try:
                                                    if column:
                                                        expression += ' ' + column 
                                                        # st.subheader("Date Check Validation Results")
                                                        resultDF, publishDF,publishDF2,primarySummary = DatecheckNonFutureDate(file_name, df, column)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        # ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                except Exception as e:
                                                    st.error(e)

                                        if 'Pattern Check' in validationRule:
                                            st.sidebar.caption("Pattern Check")
                                            column = st.sidebar.selectbox("select column",df.columns,index=None,key=str(key)+'Pattern Check')
                                            operator = st.sidebar.selectbox("select Operator",['Matching','Non Matching'],index=None,key=str(key)+'PatternOperator')
                                            value = st.sidebar.text_input("Enter Pattern",key = str(key)+"pattern_value")
                                            
                                            if column and operator and value:
                                                expression += ' ' + column + ' '+ operator + ' ' + value
                                                # st.subheader("Pattern Check Validation results")
                                                resultDF, publishDF,publishDF2,primarySummary = PatternCheck(file_name, df, column,operator,value)
                                                resultantDF = resultantDF._append(resultDF)
                                                primarySummaryData = primarySummaryData._append(primarySummary)
                                                # ShowDFAndChart(resultDF)
                                                finalPublishDF = finalPublishDF._append(publishDF)
                                                finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                st.divider()
                                        
                                        if 'Date Validity Check' in validationRule:
                                            st.sidebar.caption("Date Validity Check")
                                            column = st.sidebar.selectbox("select column",df.columns,index=None,key=str(key)+'Date_Validity')
                                            if column:
                                                # st.subheader("Date Validation Check Results")
                                                resultDF, publishDF,publishDF2,primarySummary = DateValidationCheck(file_name, df, column)
                                                resultantDF = resultantDF._append(resultDF)
                                                primarySummaryData = primarySummaryData._append(primarySummary)
                                                # ShowDFAndChart(resultDF)
                                                finalPublishDF = finalPublishDF._append(publishDF)
                                                finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                st.divider()
                                        if 'Cardinality Check' in validationRule:
                                            st.sidebar.caption("Cardinality Check")   
                                            try:
                                                dfCardinality=df.copy(deep=True)
                                                col1 = st.sidebar.selectbox("select first column",dfCardinality.columns,index=None,key=str(key)+'CardiCheck1')
                                                col2 = st.sidebar.selectbox("select second column",dfCardinality.columns,index=None,key=str(key)+'CardiCheck2')
                                                operator1=st.sidebar.selectbox("Select Condition",['','Should Be','Should Not Be'],key='cardycheckOp11' + str(key))
                                                operator11=st.sidebar.selectbox("Select Cardinality",['','1 : 1','1 : Many','Many : 1','Many : Many'] ,key='cardyCheckOp2' + str(key))
                                                
                                                group1 = dfCardinality.groupby(col1)[col2].nunique()
                                                group2 = dfCardinality.groupby(col2)[col1].nunique()

                                                col1_to_col2_cardinality = {}
                                                for key, val in group1.items():
                                                    if val == 1:
                                                        col1_to_col2_cardinality[key] = "1 : 1" if group2[dfCardinality[dfCardinality[col1] == key][col2].iloc[0]] == 1 else "Many : 1"
                                                    else:
                                                        col1_to_col2_cardinality[key] = "1 : Many" if all(group2[dfCardinality[dfCardinality[col1] == key][col2]] == 1) else "Many : Many"

                                                dfCardinality['Cardinality'] = dfCardinality[col1].map(col1_to_col2_cardinality)
                                                resultDF, publishDF,publishDF2,primarySummary = cardinality_check(file_name,dfCardinality,col1,col2,operator1,operator11)
                                                resultantDF = resultantDF._append(resultDF)
                                                primarySummaryData = primarySummaryData._append(primarySummary)
                                                
                                                finalPublishDF = finalPublishDF._append(publishDF)
                                                finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                st.divider()
                                            except Exception as e:
                                                 st.write(e)
                                except Exception as e:
                                    st.write(e)
                                                                
                                st.sidebar.divider()
                        try:       
                                if 'composite_rule2' in st.session_state and len(st.session_state.rule2)>=2 and st.sidebar.button("Execute Composite Rules",disabled=True if len(st.session_state.rule2)<2 else False ) or 'ruleProfilingTable' in st.session_state:
                                    
                                    
                                    if condition=="AND":
                                        unique_rules=resultantDF['RuleID'].unique()
                                        unique_columns=resultantDF['ColumnName'].unique()
                                        firstcol=unique_columns[0]
                                        if len(unique_columns)==1:
                                            secondcol=unique_columns[0]
                                        else:
                                            secondcol=unique_columns[1]

                                        firstrule=unique_rules[0]
                                        if len(unique_rules)==1:
                                            secondrule=unique_rules[0]
                                        else:
                                            secondrule=unique_rules[1]
                                        
                                        resdf1=resultantDF[(resultantDF['RuleID']==firstrule) & (resultantDF['ColumnName']==firstcol)]
                                        resdf2=resultantDF[(resultantDF['RuleID']==secondrule) & (resultantDF['ColumnName']==secondcol)]

                                        rows=resdf1.shape[0]        
                                        cols=resdf1.shape[1]
                                        colss=resultantDF.columns
                                        # global resdf3
                                        resdf3=pd.DataFrame(columns=colss)
                                        #  for i in range(0,rows):
                                        resdf3['TableName']=resdf1['TableName']
                                        resdf3['ColumnName']=np.where(resdf1['ColumnName'].isna(),resdf1['ColumnName'],resdf1['ColumnName'].astype(str)) + ' | ' + resdf2['ColumnName'].astype(str)
                                        resdf3['RuleID']=resdf1['RuleID'].astype(str) + ' | ' + resdf2['RuleID'].astype(str)
                                        resdf3['Input']=resdf1['Input'].astype(str) + ' | ' + resdf2['Input'].astype(str)
                                        #resdf3['Status']=func(resdf1['Status'],resdf2['Status']) 
                                        resdf3['Execution_time']=resdf1['Execution_time']
                                        for i in range(0,rows):
                                            resdf3.iloc[i,4]=funcAnd(resdf1.iloc[i,4],resdf2.iloc[i,4])
                                        # st.write(resdf3)

                                    elif condition == 'OR':
                                        unique_rules=resultantDF['RuleID'].unique()
                                        unique_columns=resultantDF['ColumnName'].unique()
                                        firstcol=unique_columns[0]
                                        if len(unique_columns)==1:
                                            secondcol=unique_columns[0]
                                        else:
                                            secondcol=unique_columns[1]

                                        firstrule=unique_rules[0]
                                        if len(unique_rules)==1:
                                            secondrule=unique_rules[0]
                                        else:
                                            secondrule=unique_rules[1]
                                        resdf1=resultantDF[(resultantDF['RuleID']==firstrule) & (resultantDF['ColumnName']==firstcol)]
                                        resdf2=resultantDF[(resultantDF['RuleID']==secondrule) & (resultantDF['ColumnName']==secondcol)]

                                        rows=resdf1.shape[0]
                                        cols=resdf1.shape[1]
                                        colss=resultantDF.columns
                                        resdf3=pd.DataFrame(columns=colss)
                                        #  for i in range(0,rows):
                                        resdf3['TableName']=resdf1['TableName']
                                        resdf3['ColumnName']=resdf1['ColumnName'].astype(str) + ' | ' + resdf2['ColumnName'].astype(str)
                                        resdf3['RuleID']=resdf1['RuleID'].astype(str) + ' | ' + resdf2['RuleID'].astype(str)
                                        resdf3['Input']=resdf1['Input'].astype(str) + ' | ' + resdf2['Input'].astype(str)
                                        #resdf3['Status']=func(resdf1['Status'],resdf2['Status']) 
                                        resdf3['Execution_time']=resdf1['Execution_time']
                                        for i in range(0,rows):
                                            resdf3.iloc[i,4]=funcOr(resdf1.iloc[i,4],resdf2.iloc[i,4])
                                    else:
                                        st.write("Please choose Logical Operator to continue analysis.")
                        except Exception as e:
                             st.write(e)
                                

                        if resdf3.empty is False :
                            
                            st.write(expression)
                            ShowDFAndChart(resdf3)
                            col1, col2, col3, col4 = st.columns(4)
                            col1.download_button(label='Download Results', data= resdf3.to_csv(index=False).encode('utf-8'), file_name='Business_Rule_Profile.csv', key='downloadData', mime='text/csv', on_click=session_setting())
                            
                            calculatedDF = pd.DataFrame()
                            calculatedDF['Input'] = resdf3['Input']

                            df1,df2,df3 = assignData(calculatedDF, resdf3['ColumnName'].head(1).values[0], resdf3['RuleID'].head(1).values[0], resdf3['TableName'].head(1).values[0], resdf3['Execution_time'].head(1).values[0], resdf3['RuleStatus'])
                            Fail_dataframe = publishPrimaryDataDependentRule(df,calculatedDF,resdf3['ColumnName'], resdf3['RuleID'], resdf3['TableName'], resdf3['Execution_time'], resdf3['RuleStatus'])
                            
                            

                            if col4.button(label='Publish Business Rule Summary Results to Database', key='exportToSQL2',use_container_width=True):

                                if len(st.session_state.source_system.strip())!=0:
                                    df3['SourceSystem'] = st.session_state.source_system
                                    # st.dataframe(df3.head(3))
                                    df4 = df3.head(3)
                                    try:
                                        write_to_sql_trend(df4, 'IDATAPROFILE_BIZ_RULE_PROF_SUMMARY')                   
                                    except Exception as e:
                                        st.error(f"Current Table have no access, Change your table. Error - {e}")
                                    else:
                                        
                                        modal = Modal(key="BRPSource_system collectsummary", title='', max_width=1000)
                                        with modal.container():
                                            st.success("Successfully Published Business Rule Summary Results to Database")
                                         
                                        

                                else:
                                    # st.warning("Please Select Source System in Order to Publish Business Rule Summary Results")
                                    modal = Modal(key="BRPSource_system collectsource", title='', max_width=1000)
                                    with modal.container():
                                            st.warning("Please Select Source System in Order to Publish Business Rule Summary Results")
                                         
                                
                               
                            if col4.button(label='Publish Business Rule Fail Records to Database', key='exportToSQL4',use_container_width=True):
                                try:
                                    write_to_sql(Fail_dataframe[Fail_dataframe["RuleStatus"]=='FAIL'], 'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')
                                except Exception as e:
                                    st.error(e)
                                else:
                                    
                                    modal = Modal(key="BRPSource_system collectfail", title='', max_width=1000)
                                    with modal.container():
                                            st.success("Successfully Published Business Rule Failed Records to Database.")
                            if col4.button(label='Publish All Business Rule Records to Database', key='exportToSQL3Primarykey',disabled=True,use_container_width=True):
                                write_to_sql(Fail_dataframe, 'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')

                           
                        st.header('')   
                        st.header('')   
                        scroll_up_button('connect-to-source-data')
            else:
                        list_of_checks = ['Unallowable Keywords/ Characters', 'Null/ Blank', 'Value check', 'Length check', 'Email Validity', 'Duplicate using composite key', 'Leading or Trailing characters', 'Cross Column Compare', 'Substring Check','Date Check Weekend','Date Check Future Date','Date Check Weekday','Date Check Non Future Date', 'Allowable Keywords','Pattern Check','Date Validity Check','Cardinality Check']
          
                        if 'rule' in st.session_state:
                            for key, value in st.session_state.rule.items():
                                validationRule = st.sidebar.selectbox(label="Select rule", options=list_of_checks, index=None, key='rule' + str(key))
                            
                                if validationRule:
                                    if 'Unallowable Keywords/ Characters' in validationRule:
                                        st.sidebar.caption('Unallowable Keywords/ Characters')
                                        column = st.sidebar.selectbox('Select column', df.columns, index=None, key='unallowableCol' + str(key))
                                        keywords = st.sidebar.text_input("Enter unallowable keywords/ characters", placeholder = 'Enter only comma separated keywords', key='unallowableKeyword' + str(key))
                                        caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='unallowableColCase' + str(key))
                                        try:
                                                if column and len(keywords) != 0:
                                                    st.subheader('Unallowable Keywords/ Characters Validation Results - ' + column)
                                                    resultDF, publishDF,publishDF2,primarySummary = unallowableKeywords(file_name, df, column, keywords, caseSensitivity)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                        except Exception as e:
                                            st.error(e)
                                    
                                    if 'Allowable Keywords' in validationRule:
                                        st.sidebar.caption('Allowable Keywords')
                                        column = st.sidebar.selectbox('Select column', df.columns, index=None, key='allowableCol' + str(key))
                                        keywords = st.sidebar.text_input("Enter allowable keywords", placeholder = 'Enter only comma separated keywords', key='allowableKeyword' + str(key))
                                        caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='allowableColCase' + str(key))
                                        try:
                                                if column and len(keywords) != 0:
                                                    st.subheader('Allowable Keywords Validation Results - ' + column)
                                                    resultDF, publishDF,publishDF2,primarySummary = allowableKeywords(file_name, df, column, keywords, caseSensitivity)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            
                                        except Exception as e:
                                            st.error(e)
                                    
                                    if 'Null/ Blank' in validationRule:
                                        st.sidebar.caption('Null/ Blanks')
                                        column = st.sidebar.selectbox('Select column', df.columns, index=None, key='nullBlankCol' + str(key))
                                        NullRuleType=st.sidebar.selectbox("Select Null Rule Type",['Should be NULL','Should not be NULL'],key='nullBlankColRulType' + str(key))
                                        try:
                                            
                                                if column:
                                                    
                                                        if NullRuleType == 'Should be NULL':
                                                                    resultDF, publishDF,publishDF2,primarySummary = shouldbenullsBlanks(file_name, df, column)
                                                                    resultantDF = resultantDF._append(resultDF)
                                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                                    ShowDFAndChart(resultDF)
                                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                                    st.divider()
                                                        else:
                                                                    resultDF, publishDF,publishDF2,primarySummary = nullsBlanks(file_name, df, column)
                                                                    resultantDF = resultantDF._append(resultDF)
                                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                                    ShowDFAndChart(resultDF)
                                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                                    st.divider()
                                                    
                                        except Exception as e:
                                            st.error(e)

                                    if  'Value check' in validationRule:
                                            st.sidebar.caption('Value check')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key= 'valueCol' + str(key))
                                            operator = st.sidebar.selectbox('Operator',['should be', 'should not be', 'should be greater than', 'should be greater than or equal to', 'should be less than', 'should be less than or equal to', 'should be between','is in'], index = None, placeholder='Select operator', key='valueCheckOperator' + str(key))
                                            
                                            try:
                                                if operator == 'should be between':
                                                    value = st.sidebar.text_input('Value1', placeholder = 'Enter value', key='value1CheckInput' + str(key))
                                                    value2 = st.sidebar.text_input('Value2', placeholder = 'Enter value', key='value2CheckInput' + str(key))
                                                else:
                                                    value = st.sidebar.text_input('Value', placeholder = 'Enter value', key='valueCheckInput' + str(key))
                                                    value2 = True

                                                caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='ValueCheckColCase' + str(key))

                                                if column and operator and value and value2:
                                                        st.subheader("Value check Validation Results - " + column)
                                                    
                                                        if operator == 'should be between':
                                                            resultDF, publishDF,publishDF2,primarySummary = valueCheck(file_name, df, column, operator, value, value2, caseSensitivity)
                                                        else:
                                                            resultDF, publishDF,publishDF2,primarySummary = valueCheck(file_name, df, column, operator, value, '0', caseSensitivity)
                                                        resultantDF = resultantDF._append(resultDF)
                                                        primarySummaryData = primarySummaryData._append(primarySummary)
                                                        ShowDFAndChart(resultDF)
                                                        finalPublishDF = finalPublishDF._append(publishDF)
                                                        finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                        st.divider()
                                                        
                                            except Exception as e:
                                                st.error(e)

                                    if 'Length check' in validationRule:
                                            st.sidebar.caption('Length check')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key='lenCol' + str(key))
                                            operator = st.sidebar.selectbox('Operator',['should be', 'should not be', 'should be greater than', 'should be greater than or equal to', 'should be less than', 'should be less than or equal to', 'should be between'], index = None, placeholder='Select operator', key='lengthCheckOperator' + str(key))
                                            
                                            try:
                                                if operator == 'should be between':
                                                    length = st.sidebar.number_input('Minimum Length', min_value=0, value=0, step=1, key='minLength' + str(key))
                                                    length2 = st.sidebar.number_input('Maximum Length', min_value=0, value=0, step=1, key='maxLength' + str(key))
                                                else:
                                                    length = st.sidebar.number_input('Length', min_value=0, value=0, step=1, key='singleLength' + str(key))

                                                if column and operator:
                                                    st.subheader("Length check Validation Results - " + column)
                                                    
                                                    if operator == 'should be between':
                                                        resultDF, publishDF,publishDF2,primarySummary = lengthCheck(file_name, df, column, operator, length, length2)
                                                    else:
                                                        resultDF, publishDF,publishDF2,primarySummary = lengthCheck(file_name, df, column, operator, length)
                                                    
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)

                                    if 'Email Validity' in validationRule:
                                            st.sidebar.caption('Email Validity')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key='emailCol' + str(key))
                                            
                                            try:
                                                if column:
                                                    st.subheader("Email Validity Validation Results - " + column)
                                                    resultDF, publishDF,publishDF2,primarySummary = validateEmail(file_name, df, column)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)

                                    if 'Duplicate using composite key' in validationRule:
                                            st.sidebar.caption('Duplicate using composite key')
                                            columns = st.sidebar.multiselect('Select columns', df.columns, key='duplicateCol' + str(key))
                                            
                                            try:
                                                if columns:
                                                    st.subheader('Duplicate using composite key Validation Results - '+ str(columns))
                                                    resultDF, publishDF,publishDF2,primarySummary = compositeKeyDuplicates(file_name, df, columns)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)
                                    

                                    if 'Leading or Trailing characters' in validationRule:
                                            st.sidebar.caption('Leading or Trailing characters')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key='leadTrailCol' + str(key))
                                            option = st.sidebar.selectbox('Option', ['starts with', 'ends with', 'starts and ends with', 'starts or ends with'], index = None, placeholder='Select an option', key='leadTrailChars' + str(key))
                                            character = st.sidebar.text_input('Character', placeholder = 'Enter char to check for', key='leadTrailChar' + str(key))
                                            caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='leadTrailColCase' + str(key))
                                            
                                            try:
                                                if column and option and character:
                                                    st.subheader('Leading or Trailing characters Validation Results - ' + column)
                                                    resultDF, publishDF,publishDF2,primarySummary = leadTrailingCharacters(file_name, df, column, option, character, caseSensitivity)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)
                                    if 'Cross Column Compare' in validationRule:
                                            st.sidebar.caption('Cross Column Compare')
                                            column1 = st.sidebar.selectbox('Select column 1', df.columns, index=None, key='crossCol1' + str(key))
                                            operator = st.sidebar.selectbox('Operation', ['matching', 'non-matching','is substring of', 'less than', 'less than or equal to', 'greater than', 'greater than or equal to'], index = None, placeholder='Select operator', key='columnCompareCheckOp' + str(key))
                                            column2 = st.sidebar.selectbox('Select column 2', df.columns, index=None, key='crossCol2' + str(key))
                                            caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='ValueCheckColCase' + str(key))
                                            try:
                                                if column1 and operator and column2:
                                                     
                                                    st.subheader("Cross Column Compare Validation Results - [" + column1 + ', ' + column2 + ']')
                                                    resultDF, publishDF,publishDF2,primarySummary = crossColumnCompare(file_name, df.copy(), column1, column2, operator, caseSensitivity)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)

                                    if 'Substring Check' in validationRule:
                                            st.sidebar.caption('Substring Check')
                                            column = st.sidebar.selectbox('Select column', df.columns, index=None, key=7)
                                            subString = st.sidebar.text_input('Substring to be searched for', placeholder = 'Enter Substring')
                                            startPos = st.sidebar.number_input('Start position of substring', min_value=1, value='min',placeholder='Enter Start Position')
                                            caseSensitivity = not st.sidebar.checkbox('Case Sensitivity', key='SubstringColCase' + str(key))
                                            
                                            try:
                                                if column and subString and startPos:
                                                    st.subheader("Substring Check Validation Results - " + column)
                                                    resultDF, publishDF,publishDF2,primarySummary = substringCheck(file_name, df.copy(), column,subString,startPos, caseSensitivity)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)
                                    
                                    if 'Date Check Weekend' in validationRule:
                                            st.sidebar.caption("Date Check")
                                            column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                            # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                            
                                            try:
                                                if column:
                                                    st.subheader("Date Check Validation Results")
                                                    resultDF, publishDF,publishDF2,primarySummary = DatecheckWeekend(file_name, df, column)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)

                                    if 'Date Check Future Date' in validationRule:
                                            st.sidebar.caption("Date Check")
                                            column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                            # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                            
                                            try:
                                                if column:
                                                    st.subheader("Date Check Validation Results")
                                                    resultDF, publishDF,publishDF2,primarySummary = DatecheckFutureDate(file_name, df, column)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)

                                    if 'Date Check Weekday' in validationRule:
                                            st.sidebar.caption("Date Check")
                                            column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                            # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                            
                                            try:
                                                if column:
                                                    st.subheader("Date Check Validation Results")
                                                    resultDF, publishDF,publishDF2,primarySummary = DatecheckWeekday(file_name, df, column)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)

                                    if 'Date Check Non Future Date' in validationRule:
                                            st.sidebar.caption("Date Check")
                                            column = st.sidebar.selectbox("select column",df.columns,index=None,key='Datecheck')
                                            # operator = st.sidebar.selectbox("Operation",['Should be Future Date','Should Not be Future Date','Should Not be Weekend','Should be on weekend'],index = None, placeholder='Select operator', key='DatecheckOp' + str(key))
                                            
                                            try:
                                                if column:
                                                    st.subheader("Date Check Validation Results")
                                                    resultDF, publishDF,publishDF2,primarySummary = DatecheckNonFutureDate(file_name, df, column)
                                                    resultantDF = resultantDF._append(resultDF)
                                                    primarySummaryData = primarySummaryData._append(primarySummary)
                                                    ShowDFAndChart(resultDF)
                                                    finalPublishDF = finalPublishDF._append(publishDF)
                                                    finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                    st.divider()
                                            except Exception as e:
                                                st.error(e)

                                    if 'Pattern Check' in validationRule:
                                        st.sidebar.caption("Pattern Check")
                                        column = st.sidebar.selectbox("select column",df.columns,index=None,key=str(key)+'Pattern Check')
                                        operator = st.sidebar.selectbox("select Operator",['Matching','Non Matching'],index=None,key=str(key)+'PatternOperator')
                                        value = st.sidebar.text_input("Enter Pattern",key = str(key)+"pattern_value")
                                        if column and operator and value:
                                            st.subheader("Pattern Check Validation results")
                                            resultDF, publishDF,publishDF2,primarySummary = PatternCheck(file_name, df, column,operator,value)
                                            resultantDF = resultantDF._append(resultDF)
                                            primarySummaryData = primarySummaryData._append(primarySummary)
                                            ShowDFAndChart(resultDF)
                                            finalPublishDF = finalPublishDF._append(publishDF)
                                            finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                            st.divider()
                                    
                                    if 'Date Validity Check' in validationRule:
                                        st.sidebar.caption("Date Validity Check")
                                        column = st.sidebar.selectbox("select column",df.columns,index=None,key=str(key)+'Date_Validity')
                                        if column:
                                            st.subheader("Date Validation Check Results")
                                            resultDF, publishDF,publishDF2,primarySummary = DateValidationCheck(file_name, df, column)
                                            resultantDF = resultantDF._append(resultDF)
                                            primarySummaryData = primarySummaryData._append(primarySummary)
                                            ShowDFAndChart(resultDF)
                                            finalPublishDF = finalPublishDF._append(publishDF)
                                            finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                            st.divider()  
                                    if 'Cardinality Check' in validationRule:
                                            st.sidebar.caption("Cardinality Check")   
                                            try:
                                                dfCardinality=df.copy(deep=True)
                                                col1 = st.sidebar.selectbox("select first column",dfCardinality.columns,index=None,key=str(key)+'CardiCheck1')
                                                col2 = st.sidebar.selectbox("select second column",dfCardinality.columns,index=None,key=str(key)+'CardiCheck2')
                                                operator1=st.sidebar.selectbox("Select Condition",['','Should Be','Should Not Be'],key='cardycheckOp11' + str(key))
                                                operator11=st.sidebar.selectbox("Select Cardinality",['','1 : 1','1 : Many','Many : 1','Many : Many'] ,key='cardyCheckOp2' + str(key))
                                                
                                                group1 = dfCardinality.groupby(col1)[col2].nunique()
                                                group2 = dfCardinality.groupby(col2)[col1].nunique()

                                                col1_to_col2_cardinality = {}
                                                for key, val in group1.items():
                                                    if val == 1:
                                                        col1_to_col2_cardinality[key] = "1 : 1" if group2[dfCardinality[dfCardinality[col1] == key][col2].iloc[0]] == 1 else "Many : 1"
                                                    else:
                                                        col1_to_col2_cardinality[key] = "1 : Many" if all(group2[dfCardinality[dfCardinality[col1] == key][col2]] == 1) else "Many : Many"

                                                dfCardinality['Cardinality'] = dfCardinality[col1].map(col1_to_col2_cardinality)
                                                resultDF, publishDF,publishDF2,primarySummary = cardinality_check(file_name,dfCardinality,col1,col2,operator1,operator11)
                                                resultantDF = resultantDF._append(resultDF)
                                                primarySummaryData = primarySummaryData._append(primarySummary)
                                                ShowDFAndChart(resultDF)
                                                finalPublishDF = finalPublishDF._append(publishDF)
                                                finalPublishDF2 = finalPublishDF2._append(publishDF2)
                                                st.divider()
                                            except:
                                                 pass            

                                st.sidebar.divider()

                        if st.sidebar.button('Add rule', on_click=session_setting_rule):
                            new_uuid = uuid.uuid4()
                            

                        if resultantDF.empty is False:
                            col1, col2, col3, col4 = st.columns(4)
                            col1.download_button(label='Download Results', data= resultantDF.to_csv(index=False).encode('utf-8'), file_name='Business_Rule_Profile.csv', key='downloadData', mime='text/csv', on_click=session_setting())
                           
                            if col4.button(label='Publish Business Rule Summary Results to Database', key='exportToSQL2',use_container_width=True):

                                if len(st.session_state.source_system.strip())!=0:
                                    finalPublishDF2['SourceSystem'] = st.session_state.source_system
                                    # st.dataframe(finalPublishDF2)

                                    try:
                                        write_to_sql_trend(finalPublishDF2, 'IDATAPROFILE_BIZ_RULE_PROF_SUMMARY')                   
                                    except Exception as e:
                                        st.error(f"Current Table have no access, Change your table. Error - {e}")
                                    else:
                                        
                                        modal = Modal(key="BRPSource_system collectSummary2", title='', max_width=1000)
                                        with modal.container():
                                            st.success("Successfully Published Business Rule Summary Results to Database")

                                else:
                                    
                                    modal = Modal(key="BRPSource_system collectSource2", title='', max_width=1000)
                                    with modal.container():
                                            st.warning("Please Select Source System in Order to Publish Business Rule Summary Results")

                            if col4.button(label='Publish Business Rule Fail Records to Database', key='exportToSQL4',use_container_width=True):
                                try:
                                    write_to_sql(primarySummaryData[primarySummaryData["RuleStatus"]=='FAIL'], 'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')
                                except Exception as e:
                                    st.error(e)
                                else:
                                    
                                    modal = Modal(key="BRPSource_system collectFail2", title='', max_width=1000)
                                    with modal.container():
                                            st.success("Successfully Published Business Rule Failed Records to Database.")

                            if col4.button(label='Publish All Business Rule Records to Database', key='exportToSQL3Primarykey',disabled=True,use_container_width=True):
                                write_to_sql(primarySummaryData[primarySummaryData["RuleStatus"].isin(['PASS','NULL','FAIL'])], 'IDATAPROFILE_BIZ_RULE_PROF_STATS_FAIL')

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
                                columns = resultantDF.drop_duplicates(subset = ['ColumnName','RuleID'])[['ColumnName','RuleID']]
                                colibra_columns = ['SourceSystemName','TableName','ColumnName','Name','Full Name','DQ Dimension','Definition','Inclusion Scenario','Exception Scenario','Threshold','Loaded Rows','Rows Passed','Rows Failed','Result','Entity Load Date','Conformity Score','Non Conformity Score','Last Sync Date','SchemaName','DatabaseName','Passing Fraction']
                                colibra_data = pd.DataFrame(columns=colibra_columns)
                                i = 0
                                for index, row in columns.iterrows():
                                    i+=1
                                    df = resultantDF.query(f" ColumnName == '{row['ColumnName']}' and RuleID == '{row['RuleID']}' ")
                                    central_dq_repo_df = pd.read_csv('IDATAPROFILER/static/CentralDQRepository.csv',encoding_errors= 'replace')
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
                                        'TableName': file_name ,
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

                                    if st.button("Publish For Collibra Integration",on_click = remove_collibra_state, disabled = missing_fields) or 'publish_collibra' in st.session_state:
                                        try:
                                            write_to_sql(colibra_data, 'IDATAPROFILE_CATALOG_DQ_METRICS')
                                        except Exception as e:
                                            st.error(e)
                                        else:
                                            st.session_state.show_collibra_success = True
                                        finally:
                                            del st.session_state.publish_collibra
                                            if 'collibra' in st.session_state:
                                                del st.session_state.collibra
                                                st.rerun()

                                if st.session_state.show_collibra_success:
                                    modal = Modal(key="BRPSource_system collectSummary2", title='', max_width=1000)
                                    with modal.container():
                                        st.success("Successfully Published Business Rule Summary Results to Database")
                                    st.session_state.show_collibra_success = False

                        st.header('')   
                        st.header('')   
                        scroll_up_button('connect-to-source-data')
