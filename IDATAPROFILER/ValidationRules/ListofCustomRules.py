import pandas as pd
import string
import numpy as np
import datetime
from collections import Counter
import math
import calendar
import streamlit as st
from utility import *
import re


order_of_columns = ['TableName', 'ColumnName', 'RuleID', 'Input', 'RuleStatus', 'Execution_time']
publish_columns = ['TableName', 'ColumnName', 'RuleID', 'PASS', 'FAIL', 'NULL', 'Execution_time']
publish_columns2 = ['TableName', 'ColumnName', 'RuleID', 'RuleStatus', 'Count','Execution_time']
publish_primary_key_summary = ['TableName', 'ColumnName', 'RuleID', 'KeyColumns', 'KeyValues', 'Input', 'RuleStatus', 'Execution_time']

def publishPrimaryDataDependentRule(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, status):
    publish_primary = pd.DataFrame()
    
    publish_primary['Input'] = calculatedDF['Input']
    publish_primary['KeyValues'] = inputTableDF[st.session_state.primary_columns].apply(lambda row: '|'.join(row.values.astype(str)), axis=1)
    publish_primary['KeyColumns'] = '|'.join(st.session_state.primary_columns)
    publish_primary['RuleID'] = ruleId
    publish_primary['TableName'] = tableName
    publish_primary['ColumnName'] = columnName
    publish_primary['Execution_time'] = execution_datetime
    publish_primary['RuleStatus'] = status
    publish_primary = publish_primary.reindex(columns = publish_primary_key_summary)
    # st.dataframe(publish_primary)
    return publish_primary

def publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, status):
    publish_primary = pd.DataFrame()
  
    publish_primary['Input'] = calculatedDF.rename(columns = {columnName:'Input'})[['Input']]
    publish_primary['KeyValues'] = inputTableDF[st.session_state.primary_columns].apply(lambda row: '|'.join(row.values.astype(str)), axis=1)
    publish_primary['KeyColumns'] = '|'.join(st.session_state.primary_columns)
    publish_primary['RuleID'] = ruleId
    publish_primary['TableName'] = tableName
    publish_primary['ColumnName'] = columnName
    publish_primary['Execution_time'] = execution_datetime
    publish_primary['RuleStatus'] = status
    publish_primary = publish_primary.reindex(columns = publish_primary_key_summary)
    # st.dataframe(publish_primary)
    return publish_primary

def assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, status):
    calculatedDF['RuleID'] = ruleId
    calculatedDF['TableName'] = tableName
    calculatedDF['ColumnName'] = columnName
    calculatedDF['RuleStatus'] = status
    calculatedDF['Execution_time'] = execution_datetime
    calculatedDF = calculatedDF.reindex(columns = order_of_columns)

    publishDF = {}
    statusCount = Counter(status)
    publishDF['PASS'] = statusCount['PASS']
    publishDF['FAIL'] = statusCount['FAIL']
    publishDF['NULL'] = statusCount['NULL']
    publishDF['RuleID'] = ruleId
    publishDF['TableName'] = tableName
    publishDF['ColumnName'] = columnName
    publishDF['Execution_time'] = execution_datetime
    publishDF = pd.DataFrame(publishDF, index=[0]).reindex(columns = publish_columns)

    publishDF2 = {}
    statusCount = Counter(status)
    publishDF2['RuleStatus'] = pd.Series(['PASS','FAIL','NULL'])
    publishDF2['Count'] = pd.Series([statusCount['PASS'],statusCount['FAIL'],statusCount['NULL']])
    publishDF2['RuleID'] = pd.Series([ruleId]*3)
    publishDF2['TableName'] = pd.Series([tableName]*3)
    publishDF2['ColumnName'] = pd.Series([columnName]*3)
    publishDF2['Execution_time'] = pd.Series([execution_datetime]*3)
    publishDF2 = pd.DataFrame(publishDF2).reindex(columns = publish_columns2)

    return calculatedDF, publishDF, publishDF2
@st.cache_data
#Function to check for unallowed keywords in a column - DQ001
def unallowableKeywords(tableName, inputTableDF, columnName, arrayOfKeywords, notCaseSensitive):
    ruleId = 'DQ001'
    tempDF = pd.DataFrame()
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    tempDF[columnName] = inputTableDF[columnName]
    tempDF[columnName] = np.where(inputTableDF[columnName].isna(),inputTableDF[columnName],inputTableDF[columnName].astype(str))

    if notCaseSensitive:
         tempDF[columnName] = tempDF[columnName].str.lower()
         arrayOfKeywords = arrayOfKeywords.lower()

    arrayOfKeywords = [arrayOfKeywords] if (arrayOfKeywords[-1] == ',' or arrayOfKeywords[0] == ',') else arrayOfKeywords.split(',')
    rulestatus = tempDF[columnName].apply(lambda x: 'NULL' if pd.isna(x) or x in ['nan','<na>','<NA>','NAN'] else ('FAIL' if (any(keyword in x for keyword in arrayOfKeywords) and (arrayOfKeywords[-1] != ',' or arrayOfKeywords[0] != ',' or arrayOfKeywords == [','])) else 'FAIL' if ((arrayOfKeywords[-1] == ',' or arrayOfKeywords[0] == ',') and (x == arrayOfKeywords)) else 'PASS'))
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
#Function to check for null and blank values - DQ002
def nullsBlanks(tableName, inputTableDF, columnName):
    ruleId = 'DQ002'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    if inputTableDF[columnName].dtype == 'object':
        rulestatus = np.where(inputTableDF[columnName].isna(), 'FAIL', np.where(inputTableDF[columnName].str.strip().str.len() == 0,'FAIL','PASS'))
    else:
        rulestatus = np.where(inputTableDF[columnName].isna(), 'FAIL', 'PASS')
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
def shouldbenullsBlanks(tableName, inputTableDF, columnName):
    ruleId = 'DQ002'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    if inputTableDF[columnName].dtype == 'object':
        rulestatus = np.where(inputTableDF[columnName].isna(), 'PASS', np.where(inputTableDF[columnName].str.strip().str.len() == 0,'PASS','FAIL'))
    else:
        rulestatus = np.where(inputTableDF[columnName].isna(), 'PASS', 'FAIL')
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
#Function to compare two values - DQ003
def valueCheck(tableName, inputTableDF, columnName, operator, value, value2, notCaseSensitive):
     ruleId = 'DQ003'
     execution_datetime = datetime.now().date()
     calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
     tempDF = pd.DataFrame()
     tempDF[columnName] = inputTableDF[columnName]
     try:
        if operator != 'is in':
            if 'int' in str(calculatedDF['Input'].dtype).lower():
                value = int(value)
                value2 = int(value2)
            elif 'float' in str(calculatedDF['Input'].dtype).lower(): 
                value = float(value)
                value2 = float(value2)
            elif 'date' in str(calculatedDF['Input'].dtype).lower():
                value = pd.to_datetime(value,format='mixed')

            # calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, status)
            # publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, status)  
            # return calculatedDF, publishDF,publishDF2,publishPrimary
     except ValueError:
         value = float('NaN') if 'float' in str(calculatedDF['Input'].dtype) else float('NaN')
         

     if notCaseSensitive and inputTableDF[columnName].dtype == 'object':
         tempDF[columnName] = inputTableDF[columnName].str.lower()
         value = value.lower()
         value2 = value2.lower()

     if operator == 'should be':
                rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where((tempDF[columnName].replace({pd.NA:None})) == value, 'PASS', 'FAIL'))
            
     elif operator == 'should be less than':
                rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].replace({pd.NA:None}) < value, 'PASS', 'FAIL'))

     elif operator == 'should be greater than':
                rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].replace({pd.NA:None}) > value, 'PASS', 'FAIL'))

     elif operator == 'should be less than or equal to':
                rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].replace({pd.NA:None}) <= value, 'PASS', 'FAIL'))

     elif operator == 'should be greater than or equal to':
                rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].replace({pd.NA:None}) >= value, 'PASS', 'FAIL'))

     elif operator == 'should not be':
                rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].replace({pd.NA:None}) != value, 'PASS', 'FAIL'))

     elif operator == 'should be between':
                if 'date' in str(calculatedDF['Input'].dtype).lower():
                      value2 = pd.to_datetime(value2,format='mixed')
                rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where((tempDF[columnName].replace({pd.NA:None}) >= value) & (tempDF[columnName].replace({pd.NA:None}) <= value2), 'PASS', 'FAIL')) 
     elif operator == 'is in':
                datatype_change = pd.DataFrame(np.where(tempDF[columnName].isna(),tempDF[columnName],tempDF[columnName].astype('str')),columns=[columnName])
                rulestatus = np.where(datatype_change[columnName].isna(), 'NULL', np.where(datatype_change[columnName].replace({pd.NA:None}).isin(value.split(',')), 'PASS', 'FAIL'))
     calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)
     publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
     return calculatedDF, publishDF,publishDF2,publishPrimary

    
@st.cache_data
#Function to compare lengths of two values - DQ004
def lengthCheck(tableName, inputTableDF, columnName, operator, length, length2=0):
    ruleId = 'DQ004'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]

    # if inputTableDF[columnName].dtype != 'object':
    #     inputTableDF[columnName] = np.where(pd.isna(inputTableDF[columnName]),inputTableDF[columnName],inputTableDF[columnName].astype(str))

    if operator == 'should be':
        rulestatus = np.where(pd.isna(inputTableDF[columnName]), 'NULL', np.where(inputTableDF[columnName].astype(str).str.len() == length, 'PASS', 'FAIL'))

    elif operator == 'should be less than':
        rulestatus = np.where(pd.isna(inputTableDF[columnName]), 'NULL', np.where(inputTableDF[columnName].astype(str).str.len() < length, 'PASS', 'FAIL'))

    elif operator == 'should be greater than':
        rulestatus = np.where(pd.isna(inputTableDF[columnName]), 'NULL', np.where(inputTableDF[columnName].astype(str).str.len() > length, 'PASS', 'FAIL'))

    elif operator == 'should be greater than or equal to':
        rulestatus = np.where(pd.isna(inputTableDF[columnName]), 'NULL', np.where(inputTableDF[columnName].astype(str).str.len() >= length, 'PASS', 'FAIL'))

    elif operator == 'should be less than or equal to':
        rulestatus = np.where(pd.isna(inputTableDF[columnName]), 'NULL', np.where(inputTableDF[columnName].astype(str).str.len() <= length, 'PASS', 'FAIL'))

    elif operator == 'should not be':
        rulestatus = np.where(pd.isna(inputTableDF[columnName]), 'NULL', np.where(inputTableDF[columnName].astype(str).str.len() != length, 'PASS', 'FAIL'))

    elif operator == 'should be between':
        rulestatus = np.where(pd.isna(inputTableDF[columnName]), 'NULL', np.where((inputTableDF[columnName].astype(str).str.len() >= length) & (inputTableDF[columnName].astype(str).str.len() <= length2), 'PASS', 'FAIL')) 

    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary


@st.cache_data
def validateEmail(tableName, inputTableDF, columnName):
    ruleId = 'DQ005'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]

    if inputTableDF[columnName].dtype != 'object':
        rulestatus = np.where(inputTableDF[columnName].isna(), 'NULL', 'FAIL')
    else:
        rulestatus = np.where(inputTableDF[columnName].isna(), 'NULL', np.where(inputTableDF[columnName].str.match("(?:[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[A-Za-z0-9-]*[A-Za-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"), 'PASS', 'FAIL'))
    
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
#Function to check for duplicates based on composite keys - DQ007
def compositeKeyDuplicates(tableName, inputTableDF, columnNames):
    ruleId = 'DQ006'
    execution_datetime = datetime.now().date()
    columnName = columnNames[0]
    calculatedDF = pd.DataFrame(columns=order_of_columns)
    calculatedDF['Input'] = inputTableDF[columnNames].apply(lambda row: '|'.join(row.values.astype(str)), axis=1)
    rulestatus = np.where(inputTableDF[columnNames].isna().any(axis=1), 'NULL', np.where(inputTableDF.duplicated(subset=columnNames, keep=False), 'FAIL', 'PASS'))
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
#Function to check for presence of leading and trailing characters - DQ008
def leadTrailingCharacters(tableName, inputTableDF, columnName, option, specialCharacter, notCaseSensitive):
    ruleId = 'DQ007'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    tempDF = pd.DataFrame()
    tempDF[columnName] = inputTableDF[columnName]

    if notCaseSensitive:
         tempDF[columnName] = inputTableDF[columnName].str.lower()
         specialCharacter = specialCharacter.lower()

    if option == 'starts with':
        rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].str.startswith(specialCharacter), 'FAIL', 'PASS'))
    elif option == 'ends with':
        rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].str.endswith(specialCharacter), 'FAIL', 'PASS'))
    elif option == 'starts and ends with':
        rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].str.startswith(specialCharacter) & tempDF[columnName].str.endswith(specialCharacter), 'FAIL', 'PASS'))
    elif option == 'starts or ends with':
        rulestatus = np.where(tempDF[columnName].isna(), 'NULL', np.where(tempDF[columnName].str.startswith(specialCharacter) | tempDF[columnName].str.endswith(specialCharacter), 'FAIL', 'PASS'))

    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary


def compareTwoColumns(df, column1, column2, operator, caseSensitivity):
    df.replace({pd.NA:None},inplace=True)
    # if(df[column1].dtype != df[column2].dtype):
    df[column1] = np.where(df[column1].isna(),df[column1] , df[column1].astype(str))
    df[column2] = np.where(df[column2].isna(), df[column2], df[column2].astype(str))
    
        
    if caseSensitivity:
         df[column1] = np.where(df[column1].isna(),df[column1], df[column1].str.lower())
         df[column2] = np.where(df[column2].isna(), df[column2], df[column2].str.lower())
         
        
    if operator == 'matching':
            rulestatus = np.where((df[column1].isna() & df[column2].isna()), 'NULL', np.where(df[column1] == df[column2], 'PASS', 'FAIL'))
    elif operator == 'non-matching':
            rulestatus = np.where((df[column1].isna() & df[column2].isna()), 'NULL', np.where(df[column1] != df[column2], 'PASS', 'FAIL'))
    elif operator == 'is substring of':
            rulestatus =df.apply(lambda row: 'NULL' if pd.isna(row[column1]) else 'PASS' if row[column1] in row[column2] else ('NULL' if row[column2] == '' or row[column1] == '' else 'FAIL'),axis=1)
    elif operator == 'less than':
            rulestatus = np.where((df[column1].isna() & df[column2].isna()), 'NULL', np.where(df[column1] < df[column2], 'PASS', 'FAIL'))
    elif operator == 'less than or equal to':
            rulestatus = np.where((df[column1].isna() & df[column2].isna()), 'NULL', np.where(df[column1] <= df[column2], 'PASS', 'FAIL'))
    elif operator == 'greater than':
            rulestatus = np.where((df[column1].isna() & df[column2].isna()), 'NULL', np.where(df[column1] > df[column2], 'PASS', 'FAIL'))
    elif operator == 'greater than or equal to':
            rulestatus = np.where((df[column1].isna() & df[column2].isna()), 'NULL', np.where(df[column1] >= df[column2], 'PASS', 'FAIL'))
    
    return rulestatus

@st.cache_data
def cardinality_check(file_name,dfCardinality, col1, col2,operator1,operator11):
   ruleId='DQ017'
   execution_datetime = datetime.now().date()
   calculatedDF = dfCardinality.rename(columns = {col1:'Input'})[['Input']]
   dfCardinality['RuleStatus']=''
   
   try: 
    if operator1 == 'Should Be':
        for i in range(len(dfCardinality)):
            if dfCardinality.iloc[i,-2] == operator11:
                 dfCardinality.iloc[i,-1]='PASS'
            else:
                 dfCardinality.iloc[i,-1]='FAIL' 
    elif operator1 == 'Should Not Be':
            for i in range(len(dfCardinality)):
                if dfCardinality.iloc[i,-2] == operator11:
                 dfCardinality.iloc[i,-1]='FAIL'
                else:
                 dfCardinality.iloc[i,-1]='PASS'


        
    else:
        # df['Status']='Fail' if df[f'{col1} to {col2} cardinality'] == operator2 else 'Pass'
        pass
    
   except:
        pass
   calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, col1, ruleId, file_name, execution_datetime, dfCardinality['RuleStatus'])               
   publishPrimary = publishPrimaryData(dfCardinality,calculatedDF, col1, ruleId, file_name, execution_datetime, dfCardinality['RuleStatus'])  
   return calculatedDF, publishDF,publishDF2,publishPrimary
    
@st.cache_data
def compareTwoColumnsCrossTable(df, column1, column2, operator):
    df.replace({pd.NA:None},inplace=True)
    if(df[column1].dtype != df[column2].dtype):
        df[column1]=df[column1].astype(str)
        df[column2]=df[column2].astype(str)

    if operator == 'matching':
            rulestatus = np.where((df[column1].isna()), 'NULL', np.where(df[column1] == df[column2], 'PASS', 'FAIL'))
    elif operator == 'non-matching':
            rulestatus = np.where((df[column1].isna()), 'NULL', np.where(df[column1] != df[column2], 'PASS', 'FAIL'))
    elif operator == 'is substring of':
            rulestatus =df.apply(lambda row: 'NULL' if pd.isna(row[column1]) else 'FAIL' if  pd.isna(row[column2]) else  'PASS' if str(row[column1]) in str(row[column2]) else ('NULL' if str(row[column2]) == '' or str(row[column1]) == '' else 'FAIL'),axis=1)
    elif operator == 'less than':
            rulestatus = np.where((df[column1].isna()), 'NULL', np.where(df[column1] < df[column2], 'PASS', 'FAIL'))
    elif operator == 'less than or equal to':
            rulestatus = np.where((df[column1].isna()), 'NULL', np.where(df[column1] <= df[column2], 'PASS', 'FAIL'))
    elif operator == 'greater than':
            rulestatus = np.where((df[column1].isna()), 'NULL', np.where(df[column1] > df[column2], 'PASS', 'FAIL'))
    elif operator == 'greater than or equal to':
            rulestatus = np.where((df[column1].isna()), 'NULL', np.where(df[column1] >= df[column2], 'PASS', 'FAIL'))
    
    return rulestatus

@st.cache_data
def crossColumnCompare(tableName, inputTableDF, columnName1, columnName2, operator, caseSensitivity):
    ruleId = 'DQ008'
    execution_datetime = datetime.now().date()
    columnName = columnName1
    calculatedDF = pd.DataFrame(columns=order_of_columns)
    calculatedDF['Input'] = inputTableDF[[columnName1, columnName2]].apply(lambda row: '|'.join(row.values.astype(str) ), axis=1)
    rulestatus = compareTwoColumns(inputTableDF[:], columnName1, columnName2, operator, caseSensitivity)
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)     
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
def substringCheck(tableName, inputTableDF,columnName,substring,startPos, notCaseSensitive):
    ruleId = 'DQ009'
    execution_datetime = datetime.now().date()
    # inputTableDF[columnName]=inputTableDF[columnName].astype(str)
    startPos-=1
    # inputTableDF[columnName]=inputTableDF[columnName].fillna('')
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    tempDF = pd.DataFrame()
    tempDF[columnName] = inputTableDF[columnName]

    if notCaseSensitive:
         tempDF[columnName] = np.where(inputTableDF[columnName].isna(),inputTableDF[columnName],inputTableDF[columnName].str.lower())
         substring = substring.lower()
    
    substring_column=tempDF[columnName].apply(lambda x: x[startPos:startPos+len(substring)] if not pd.isna(x) else x)
    
    rulestatus=substring_column.apply(lambda x: 'NULL' if pd.isna(x)  else 'PASS' if substring in x else 'FAIL')
    
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)               
    # calculatedDF.loc[calculatedDF['Input'] == 'nan','Status'] = 'NULL'
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
#Check if 2 columns of 2 tables are equal - CT001
def crossTableColumnCompare(tableName, inputTableDF, refTableDF, columnName1, columnName2, operator):
    ruleId = 'CT001'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName1:'Input'})[['Input']]
    columnName2 += '_ref'
    mergedDF = inputTableDF.merge(refTableDF.add_suffix('_ref').drop_duplicates(columnName2, keep='first'), how='left', left_on=columnName1, right_on=columnName2)
    rulestatus = compareTwoColumnsCrossTable(mergedDF[:], columnName1, columnName2, operator)
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName1, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName1, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
#Check for presence of column 1 as substring of column 2 - CT002
def crossTableSubstringCheck(tableName, inputTableDF, refTableDF, columnName1, columnName2,):
    ruleId = 'CT002'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName1:'Input'})[['Input']]
    columnName2 += '_ref'
    mergedDF = inputTableDF.merge(refTableDF.add_suffix('_ref').drop_duplicates(columnName2, keep='first'), how='cross')
    mergedDF['RuleStatus'] = mergedDF.apply(lambda row: 'PASS' if str(row[columnName1]) in str(row[columnName2]) else None,axis=1)
    mergedDF.dropna(axis=0, subset=['RuleStatus'], inplace=True)
    mergedDF.drop_duplicates(columnName1, keep='first', inplace=True)
    DF = inputTableDF.astype('str').merge(mergedDF.astype('str'), how='left', on=columnName1)
    rulestatus = np.where(DF[columnName1] == 'nan', 'NULL', np.where(DF['RuleStatus'].isna(), 'FAIL', 'PASS'))
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName1, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName1, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
#Compare 2 columns based on a join key - CT003
def crossTableJoinKeyColumnCompare(tableName, table1, table2, columnName1, columnName2, key1, key2, operator):
    ruleId = 'CT003'
    execution_datetime = datetime.now().date()
    calculatedDF = pd.DataFrame(columns=order_of_columns)
    columnName2 += '_ref'
    key2 += '_ref'
    # mergedDF = table1.astype('str').merge(table2.add_suffix('_ref').drop_duplicates(key2, keep='first').dropna(subset=[key2]).astype('str'), how='left', left_on=key1, right_on=key2)
    mergedDF = table1.merge(table2.add_suffix('_ref').drop_duplicates(key2, keep='first').dropna(subset=[key2]), how='left', left_on=key1, right_on=key2)

    calculatedDF['Input'] = mergedDF[[columnName1, columnName2]].apply(lambda row: '|'.join(row.values.astype(str)), axis=1)
    # print(mergedDF)
    rulestatus = compareTwoColumnsCrossTable(mergedDF, columnName1, columnName2, operator)
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName1, ruleId, tableName, execution_datetime, rulestatus)               
    publishPrimary = publishPrimaryData(table1,calculatedDF, columnName1, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary


def get_weekend(x,y):
             
             if x is None:
                  return "NULL"
             elif x.__class__.__name__ in ['int','float']:
                    if math.isnan(x):
                       return 'NULL'
                    if y == 'weekday':
                            return 'PASS' if calendar.day_name[int(x)] not in ['Saturday','Sunday'] else 'FAIL'
                    else:
                            return 'FAIL' if calendar.day_name[int(x)] not in ['Saturday','Sunday'] else 'PASS' 
@st.cache_data
def DatecheckWeekend(tableName, inputTableDF, columnName):
     
    ruleId = 'DQ010'
    execution_datetime = datetime.now().date()
    inputTableDF[columnName]=inputTableDF[columnName].fillna('')
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    rulestatus = inputTableDF[columnName].dt.weekday.apply(lambda x: get_weekend(x,'weekend'))                   
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)
    calculatedDF.loc[calculatedDF['Input'] == 'None','Status'] = 'NULL'
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
def DatecheckFutureDate(tableName, inputTableDF, columnName):
     
    ruleId = 'DQ011'
    execution_datetime = datetime.now().date()
    inputTableDF[columnName]=inputTableDF[columnName].fillna('')
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']] 
    rulestatus = np.where(np.isnat(inputTableDF[columnName] ), 'NULL', np.where(inputTableDF[columnName] > datetime.now(), 'PASS', 'FAIL'))                     
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)
    calculatedDF.loc[calculatedDF['Input'] == 'None','Status'] = 'NULL'
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
def DatecheckWeekday(tableName, inputTableDF, columnName):
     
    ruleId = 'DQ012'
    execution_datetime = datetime.now().date()
    inputTableDF[columnName]=inputTableDF[columnName].fillna('')
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]

                
    rulestatus = inputTableDF[columnName].dt.weekday.apply(lambda x: get_weekend(x,'weekday'))                     
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)
    calculatedDF.loc[calculatedDF['Input'] == 'None','Status'] = 'NULL'
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary


@st.cache_data
def DatecheckNonFutureDate(tableName, inputTableDF, columnName):
     
    ruleId = 'DQ013'
    execution_datetime = datetime.now().date()
    inputTableDF[columnName]=inputTableDF[columnName].fillna('')
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    rulestatus = np.where(np.isnat(inputTableDF[columnName] ), 'NULL', np.where(inputTableDF[columnName] > datetime.now(), 'FAIL', 'PASS'))                  
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)
    calculatedDF.loc[calculatedDF['Input'] == 'None','Status'] = 'NULL'
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

@st.cache_data
def allowableKeywords(tableName, inputTableDF, columnName, arrayOfKeywords, notCaseSensitive):
    ruleId = 'DQ014'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    tempDF = pd.DataFrame()
    tempDF[columnName] = inputTableDF[columnName]
    tempDF[columnName] = np.where(inputTableDF[columnName].isna(),inputTableDF[columnName],inputTableDF[columnName].astype('str'))

    if notCaseSensitive:
         tempDF[columnName] = np.where(inputTableDF[columnName].isna(),inputTableDF[columnName],inputTableDF[columnName].astype('str').str.lower())
         arrayOfKeywords = arrayOfKeywords.lower()

    arrayOfKeywords = [arrayOfKeywords] if (arrayOfKeywords[-1] == ',' or arrayOfKeywords[0] == ',') else arrayOfKeywords.split(',')
    rulestatus = tempDF[columnName].apply(lambda x: 'NULL' if pd.isna(x) or x in ['nan','<na>','<NA>','NAN',None] or x is None else ('PASS' if (any(keyword in x for keyword in arrayOfKeywords) and (arrayOfKeywords[-1] != ',' or arrayOfKeywords[0] != ',' or arrayOfKeywords == [','])) else 'PASS' if ((arrayOfKeywords[-1] == ',' or arrayOfKeywords[0] == ',') and (x == arrayOfKeywords)) else 'FAIL'))
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)   
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary
     
@st.cache_data 
def PatternCheck(tableName, inputTableDF, columnName, operator, value):
    ruleId = 'DQ015'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    tempDF = pd.DataFrame()
    tempDF[columnName] = inputTableDF[columnName]
    tempDF[columnName] = np.where(pd.isna(inputTableDF[columnName]),inputTableDF[columnName],inputTableDF[columnName].astype('str'))
    tempDF[columnName] = generate_pattern(tempDF,columnName)
    tempDF[columnName] = tempDF[columnName].apply(lambda x : string_pattern(x) if x is not None or not pd.isna(x) else None)
    value = value.upper()
    value = string_pattern(value)
    if operator == 'Matching':
          rulestatus = tempDF[columnName].apply(lambda x: 'NULL' if x is None else 'PASS' if x == value else 'FAIL')
    else:
          rulestatus = tempDF[columnName].apply(lambda x: 'NULL' if x is None else 'PASS' if x != value else 'FAIL')
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)   
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

def validate_scalar_date(x):
    if x is None or pd.isna(x):
          return "NULL"
    else:
          try:
                x = str(x).replace(' ','').replace('.','-').replace('/','-')
                if len(x) >= 8:
                        
                        pattern = r'--'
                        
                        if len(re.findall(pattern, x))==0:
                             x = pd.to_datetime(x,format='mixed', infer_datetime_format=True )
                        else:
                             return 'FAIL'
                else:
                     return 'FAIL'
                
          except Exception as e:
                return 'FAIL'
          else:
                return 'PASS'

       
def DateValidationCheck(tableName, inputTableDF, columnName):
    ruleId = 'DQ016'
    execution_datetime = datetime.now().date()
    calculatedDF = inputTableDF.rename(columns = {columnName:'Input'})[['Input']]
    tempDF = pd.DataFrame()
    tempDF[columnName] = inputTableDF[columnName]
    rulestatus = tempDF[columnName].apply(lambda x: validate_scalar_date(x))
    calculatedDF, publishDF,publishDF2 = assignData(calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)   
    publishPrimary = publishPrimaryData(inputTableDF,calculatedDF, columnName, ruleId, tableName, execution_datetime, rulestatus)  
    return calculatedDF, publishDF,publishDF2,publishPrimary

    

