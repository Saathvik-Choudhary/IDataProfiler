import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go 
from utility import *
import plotly.express as px
import time
import seaborn as sns
from sklearn.preprocessing import LabelEncoder
from streamlit_modal import Modal

def draw_plotly_graph(df,x_column,y_column,title,col):
        """
        Draw a Plotly bar chart in a Streamlit column.

        Parameters:
        df (DataFrame): Data to plot.
        x_column (str): Column name for x-axis.
        y_column (str): Column name for y-axis.
        title (str): Title of the chart.
        col (Streamlit column): Streamlit column to render the chart in.
        """
        fig,ax=plt.subplots()
        col.subheader(title)
        plot=px.bar(data_frame=df,x=x_column,y=y_column,width = df[x_column].nunique()*100,height=600)
        plot.update_layout(xaxis=dict(type='category'))
        col.plotly_chart(plot)
                          
@st.cache_data    
def compute_data_completeness(profile_df):
    """
    Compute completeness percentage for each column in the profile DataFrame.

    Parameters:
    profile_df (DataFrame): Profile summary with 'Non_Null_Count' and 'Total_Count'.

    Returns:
    DataFrame: Completeness percentages.
    """
    completeness_df = pd.DataFrame()
    #utilising the profile df results to compute the data completeness.
    completeness_df['Completeness(%)'] = (profile_df['Non_Null_Count'].astype('int')/profile_df['Total_Count'].astype('int'))*100
    return completeness_df

@st.cache_data  
def compute_data_uniqueness(profile_df):
                """
                Compute uniqueness percentage for each column in the filtered DataFrame.
 
                Parameters:
                profile_df (DataFrame): Profile summary with column metadata.
 
                Returns:
                DataFrame: Uniqueness percentages.
                """
                uniqueness_df = pd.DataFrame()
               
                #following for loop id used to remove the leading and trailing space from the data.
 
                test_df = profile_df[:]
 
                #find uniqueness for each column with respect to non null counts present in each column,
                def fun(row):
                      return round(int(row[7])*100/int(row[2]),2) if int(row[2]) != 0 else 0
 
                test_df.reset_index(inplace=True)
 
 
                uniqueness_df['Column_Name'] = test_df['Column_Name']
                #finding uniqueness for each column.
                uniqueness_df['Uniqueness(%)'] = test_df.apply(fun,axis=1)
                return uniqueness_df

def create_frequency_dataframe():
                """
                Create a frequency distribution DataFrame for all columns in the filtered DataFrame.

                Returns:
                DataFrame: Frequency distribution with column name, data value, and count.
                """
                frequency_df = pd.DataFrame()
                
                
                for column in st.session_state.filtered_df.columns:
                    df=pd.DataFrame(st.session_state.filtered_df[column].value_counts()).reset_index()
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

def column_select_box():
    """Set session state to indicate column selection."""
    st.session_state.column = 1

def dataset_select_box():
    """Set session state to indicate dataset selection."""
    st.session_state.data = 1

def table_level_stat_download():
     """Set session state to indicate table-level stats download."""
     st.session_state.table = 1
     
def set_down_profile():
     """Set session state to enable profile download."""
     st.session_state.table = 1
     st.session_state.set_download_profile = 1
   
def datatypes(x):
       
        """
        Convert pandas dtype to simplified string representation.

        Parameters:
        x (str): Pandas dtype string.

        Returns:
        str: Simplified type ('string', 'int', 'float', or original).
        """
        if str(x).lower().startswith('object'):
                return 'string'

        if str(x).lower().startswith('int') :
                return 'int'

        if str(x).lower().startswith('float'):
                return 'float'

        return str(x)

def table():
        """
        Display table-level profiling summary and visual insights in Streamlit.
        """ 
        if 'new_dup_src' in st.session_state:
              del st.session_state.new_dup_src
        
        if 'dataframe' in st.session_state:
                st.session_state.table_level = 1 

                if 'column' in st.session_state:
                    del st.session_state.column

                # df=st.session_state.dataframe
                st.header("Column Profile Summary Statistics")
                st.subheader(f"Source Data: {st.session_state.file_name}")
                st.write(f"**Number of Rows profiled: {st.session_state.filtered_df.shape[0]}**")
                st.write(f"**Number of Columns profiled: {st.session_state.profile_df.shape[0]}**")
                
                st.markdown("""<hr style="height:2px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

                profile_df = st.session_state.profile_df
                profile_df['DataType'] = np.where(profile_df['DataType'] == 'object','string',profile_df['DataType'])
                
                #display the profile result 
                st.dataframe(profile_df.drop(columns=['Leading_Trailing_space_presence']))

                st.header("Profiling Insights")
                st.dataframe(st.session_state.insight_df[['TableName','ColumnName','Description','Dimension','Execution_date']])
                st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
                col1,col2=st.columns(2)

                #display the datatype distribution of dataframe columns.
                try:  
                    fig = go.Figure(data=[go.Pie(labels=list(map(lambda x: datatypes(x),st.session_state.filtered_df.dtypes.value_counts().index.values)),
                                            values=list(st.session_state.filtered_df.dtypes.value_counts().values))])
                    fig.update_layout(title='')
                    fig.update_traces(textposition='inside', textinfo='percent+label', marker=dict( line=dict(color='#000000', width=0.5)))
                    col1.subheader("Datatype Distribution")
                    col1.plotly_chart(fig)
                except Exception as e:
                       st.error(e)

                st.write("")
                col1,col2=st.columns(2)

                try:
                    dimension_df = pd.merge(st.session_state.completeness_df.reset_index(),st.session_state.uniqueness_df.reset_index(),left_on='Column_Name',right_on='Column_Name')

                    df=dimension_df.iloc[:,:]
                    df['Uniqueness']=dimension_df['Uniqueness(%)'].map(lambda x: '=100%' if x==100 else '<100%')
                    df['Completeness']=dimension_df['Completeness(%)'].map(lambda x: '=100%' if x==100 else '=0%' if x==0 else '<100%')
                    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
                    
                    st.subheader("Completeness Profile")
                    col1,col2 = st.columns(2)
                    fig = px.bar(df.sort_values(by='Completeness(%)',ascending=False),x='Column_Name',y='Completeness(%)',color='Completeness',color_discrete_sequence=['#FAAE2A','#9C9B98','red'])
                    st.plotly_chart(fig)
                except Exception as e:
                       st.error(e)

                col1,col2 = st.columns(2)
                

                #show leading trailing space count and blank space count.
                try:

                    newdf = profile_df.query(' Leading_Trailing_space_count > 0 ').sort_values(by='Leading_Trailing_space_count',ascending=False).reset_index()
                    if newdf.shape[0]!=0:
                            col1.subheader("Columns having Leading Trailing Spaces")
                            fig = px.bar(newdf,x=newdf['Column_Name'],y='Leading_Trailing_space_count',title='',width=newdf.shape[0]*140,color_discrete_sequence=['#D5090B'])
                            col1.plotly_chart(fig)
                except Exception as e:
                       st.error(e)
                
                try:
                    newdf = profile_df.query(' Blank_Count > 0 ').sort_values(by='Blank_Count',ascending=False).reset_index()

                    if newdf.shape[0]!=0:
                            col2.subheader("Columns having Blank spaces")
                            fig = px.bar(newdf,x=newdf['Column_Name'],y='Blank_Count',title='',width=newdf.shape[0]*140,color_discrete_sequence=['#D5090B'])
                            col2.plotly_chart(fig)
                except Exception as e:
                       st.error(e)
                
                st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
                

                #showing the uniqueness of dataframe columns in chart.
                try:
                    st.subheader("Uniqueness Profile")
                    fig = px.bar(df.sort_values(by='Uniqueness(%)',ascending=False),x='Column_Name',y='Uniqueness(%)',color='Uniqueness',color_discrete_sequence=['#71EB7F','#10E3E3'])
                    st.plotly_chart(fig)
                except Exception as e:
                       st.error(e)

                st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
                
                #comparing the completeness and uniqueness across the dataframe columns.
                try:
                    st.subheader("Completeness vs Uniqueness")

                    fig = go.Figure(data=[go.Bar(x=st.session_state.completeness_df.index,y=st.session_state.completeness_df['Completeness(%)'],marker_color='#71EB7F',name='Completeness'),
                                        go.Scatter(x=st.session_state.completeness_df.index,y=st.session_state.uniqueness_df['Uniqueness(%)'],marker_color='#663300',name='Uniqueness')])
                    
                    fig.update_layout(xaxis_title='Column Name',yaxis_title='Completeness and Uniqueness (%)')
                    st.plotly_chart(fig)
                except Exception as e:
                       st.error(e)

                st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

                col1,col2 = st.columns(2)

                final_table_stat = pd.merge(profile_df.reset_index(),df,left_on='Column_Name',right_on='Column_Name')

                final_table_stat.drop(['Completeness','Uniqueness'],axis=1,inplace=True)
                final_table_stat.insert(6,'Completeness (%)',final_table_stat['Completeness(%)'])
                final_table_stat.insert(10,'Uniqueness (%)',final_table_stat['Uniqueness(%)'])
                final_table_stat.drop(['Completeness(%)','Uniqueness(%)','Leading_Trailing_space_presence'],axis=1,inplace=True)
                final_table_stat.insert(0,'Data_Source_Name',st.session_state.file_name)
                final_table_stat.insert(0,'Execution_time',str(st.session_state.profile_time))

                col1,col2 = st.columns(2)

                
                try:
            
                    frequency_df = pd.DataFrame()
                    p_df = pd.DataFrame()
                    p_df = create_publish_pattern_dataframe(st.session_state.pattern_df,profile_df)
                    frequency_df = create_publish_frequency_df(frequency_df,profile_df)
                except Exception as e:
                       st.error(e)

                p_df.insert(0,'Source Name',st.session_state.file_name)
                p_df.insert(0,'Execution_time',str(st.session_state.profile_time))
                frequency_df.insert(0,'Source Name',st.session_state.file_name)
                frequency_df.insert(0,'Execution_time',str(st.session_state.profile_time))

                col1,col2 = st.columns(2)

                p_df.rename(columns={'Execution_time':'Execution_time','Source Name':'TableName','Column':'ColumnName','Pattern':'Pattern','Values':'Count'},inplace=True)
                frequency_df.rename(columns={'Execution_time':'Execution_time','Source Name':'TableName','Column':'ColumnName','Data':'Data','Count':'Count'},inplace=True)

                datatype_change = {'Data_Source_Name':'TableName','Column_Name':'ColumnName','Completeness (%)':'Completeness%','Uniqueness (%)':'Uniqueness%'}

                final_table_stat.rename(columns=datatype_change,inplace=True)
                final_table_stat.drop(['index'],axis=1,inplace=True)

                final_table_stat['Completeness%'] =  final_table_stat['Completeness%'].astype('str')
                final_table_stat['Uniqueness%'] =  final_table_stat['Uniqueness%'].astype('str')
                final_table_stat['Blank_Count'] = final_table_stat['Blank_Count'].astype('str')
                final_table_stat['Leading_Trailing_space_count'] = final_table_stat['Leading_Trailing_space_count'].astype('str')
                
                

                if col1.button("Download/ Publish Profiling Reports",on_click=set_down_profile) or 'set_download_profile' in st.session_state:
                                
                                col1,col2,col3 = st.columns(3)
   
                                s = f"""
                                <style>
                                div.stButton > download_button:first-child {{ border: 5px solid blue; border-radius:20px 20px 20px 20px; border-color:'black'}}
                                <style>
                                """
                                
                                st.markdown(s, unsafe_allow_html=True)
                                #Download the profilng result for offline analysis.
                                
                                col1.download_button("Export Frequency Distribution",data=frequency_df.to_csv(index=False).encode('utf-8'),file_name='Frequency_Distribution.csv',on_click=table_level_stat_download,use_container_width=True,type='primary')

                                col1.download_button("Export Pattern Distribution",data=p_df.to_csv(index=False).encode('utf-8'),file_name='Pattern_Distribution.csv',on_click=table_level_stat_download,use_container_width=True,type='primary')

                                col1.download_button("Export Summary Report",data=final_table_stat.to_csv(index=False).encode('utf-8'),file_name='Table_level_statistics.csv',on_click=table_level_stat_download,use_container_width=True,type='primary')
                                
                                col1.download_button("Export DQ Insights Report",data=st.session_state.insight_df[['TableName','ColumnName','Description','Dimension','Execution_date']].to_csv(index=False).encode('utf-8'),file_name='DQInsights.csv',on_click=table_level_stat_download,use_container_width=True,type='primary')
                                timetaken = None

                                #publish profiling result to database tables.
                             
                                if col3.button(label='Publish Frequency Distribution to Database', key='exportFrequencyToSQL', on_click=table_level_stat_download, use_container_width=True):
                                     
                                     try:
                                            write_to_sql(frequency_df, 'IDATAPROFILE_VALUE_FREQ_STATISTICS')
                                     except Exception as e:
                                            st.error(e)
                                     else:
                                            modal = Modal(key="IDATAPROFILE_VALUE_FREQ_STATISTICS", title='', max_width=1000)
                                            with modal.container():
                                                 st.success("Successfully Published Frequency Distribution to Database")
                                                 time.sleep(200)
                                if col3.button(label='Publish Pattern Distribution to Database', key='exportPatternToSQL', on_click=table_level_stat_download, use_container_width=True):
                                     
                                     try:
                                            write_to_sql(p_df, 'IDATAPROFILE_PATTERN_FREQ_STATISTICS')
                                     except Exception as e:
                                            st.error(e)
                                     else:
                                            modal = Modal(key="IDATAPROFILE_PATTERN_FREQ_STATISTICS", title='', max_width=1000)
                                            with modal.container():
                                                 st.success("Successfully Published Pattern Distribution to Database")
                                                 time.sleep(200)
                                     
                                if col3.button(label='Publish Summary Report to Database', key='exportSummaryToSQL', on_click=table_level_stat_download, use_container_width=True):
                                     
                                     try:
                                            write_to_sql_summary(final_table_stat, 'IDATAPROFILE_SUMMARY_STATISTICS')
                                     except  Exception as e:
                                            st.error(e)
                                     else:
                                            modal = Modal(key="IDATAPROFILE_SUMMARY_STATISTICS", title='', max_width=1000)
                                            with modal.container():
                                                 st.success("Successfully Published Summary Report to Database")
                                                 time.sleep(200)
                                if col3.button(label='Publish Profiling Insights to Database', key='exportINSIGHTSToSQL', on_click=table_level_stat_download, use_container_width=True):
                                     
                                     try:
                                            write_to_sql(st.session_state.insight_df, 'IDATAPROFILE_PROFILE_INSIGHTS')
                                     except  Exception as e:
                                            st.error(e)
                                     else:
                                            modal = Modal(key="IDATAPROFILE_PROFILE_INSIGHTS", title='', max_width=1000)
                                            with modal.container():
                                                 st.success("Successfully Published Profile Insights to Database")
                                                 time.sleep(200)
                if 'table' in st.session_state:
                        del st.session_state.table 
           
        else:
                st.error("Please select the file...")

def column():
            """
            Display column-level profiling summary and visual insights in Streamlit.
            """
            if 'new_dup_src' in st.session_state:
              del st.session_state.new_dup_src
     
            if 'table' in st.session_state:
                 del st.session_state.table

            if 'dataframe' in st.session_state:
                st.session_state.table_level=2
                df=st.session_state.filtered_df

                if len(st.session_state.selected_columns)!=0:
                    st.session_state.filtered_df = df[st.session_state.selected_columns]
                
                else:

                    if 'filtered_df' not in st.session_state:
                        st.session_state.filtered_df = df
                    else:
                         st.session_state.filtered_df=st.session_state.filtered_df
                profile_df = st.session_state.profile_df
                try:
                    pass
                    # pattern_df =  create_pattern_dataframe()
                
                    # completeness_df = compute_data_completeness(profile_df)
                
                    # uniqueness_df = compute_data_uniqueness(profile_df)
                except Exception as e:
                       st.error(e)
         
                dimension_df = pd.merge(st.session_state.completeness_df.reset_index(),st.session_state.uniqueness_df.reset_index(),left_on='Column_Name',right_on='Column_Name')

                final_table_stat = pd.merge(profile_df.reset_index(),dimension_df,left_on='Column_Name',right_on='Column_Name')

                final_table_stat.set_index(keys='Column_Name',inplace=True)

                #select the column to view the result in more details.
                column=st.sidebar.selectbox('**Select the Column**',options=list(st.session_state.filtered_df.columns),on_change=column_select_box)
                st.header("Column Profile Result")
               
                stat_df = pd.Series(data=[column,datatypes(final_table_stat.loc[column]['DataType']),f"{round(final_table_stat.loc[column]['Completeness(%)'],2)}%",f"{round(final_table_stat.loc[column]['Uniqueness(%)'],2)}%",f"{final_table_stat.loc[column]['Leading_Trailing_space_presence']}"],index=['Column Name','Datatype','Completeness','Uniqueness','Contains Leading Trailing Spaces'],name='')

                st.dataframe(stat_df)
                
                if column:
                    df_filter=(profile_df.filter(items=[column],axis=0))
                    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
                    
                    col1,col2 = st.columns(2)
                    
                    #display count profile result for selected column.
                    col1.subheader("Counts Profile")

                    col1.dataframe(df_filter.loc[column].loc[['Total_Count','Null_Count','Blank_Count','Non_Null_Count','Distinct_Count','Unique_Count','Leading_Trailing_space_count']])
                    
                    #display range profile result for selected column.
                    col2.subheader("Range Profile")

                    col2.dataframe(df_filter.loc[column].loc[['Min_Length','Max_Length','Min_Value','Max_Value','Mean','Median','Standard_Deviation']])
               
                    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

                    col1,col2 = st.columns(2)
                   
                   #display top 10 value frequency.
                    col1.subheader("Value Frequency Profile")
                    frequency_df = pd.DataFrame()
                    frequency_df[column] = list(map(str,st.session_state.filtered_df[column].value_counts().head(10).index))
                    frequency_df['Count'] = st.session_state.filtered_df[column].value_counts().head(10).values
                    frequency_df['Percentage(%)'] = list(map(lambda x: round((x*100)/st.session_state.filtered_df.shape[0],2),list(frequency_df['Count'].values)))
                    col1.dataframe(frequency_df.style.format(thousands='',precision=2),hide_index=True)

                    draw_plotly_graph(frequency_df,column,'Count','Frequency Distribution',col2)
                   
                    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" />""", unsafe_allow_html=True)
                    col1,col2 = st.columns(2)
                    

                    #calculate the lenght of each column of dataframe.
                    length_df = pd.DataFrame()
                    
                    #length distribution top 5 values 
                    length_df[column] = list(map(lambda x: str(x),st.session_state.length_df[column].value_counts().head(5).index.values))
                    length_df['Count'] = st.session_state.length_df[column].value_counts().head(5).values
                    draw_plotly_graph(length_df,column,'Count','Length Distribution top 5 values',col1)
                    
                    #length distribution botton 5 values
                    length_df[column] = list(map(lambda x: str(x),st.session_state.length_df[column].value_counts().tail(5).index.values))
                    length_df['Count'] = st.session_state.length_df[column].value_counts().tail(5).values
                    draw_plotly_graph(length_df,column,'Count','Length Distribution bottom 5 values',col2)
                    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

                    col1,col2 = st.columns(2)
                    col1.subheader("Pattern Frequency Profile")
                   
                    #calculating the pattern for selected column of the dataframe.
                    patterns = pd.DataFrame(st.session_state.pattern_df[column].value_counts().head(5))
                    patterns.reset_index(inplace=True)

                    patterns['Patterns']=list(map(string_pattern,patterns[column].values))
                    del patterns[column]
                    patterns['Count'] = patterns['count'].values
                    del patterns['count']
                    patterns['Percentage(%)']=list(map(lambda x: round((x*100)/st.session_state.pattern_df.shape[0],2),list(patterns['Count'].values)))                    
                    
                    col1.dataframe(patterns,hide_index=True)
                    col1.text(" 'X' Represent Alphabet letter [A-Za-z]")
                    col1.text(" '9' Represent Digit[0-9]")
                    col1.text(" 'S' Represent Special character")
                    col1.text(" 'B' Represent Space character")
                    col1.text(" 'None' Represents No value(NULL)")
                    
                    fig = px.bar(patterns,x='Patterns',y='Count',width = patterns['Patterns'].nunique()*120 )
                    col2.subheader("Pattern Distribution")
                    col2.plotly_chart(fig)

                    st.markdown("""<hr style="height:5px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)
                    if 'column' in st.session_state:
                        del st.session_state.column  
            else:
                st.error("Please select the file...")