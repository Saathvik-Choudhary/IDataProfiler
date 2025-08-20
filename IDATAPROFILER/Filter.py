import streamlit as st

def filter_dataframe(df,column_name,operator,values):
    """
    Filter a DataFrame based on specified columns, operators, and values.

    Args:
        df (pd.DataFrame): The input DataFrame to be filtered.
        column_name (list): List of column names to apply filters on.
        operator (list): List of operators (e.g., 'Like', 'In', '=', etc.).
        values (list): List of values to compare against.

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    for column1 , operator , compare_value in zip(column_name,operator,values):       
            if df[column1].dtype == 'object':
                if operator == 'Is None':
                    df = df[df[column1].isnull()]
                elif operator == 'Is Not None':
                    df = df.dropna(subset=column1)
                elif operator == 'Like':
                    boolean_series = df[column1].str.contains(compare_value,regex=True,na=False).values 
                    df = df[boolean_series]
                elif operator=='Not Like':
                    boolean_series = df[column1].str.contains(compare_value,regex=True,na=False).values
                    df = df[~boolean_series]
                elif operator == 'In':
                    boolean_series = df[column1].isin([value.strip() for value in compare_value.split(',')])
                    df = df[boolean_series]
                elif operator == 'Not In':
                    boolean_series = df[column1].isin([value.strip() for value in compare_value.split(',')])
                    df =  df[~boolean_series] 
                else:
                    df = df.query(f"`{str(column1)}` {operator} \'{(compare_value)}\'")

            # Handle datetime columns
            elif 'date' in str(df[column1].dtype):
                if operator == 'Is None':
                    df = df[df[column1].isnull()]
                elif operator == 'Is Not None':
                    df = df.dropna(subset=column1)
                elif operator == 'Like':
                    boolean_series = df[column1].astype('str').str.contains(compare_value,regex=True,na=False).values
                    df = df[boolean_series]
                elif operator=='Not Like':
                    boolean_series = df[column1].astype('str').str.contains(compare_value,regex=True,na=False).values
                    df = df[~boolean_series]
                elif operator == 'In':
                    boolean_series = df[column1].isin([(value.strip()) for value in compare_value.split(',')])
                    df = df[boolean_series]
                elif operator == 'Not In':
                    boolean_series = df[column1].isin([(value.strip()) for value in compare_value.split(',')])
                    df =  df[~boolean_series]
                else:
                    df = df.query(f"`{str(column1)}` {operator} \'{str(compare_value)}\'")

            # Handle numeric and other types
            else:
                if operator == 'Is None':
                    df = df[df[column1].isnull()]
                elif operator == 'Is Not None':
                    df = df.dropna(subset=column1)
                
                elif operator == 'Like':
                    boolean_series = df[column1].astype('str').str.contains(compare_value,regex=True,na=False).values
                    df = df[boolean_series]
                elif operator=='Not Like':
                    boolean_series = df[column1].astype('str').str.contains(compare_value,regex=True,na=False).values
                    df = df[~boolean_series]
                elif operator == 'In':
                    boolean_series = df[column1].isin([int(value.strip()) for value in compare_value.split(',')])
                    df = df[boolean_series]
                elif operator == 'Not In':
                    boolean_series = df[column1].isin([int(value.strip()) for value in compare_value.split(',')])
                    df =  df[~boolean_series]
                else:
                    df = df.query(f"`{str(column1)}` {operator} {(compare_value)}")

    return df