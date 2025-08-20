import streamlit as st

def cancel_collibra_integration():
    """
    Remove Collibra-related session state variables if they exist.
    """
    if 'collibra' in st.session_state:
        del st.session_state.collibra
    
    if 'collibra_crosstable' in st.session_state:
        del st.session_state.collibra_crosstable

def clear_module_filters():
    """
    Placeholder function to clear module-specific filters.
    """
    pass

def clear_filters1():
    """
    Clear filter 1-related session state variables.
    """
    if 'col_name_dict1'  in st.session_state:
        del st.session_state.col_name_dict1

    if 'col_operator_dict1' in st.session_state:
        del st.session_state.col_operator_dict1

    if 'col_value_dict1'  in st.session_state:
        del st.session_state.col_value_dict1

    if 'rule_count1' in st.session_state:
        del st.session_state.rule_count1
    
    if 'filter1'  in st.session_state:

        del st.session_state.filter1

def clear_filters2():
    """
    Clear filter 2-related session state variables.
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

def clear_filters3():
    """
    Clear filter 3-related session state variables.
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

def clear_filters4():
    """
    Clear filter 4-related session state variables.
    """
    if 'col_name_dict4'  in st.session_state:
        del st.session_state.col_name_dict4

    if 'col_operator_dict4' in st.session_state:
        del st.session_state.col_operator_dict4

    if 'col_value_dict4'  in st.session_state:
        del st.session_state.col_value_dict4

    if 'rule_count4' in st.session_state:
        del st.session_state.rule_count4
    
    if 'filter4'  in st.session_state:

        del st.session_state.filter4

def clear_filters5():
    """
    Clear filter 5-related session state variables.
    """
    if 'col_name_dict5'  in st.session_state:
        del st.session_state.col_name_dict5

    if 'col_operator_dict5' in st.session_state:
        del st.session_state.col_operator_dict5

    if 'col_value_dict5'  in st.session_state:
        del st.session_state.col_value_dict5

    if 'rule_count5' in st.session_state:
        del st.session_state.rule_count5
    
    if 'filter5'  in st.session_state:

        del st.session_state.filter5

def clear_ccp_session_state():
    """
    Clear all session state variables related to CCP (Cross Comparison Profiling) and filters.
    """
    if 'src_select_widget_change_eda_source'  in st.session_state:
            del st.session_state.src_select_widget_change_eda_source
    if 'src_select_widget_change_reference_CTP'  in st.session_state:
            del st.session_state.src_select_widget_change_reference_CTP

    
    if 'src_select_widget_change_source_CTP'  in st.session_state:
            del st.session_state.src_select_widget_change_source_CTP

    
    if 'src_select_widget_change_source_JOIN'  in st.session_state:
            del st.session_state.src_select_widget_change_source_JOIN 

    
    if 'src_select_widget_change_reference_JOIN' in st.session_state:
            del st.session_state.src_select_widget_change_reference_JOIN

    if 'src_select_widget_change_CCP'  in st.session_state:
            del st.session_state.src_select_widget_change_CCP

    if 'src_select_widget_change_BRP' in st.session_state:
            del st.session_state.src_select_widget_change_BRP
        
    if 'new_dup_src' in st.session_state:
        del st.session_state.new_dup_src

    if 'time_taken' in st.session_state:
        del st.session_state.time_taken

    if 'enable_button' in st.session_state:
        del st.session_state.enable_button
    
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

    clear_filters1()
    clear_filters2()
    clear_filters3()
    clear_filters4()
    clear_filters5()
    cancel_collibra_integration()

def reset():
     """
     Clear all relevant session state variables used across various profiling modules.
     """
     if 'original_dataframe' in st.session_state:
        del st.session_state.original_dataframe
        
     if 'new_dup_src' in st.session_state:
         del st.session_state.new_dup_src
     
     if 'common_data' in st.session_state:
         del st.session_state.common_data

     if 'file_name' in st.session_state:
         del st.session_state.file_name
         
     if 'primary_columns' in st.session_state:
        del st.session_state.primary_columns

     if 'enable_button' in st.session_state:
         del st.session_state.enable_button

     if 'dataframe' in st.session_state:
          del st.session_state.dataframe

     if 'run_profile' in st.session_state:
        del st.session_state.run_profile

     if 'filter' in st.session_state:
         del st.session_state.filter

     if 'set_download_profile' in st.session_state:
         del st.session_state.set_download_profile

     if 'profile_df' in st.session_state:
         del st.session_state.profile_df

     if 'dataset' in st.session_state:
        del st.session_state.dataset

     if 'modal' in st.session_state:
         del st.session_state.modal

     if 'close_modal' in st.session_state:
        del st.session_state.close_modal
     
     if 'dict'  in st.session_state:
        del st.session_state.dict

     if 'dicttype' in st.session_state:
        del  st.session_state.dicttype
     
     if 'show_datatype_view' in st.session_state:
         del st.session_state.show_datatype_view
    
     if 'time_taken' in st.session_state:
         del st.session_state.time_taken

     if 'col_name_dict'  in st.session_state:
        del  st.session_state.col_name_dict

     if 'col_operator_dict' in st.session_state:
        del st.session_state.col_operator_dict

     if 'col_value_dict' in st.session_state:
        del st.session_state.col_value_dict 

     if 'rule_count' in st.session_state:
        del st.session_state.rule_count 
    
     if 'filter' in st.session_state:

        del st.session_state.filter
     
     if 'apply_filter' in st.session_state:
        del  st.session_state.apply_filter

     if 'update_dataframe' in st.session_state:
         del st.session_state.update_dataframe
     
     if 'modal_set1' in st.session_state:
         del st.session_state.modal_set1

     if 'completeness_df' in st.session_state:
         del st.session_state.completeness_df
     if 'uniqueness_df' in st.session_state:
         del st.session_state.uniqueness_df
        
     if 'pattern_df' in st.session_state:
         del st.session_state.pattern_df

     st.session_state.reset = 1
       
def reset_dataframe():
    """
    Reset session state variables related to the main business rule profiling dataframe.
    """
    if 'primary_columns' in st.session_state:
        del st.session_state.primary_columns

    if 'business_rule_data' in st.session_state:
          del st.session_state.business_rule_data

    if 'col_name_dict1'  in st.session_state:
        del st.session_state.col_name_dict1

    if 'col_operator_dict1' in st.session_state:
        del st.session_state.col_operator_dict1

    if 'col_value_dict1'  in st.session_state:
        del st.session_state.col_value_dict1

    if 'rule_count1' in st.session_state:
        del st.session_state.rule_count1
    
    if 'filter1'  in st.session_state:

        del st.session_state.filter1

def reset_crosscolumn_df1():
    """
    Reset session state variables related to the first cross-column profiling dataframe.
    """
    if 'primary_columns' in st.session_state:
        del st.session_state.primary_columns

    if 'cross_column_df1' in st.session_state:
          del st.session_state.cross_column_df1

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

def reset_crosscolumn_df2():
    """
    Reset session state variables related to the second cross-column profiling dataframe.
    """
    if 'cross_column_df2' in st.session_state:
          del st.session_state.cross_column_df2

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

def reset_joinprofile_df1():
    """
    Reset session state variables related to the first join profile dataframe.
    """
    if 'cross_column_df4' in st.session_state:
          del st.session_state.cross_column_df4

    if 'col_name_dict4'  in st.session_state:
        del st.session_state.col_name_dict4

    if 'col_operator_dict4' in st.session_state:
        del st.session_state.col_operator_dict4

    if 'col_value_dict4'  in st.session_state:
        del st.session_state.col_value_dict4

    if 'rule_count4' in st.session_state:
        del st.session_state.rule_count4
    
    if 'filter4'  in st.session_state:

        del st.session_state.filter4

def reset_joinprofile_df2():
    """
    Reset session state variables related to the second join profile dataframe.
    """
    if 'cross_column_df5' in st.session_state:
          del st.session_state.cross_column_df5

    if 'col_name_dict5'  in st.session_state:
        del st.session_state.col_name_dict5

    if 'col_operator_dict5' in st.session_state:
        del st.session_state.col_operator_dict5

    if 'col_value_dict5'  in st.session_state:
        del st.session_state.col_value_dict5

    if 'rule_count5' in st.session_state:
        del st.session_state.rule_count5
    
    if 'filter5'  in st.session_state:

        del st.session_state.filter5