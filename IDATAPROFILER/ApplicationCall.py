# ApplicationCall.py handles the main navigations in the home page

import streamlit as st

from CoreLogic import home, ComprehensiveColumnProfiling
from RuleProfiling import BusinessRuleProfiling
from CrossTable import CrossTableProfiling
from JoinProfiling import JoinProfiling
#from Analytics import Visual_Analytics
#from Test_databricks import *
from SessionStateVar import clear_module_filters,clear_ccp_session_state

"""
Main function to control the navigation and display of different profilling pages.
It includes a sidebar for navigation and a logout button.
"""
def applicationHome():
    # Logout button in the sidebar
    Logout=st.sidebar.button("Logout")

    # Navigation menu in the sidebar
    menu = ["Home", "Comprehensive Column Profiling", "Business Rule Profiling", "Join Profiling", "Cross Table Profiling"]
    choice = st.sidebar.selectbox("Navigate", menu,on_change=clear_ccp_session_state)

    # Display the selected page based on the user's choice
    if choice == "Home":
        home()
    elif choice == "Comprehensive Column Profiling":
        ComprehensiveColumnProfiling()
    elif choice == "Business Rule Profiling":
        BusinessRuleProfiling()
    elif choice == 'Cross Table Profiling':
        CrossTableProfiling()
    elif choice == "Join Profiling":
        JoinProfiling()
    #elif choice == 'Exploratory Data Analysis':
        #ExploratoryDataAnalysis()
    # elif choice == 'Analytics':
    #     Visual_Analytics()
        
    # Handle logout action        
    if Logout:
        st.session_state.clear()
        st.experimental_rerun()
