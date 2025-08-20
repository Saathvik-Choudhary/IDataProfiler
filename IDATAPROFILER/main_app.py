# main_app.py

import streamlit as st
from login import login_page
from ApplicationCall import applicationHome


# Set the configuration for the Streamlit app
st.set_page_config(
    page_title="IDAtaProfiler",  # Title of the app
    page_icon=":bar_chart:",     # Icon for the app
    layout="wide",               # Layout setting for the app
    initial_sidebar_state="expanded"  # Initial state of the sidebar
)

# Main function to control the flow of the application.
# It checks the session state to determine which page to display.
def main():
    if getattr(st.session_state, "is_authenticated", False):
        # If the user is authenticated, show application home page
        applicationHome()
    else:
        # If the user is not authenticated, show the login page 
        login_page()

if __name__ == "__main__":
    main()