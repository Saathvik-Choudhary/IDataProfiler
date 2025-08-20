# login_page.py
import streamlit as st
from PIL import Image
import json
import os

# Retrieve credentials from environment variable
cred_json = os.getenv("creds_json")

"""
Authenticate the user based on the provided username and password

Args:
    username (str): The username entered by the user.
    password (str): The password entered by the user.

Returns:
    bool: True if authentication is successful, False otherwise.
"""
def authenticate(username, password):
   
    # path_to_json = "Rapid Response Reliability/credentials.json"

    # with open(path_to_json, "r") as handler:
    # Load credentials from JSON
    info = json.loads(cred_json)

    # Check if the provided credentials match any in the JSON data
    for credentials in info.get("Credentials"):
        if credentials["Username"] == username and credentials["Password"] == password:
            return True
                
    return False                  

"""
Display the login page with input feilds for username and password.
Authentiucate the user upon clicking the login button.
"""
def login_page():
    # Load and display background image
    image = Image.open(r"IDATAPROFILER/static/bg2.jpg")
    st.image(image, caption=None, width = 1000)
    
    # Labels for the input fields
    label_text_usr = "**Username**"
    label_text_pwd = "**Password**"

    # Input fields for username and password
    username = st.text_input(label=label_text_usr, max_chars=50, key="username_input")
    password = st.text_input(label=label_text_pwd, type="password", max_chars=20, key="password_input")

    # Login button
    if st.button("Login"):
        # Trigger authentication logic
        if authenticate(username, password):
            st.success("Login successful!")
            st.session_state.is_authenticated = True  # Set authentication status
            # Explicitly rerun the app to update the displayed page
            st.rerun()
        else:
            st.error("Invalid credentials. Please try again.")