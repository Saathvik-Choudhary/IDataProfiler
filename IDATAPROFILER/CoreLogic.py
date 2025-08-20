# CoreLogic.py sets up page layout for the frontend and links to the Column profiling page

import streamlit as st
from PIL import Image
import base64
from Home import *
from utility import scroll_up_button

"""
Display the home page of the IDATAProfiler application.
"""
def home():
     
    # Set the maximum width of the main content area
    st.markdown(
        """
        <style>
            .main {
                max-width: 2000px;  /* Set your desired width */
            }
        </style>
        """,
        unsafe_allow_html=True
    )
    #col1, col2 = st.columns([1,1])
    
    # Load and display the welcome image
    image = Image.open(r"IDATAPROFILER/static/DQ.jpg")
    st.markdown("## Welcome!! \n ### You're One Step Closer to Unveiling Your Data Insights!")
    st.image(image, caption=None, width = 700)

    # Display introductory text about IDATAProfiler
    st.markdown("###### Introducing IDATAProfiler, an open-source Python based Azure web application to enable data teams to achieve comprehensive column profiling and business rule profiling across diverse datasets.")
    st.markdown("###### Offering automatic generation of comprehensive column summaries with key statistical metrics and visualizations, IDATAProfiler enables quick insights into data distributions and patterns.")
    st.markdown("###### Also, users can define commonly used custom validation rules, to understand data consistency, completeness, Uniqueness and adherence to standards. \n ###### The tool boasts a user-friendly interface, extensibility for custom metrics and rules, and connectivity to disparate data sources. Whether you're a seasoned data professional or a beginner, IDATAProfiler is designed for efficient accesibility and performance. Embracing an open-source community, it evolves continuously, making it an essential resource for conducting data profiling, which is the first step towards enhancing data quality in any data analytics or data science project.")

    """
    Embed a PDF file as a clickable link in the Streamlit app.
    
    Args:
        pdf_file_path (str): Path to the PDF file.
        link_text (str): Text to display for the link.
    """
    def embed_pdf_link(pdf_file_path, link_text):
        with open(pdf_file_path, "rb") as pdf_file:
            pdf_base64 = base64.b64encode(pdf_file.read()).decode("utf-8")
            pdf_embed = f'<a href="data:application/pdf;base64,{pdf_base64}" target="_blank" rel="noopener noreferrer">{link_text}</a>'
            st.markdown(pdf_embed, unsafe_allow_html=True)

    with st.expander("Read more about IDAtaProfiler"):
        # Example usage
        image_paths = [r"IDATAPROFILER/static/About1.jpg", r"IDATAPROFILER/static/About2.jpg"]
        image_gap = 20  # Adjust as needed

        for i, image_path in enumerate(image_paths):
            image = Image.open(image_path)
            st.image(image, caption=f"Image {i + 1}", use_column_width=True)

            # Add a vertical gap between images
            if i < len(image_paths) - 1:
                st.markdown(f"<p style='margin-bottom: {image_gap}px'></p>", unsafe_allow_html=True)

    # Add a scroll-up button
    scroll_up_button('welcome')

"""
Display the comprehensive column profiling page.
"""
def ComprehensiveColumnProfiling():
    Home()