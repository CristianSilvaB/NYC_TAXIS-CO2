import streamlit as st
import streamlit.components.v1 as components

def app():
    st.title("Visualización de Power BI")
    powerbi_url = "https://app.powerbi.com/view?r=eyJrIjoiZGNmMzIwODQtYzY1Ni00MjhhLTkzOTQtZTFjNGVhMzcyNjg3IiwidCI6IjdlOTE2OGM3LWZiYmYtNDQ3OS1iODBlLWYzMmI2ODM4ZmJkZCIsImMiOjR9"  
    components.iframe(powerbi_url, width=800, height=600)


