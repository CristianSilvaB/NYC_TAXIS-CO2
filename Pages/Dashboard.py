import streamlit as st

power_bi_embed_url = "https://app.powerbi.com/view?r=eyJrIjoiZGNmMzIwODQtYzY1Ni00MjhhLTkzOTQtZTFjNGVhMzcyNjg3IiwidCI6IjdlOTE2OGM3LWZiYmYtNDQ3OS1iODBlLWYzMmI2ODM4ZmJkZCIsImMiOjR9"

html_code = f"""
<div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden;">
  <iframe src="{power_bi_embed_url}" 
          style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;" 
          frameborder="0" allowfullscreen></iframe>
</div>
"""

st.markdown(html_code, unsafe_allow_html=True)
