import streamlit as st

power_bi_embed_url = "YOUR_POWER_BI_EMBED_URL_HERE"

html_code = f"""
<div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden;">
  <iframe src="{power_bi_embed_url}" 
          style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;" 
          frameborder="0" allowfullscreen></iframe>
</div>
"""

st.markdown(html_code, unsafe_allow_html=True)
