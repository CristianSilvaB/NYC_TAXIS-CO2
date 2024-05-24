import streamlit as st

power_bi_embed_url = "https://app.powerbi.com/reportEmbed?reportId=dcc01800-b02e-4efc-9b26-629c4e142bf4&autoAuth=true&ctid=7e9168c7-fbbf-4479-b80e-f32b6838fbdd"

html_code = f"""
<div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden;">
  <iframe src="{power_bi_embed_url}" 
          style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;" 
          frameborder="0" allowfullscreen></iframe>
</div>
"""

st.markdown(html_code, unsafe_allow_html=True)
