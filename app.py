import streamlit as st

# Configurar el título de la aplicación
st.set_page_config(page_title="Predicción de Demanda de Taxis en NYC", layout="wide")

st.title("Demanda de Taxis en NYC")
st.sidebar.title("Navegación")

# Selección de página
page = st.sidebar.radio("Ir a", ["Página de Predicción", "Visualización de Power BI"])

# Importar las páginas
if page == "Página de Predicción":
    import pages.prediction as prediction
    prediction.app()
elif page == "Visualización de Power BI":
    import pages.powerbi as powerbi
    powerbi.app()






