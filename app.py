import streamlit as st
import pandas as pd
import joblib
from datetime import datetime

# Cargar el modelo entrenado
model = joblib.load('./ML/taxi_demand_model.joblib')

# Definir la función predict_demand
def predict_demand(hora_inicio, dia_semana, pickup_borough, servicio):
    input_data = pd.DataFrame({
        'hora_inicio': [hora_inicio],
        'dia_semana': [dia_semana],
        'Pickup_borough': [pickup_borough],
        'Servicio': [servicio]
    })
    prediction = model.predict(input_data)
    return int(round(prediction[0]))

# Configurar el título de la aplicación
st.title("Predicción de Demanda de Taxis en NYC")

# Configurar los controles de entrada
hora_inicio = st.slider("Hora de inicio (0-23):", 0, 23, 10)
dia_semana = st.selectbox("Día de la semana:", 
                          ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"],
                          index=0)
pickup_borough = st.selectbox("Distrito de recogida:", 
                              ["Manhattan", "Queens", "Brooklyn", "Staten Island", "EWR"],
                              index=0)
servicio = st.selectbox("Tipo de servicio:", ["Green", "Yellow"], index=0)

# Mapear entradas de texto a valores numéricos
dias_semana_map = {"Lunes": 0, "Martes": 1, "Miércoles": 2, "Jueves": 3, "Viernes": 4, "Sábado": 5, "Domingo": 6}
pickup_borough_map = {"Manhattan": 0, "Queens": 1, "Brooklyn": 2, "Staten Island": 3, "EWR": 4}
servicio_map = {"Green": 0, "Yellow": 1}

dia_semana_val = dias_semana_map[dia_semana]
pickup_borough_val = pickup_borough_map[pickup_borough]
servicio_val = servicio_map[servicio]

# Botón de predicción
if st.button("Predecir Demanda"):
    predicted_demand = predict_demand(hora_inicio, dia_semana_val, pickup_borough_val, servicio_val)
    st.write(f"La demanda predicha de taxis es: {predicted_demand}")

# Ejecución del script de Streamlit desde la línea de comandos
# streamlit run app.py

