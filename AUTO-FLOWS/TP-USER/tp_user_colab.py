from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
from google.colab import auth

# 1. Autenticación de usuario para Google Colab
# Este paso abrirá una ventana de navegador para que te loguees.
print("Iniciando la autenticación de usuario...")
auth.authenticate_user()
print("Autenticación completada. Creando el cliente de BigQuery...")

# 2. Definir el cliente de BigQuery
# Ahora el cliente usará las credenciales que acabas de autenticar
client = bigquery.Client(project='meli-bi-data') # Especificamos el proyecto explícitamente

print("Conexión a BigQuery establecida correctamente.")

# A partir de aquí, el resto de tu código es correcto y no necesita cambios.
# ... (el resto del código para fechas, consulta y DataFrame)

# 2. Calcular las fechas dinámicamente
today = datetime.now()
days_since_monday = today.weekday()
current_week_monday = today - timedelta(days=days_since_monday)

monday_week_a = current_week_monday - timedelta(days=7)
sunday_week_a = monday_week_a + timedelta(days=6)
monday_week_b = monday_week_a - timedelta(days=7)
sunday_week_b = monday_week_b + timedelta(days=6)

date_a_start = monday_week_a.strftime('%Y-%m-%d')
date_a_end = sunday_week_a.strftime('%Y-%m-%d')
date_b_start = monday_week_b.strftime('%Y-%m-%d')
date_b_end = sunday_week_b.strftime('%Y-%m-%d')

print(f"Semana A (Semana anterior): {date_a_start} a {date_a_end}")
print(f"Semana B (Semana anterior a la anterior): {date_b_start} a {date_b_end}")

# 3. Construir y ejecutar la consulta SQL con los filtros de fecha
query = f"""
query original
"""

# 4. Leer los resultados de la consulta en un DataFrame de Pandas
df = client.query(query).to_dataframe()

print(f"\nDataFrame cargado con {len(df)} filas.")
print("Primeras 5 filas del DataFrame:")
print(df.head())

# Asegúrate de que las columnas de fecha son del tipo de dato correcto
df['WEEK_ID'] = pd.to_datetime(df['WEEK_ID'])
monday_week_a = pd.to_datetime(date_a_start)
monday_week_b = pd.to_datetime(date_b_start)

# Agrupamos por la Clasificación y la semana, sumando las métricas clave
df_macro_analysis = df.groupby(['Clasificacion', 'WEEK_ID']).agg(
    Sessions=('Sessions', 'sum'),
    Sessions_valid_view=('Sessions_valid_view', 'sum')
).reset_index()

# Calculamos el CVR para cada semana y clasificación
df_macro_analysis['CVR_Sessions'] = df_macro_analysis['Sessions_valid_view'] / df_macro_analysis['Sessions']

# Convertimos a formato 'largo' para poder hacer la comparación
# Pivotamos el DataFrame para que cada métrica tenga una columna para cada semana
df_macro_pivoted = df_macro_analysis.pivot_table(
    index='Clasificacion',
    columns='WEEK_ID',
    values=['Sessions', 'Sessions_valid_view', 'CVR_Sessions']
).reset_index()

# Limpiamos y renombramos las columnas para una mejor lectura
# Limpiamos y renombramos las columnas para una mejor lectura
df_macro_pivoted.columns = [
    f'{col[0]}_{col[1].strftime("%Y-%m-%d")}' if isinstance(col[1], datetime) and not pd.isna(col[1]) else col[0]
    for col in df_macro_pivoted.columns
]

# A more robust way to handle the renaming, avoiding the list comprehension entirely
# df_macro_pivoted.columns = [f'{col[0]}_{col[1]}' if not pd.isna(col[1]) else col[0] for col in df_macro_pivoted.columns]
# You can then perform the final rename based on the string values

df_macro_pivoted.rename(columns={
    f'Sessions_{monday_week_a.strftime("%Y-%m-%d")}': 'Sessions_A',
    f'Sessions_{monday_week_b.strftime("%Y-%m-%d")}': 'Sessions_B',
    f'Sessions_valid_view_{monday_week_a.strftime("%Y-%m-%d")}': 'Sessions_valid_view_A',
    f'Sessions_valid_view_{monday_week_b.strftime("%Y-%m-%d")}': 'Sessions_valid_view_B',
    f'CVR_Sessions_{monday_week_a.strftime("%Y-%m-%d")}': 'CVR_Sessions_A',
    f'CVR_Sessions_{monday_week_b.strftime("%Y-%m-%d")}': 'CVR_Sessions_B'
}, inplace=True)

# Calculamos las variaciones (WoW)
df_macro_pivoted['Sessions_valid_view_WoW_Change'] = ((df_macro_pivoted['Sessions_valid_view_A'] - df_macro_pivoted['Sessions_valid_view_B']) / df_macro_pivoted['Sessions_valid_view_B']).fillna(0)
df_macro_pivoted['CVR_Sessions_WoW_Change'] = ((df_macro_pivoted['CVR_Sessions_A'] - df_macro_pivoted['CVR_Sessions_B']) / df_macro_pivoted['CVR_Sessions_B']).fillna(0)

# Ordenamos por la cantidad de sessions_valid_view en la semana A de forma descendente
df_macro_pivoted.sort_values(by='Sessions_valid_view_A', ascending=False, inplace=True)

# Mostramos el resultado
print("Análisis macro por Clasificación (Resumen WoW):")
print(df_macro_pivoted)

import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Aca continuas con el codigo para el calculo de fechas y la carga del DataFrame principal `df`
# ... (código para monday_week_a, sunday_week_a, etc., y la carga del DataFrame df)

# 1. Agrupamos por touchpoint_no_team y la semana
df_micro_analysis = df.groupby(['touchpoint_no_team', 'WEEK_ID']).agg(
    Sessions=('Sessions', 'sum'),
    Sessions_valid_view=('Sessions_valid_view', 'sum')
).reset_index()

# 2. Calculamos el CVR por touchpoint y semana
df_micro_analysis['CVR_Sessions'] = np.where(
    df_micro_analysis['Sessions'] > 0,
    df_micro_analysis['Sessions_valid_view'] / df_micro_analysis['Sessions'],
    0
)

# 3. Pivotamos para la comparación de la semana A vs. la semana B
df_micro_pivoted = df_micro_analysis.pivot_table(
    index='touchpoint_no_team',
    columns='WEEK_ID',
    values=['Sessions', 'Sessions_valid_view', 'CVR_Sessions']
).reset_index()

# 4. Limpiamos y renombramos las columnas
# Asume que ya tienes las variables monday_week_a y monday_week_b de tu código de fechas
df_micro_pivoted.columns = [
    f'{col[0]}_{col[1].strftime("%Y-%m-%d")}' if isinstance(col[1], (datetime, np.datetime64)) and not pd.isna(col[1]) else col[0]
    for col in df_micro_pivoted.columns
]
df_micro_pivoted.rename(columns={
    f'Sessions_{monday_week_a.strftime("%Y-%m-%d")}': 'Sessions_A',
    f'Sessions_{monday_week_b.strftime("%Y-%m-%d")}': 'Sessions_B',
    f'Sessions_valid_view_{monday_week_a.strftime("%Y-%m-%d")}': 'Sessions_valid_view_A',
    f'Sessions_valid_view_{monday_week_b.strftime("%Y-%m-%d")}': 'Sessions_valid_view_B',
    f'CVR_Sessions_{monday_week_a.strftime("%Y-%m-%d")}': 'CVR_Sessions_A',
    f'CVR_Sessions_{monday_week_b.strftime("%Y-%m-%d")}': 'CVR_Sessions_B'
}, inplace=True)

# 5. Filtrar solo los touchpoints relevantes (los que superan un umbral de volumen)
# Aquí usamos un umbral fijo, que es más simple y robusto que la regla 80/20 en scripts
umbral_sesiones_validas = 500  # Puedes ajustar este número según tu negocio

df_relevant_tp = df_micro_pivoted[df_micro_pivoted['Sessions_valid_view_A'] >= umbral_sesiones_validas]

# 6. Calculamos las variaciones WoW solo para los touchpoints relevantes
df_relevant_tp['Sessions_WoW_Change'] = ((df_relevant_tp['Sessions_A'] - df_relevant_tp['Sessions_B']) / df_relevant_tp['Sessions_B']).fillna(0)
df_relevant_tp['Sessions_valid_view_WoW_Change'] = ((df_relevant_tp['Sessions_valid_view_A'] - df_relevant_tp['Sessions_valid_view_B']) / df_relevant_tp['Sessions_valid_view_B']).fillna(0)
df_relevant_tp['CVR_WoW_Change'] = ((df_relevant_tp['CVR_Sessions_A'] - df_relevant_tp['CVR_Sessions_B']) / df_relevant_tp['CVR_Sessions_B']).fillna(0)

# 7. Seleccionamos los top 5 de crecimiento y caída en Sessions_valid_view
top_5_growth_sessions = df_relevant_tp.sort_values(by='Sessions_valid_view_WoW_Change', ascending=False).head(5)
top_5_decline_sessions = df_relevant_tp.sort_values(by='Sessions_valid_view_WoW_Change', ascending=True).head(5)

print("\nAnálisis Micro: Top 5 Touchpoints con Mayor Crecimiento en Vistas Válidas (WoW):")
print(top_5_growth_sessions[['touchpoint_no_team', 'Sessions_valid_view_A', 'Sessions_valid_view_B', 'Sessions_valid_view_WoW_Change']])

print("\nAnálisis Micro: Top 5 Touchpoints con Mayor Caída en Vistas Válidas (WoW):")
print(top_5_decline_sessions[['touchpoint_no_team', 'Sessions_valid_view_A', 'Sessions_valid_view_B', 'Sessions_valid_view_WoW_Change']])

# También podemos hacer un reporte separado para CVR, si un TP relevante tuvo un cambio significativo
# Ejemplo: Top 5 con mayor crecimiento en CVR, entre los touchpoints relevantes
top_5_growth_cvr = df_relevant_tp.sort_values(by='CVR_WoW_Change', ascending=False).head(5)

print("\nAnálisis Complementario: Top 5 Touchpoints Relevantes con Mayor Crecimiento de CVR (WoW):")
print(top_5_growth_cvr[['touchpoint_no_team', 'Sessions_valid_view_A', 'CVR_Sessions_A', 'CVR_Sessions_B', 'CVR_WoW_Change']])

# Convertimos los valores de cambio a porcentaje
df_macro_pivoted['Sessions_valid_view_WoW_Change'] = (df_macro_pivoted['Sessions_valid_view_WoW_Change'] * 100).round(2)
df_macro_pivoted['CVR_Sessions_WoW_Change'] = (df_macro_pivoted['CVR_Sessions_WoW_Change'] * 100).round(2)

df_relevant_tp['Sessions_WoW_Change'] = (df_relevant_tp['Sessions_WoW_Change'] * 100).round(2)
df_relevant_tp['Sessions_valid_view_WoW_Change'] = (df_relevant_tp['Sessions_valid_view_WoW_Change'] * 100).round(2)
df_relevant_tp['CVR_WoW_Change'] = (df_relevant_tp['CVR_WoW_Change'] * 100).round(2)

# Formateamos las fechas para el título del mensaje
date_a_str = monday_week_a.strftime('%d-%b')
date_b_str = monday_week_b.strftime('%d-%b')

# Iniciamos el mensaje con una cabecera
message_text = f"📊 *Reporte Semanal de Touchpoints* 📊\n"
message_text += f"Análisis de {date_a_str} vs. {date_b_str}\n\n"

# --- Sección de Análisis Macro por Clasificación ---
message_text += "--- *Resumen por Clasificación (Nivel Macro)* ---\n"
# Iteramos sobre el DataFrame de análisis macro para construir el mensaje
for index, row in df_macro_pivoted.iterrows():
    clasificacion = row['Clasificacion']
    sessions_change = row['Sessions_valid_view_WoW_Change']
    cvr_change = row['CVR_Sessions_WoW_Change']

    message_text += f"• *{clasificacion}*: WoW Sessions valid view: `{sessions_change}%`, WoW CVR: `{cvr_change}%`\n"

# --- Sección de Análisis Micro (Top 5) ---
message_text += "\n--- *Touchpoints con Mayor Crecimiento (Top 5)* 📈 ---\n"
if not top_5_growth_sessions.empty:
    for index, row in top_5_growth_sessions.iterrows():
        tp_name = row['touchpoint_no_team']
        sessions_change = row['Sessions_valid_view_WoW_Change']
        sessions_A_formatted = f'{row["Sessions_valid_view_A"]:,}'
        sessions_B_formatted = f'{row["Sessions_valid_view_B"]:,}'
        message_text += f"• *{tp_name}*: {sessions_B_formatted} -> {sessions_A_formatted} (`+{round(sessions_change * 100, 2)}%`) Sessions valid view\n"
else:
    message_text += "No se encontraron touchpoints con crecimiento significativo.\n"

message_text += "\n--- *Touchpoints con Mayor Caída (Top 5)* 📉 ---\n"
if not top_5_decline_sessions.empty:
    for index, row in top_5_decline_sessions.iterrows():
        tp_name = row['touchpoint_no_team']
        sessions_change = row['Sessions_valid_view_WoW_Change']
        sessions_A_formatted = f'{row["Sessions_valid_view_A"]:,}'
        sessions_B_formatted = f'{row["Sessions_valid_view_B"]:,}'
        message_text += f"• *{tp_name}*: {sessions_B_formatted} -> {sessions_A_formatted} (`{round(sessions_change * 100, 2)}%`) Sessions valid view \n"
else:
    message_text += "No se encontraron touchpoints con caída significativa.\n"

print(message_text)

import slack_sdk

# ... (código de análisis y construcción del mensaje 'message_text')

# 1. Definir las credenciales de Slack
SLACK_TOKEN = "xoxb-9378410336625-9427383926241-iG4t92BzlJXLl1M1YbPdp9iI"  # Reemplaza con tu token
SLACK_CHANNEL = "#general-tester"  # Reemplaza con el nombre de tu canal

# 2. Inicializar el cliente de la API de Slack
client = slack_sdk.WebClient(token=SLACK_TOKEN)

# 3. Enviar el mensaje
try:
    response = client.chat_postMessage(
        channel=SLACK_CHANNEL,
        text=message_text
    )
    print("✅ Mensaje enviado a Slack con éxito.")
    print(f"Respuesta de la API: {response['ts']}")
except slack_sdk.errors.SlackApiError as e:
    print(f"❌ Error al enviar el mensaje a Slack: {e.response['error']}")