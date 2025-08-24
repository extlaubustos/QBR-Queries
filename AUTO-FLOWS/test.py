import datetime
import time
import sys
import logging
logging.basicConfig(level=logging.ERROR)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

SLACK_TOKEN = connections['NAME CONNECTION SLACK'].get_secret()
SLACK_CHANNEL = 'CÓDIGO 11 CARACTERES'
client = WebClient(token=SLACK_TOKEN)
conexion_bq = 'NAME CONNECTION BIGQUERY'

def get_bq_data():
    client = connections[conexion_bq].bigquery_client
    query_job = client.query('''
        QUERY
    '''
    query_job = client.query(query)
    query_job.result()

def make_messages():
    messages = []
    message = ""
    df = get_bq_data()
    count = 0
    for index, row in df.iterrows():
        if row["STATUS_UPDATE"] in ["ERROR", "TIMEOUT_DF", "TIMEOUT_AVG"]:
            count += 1
            if count % 5 == 0:  # Si el contador es divisible por 5, crea un nuevo mensaje
                messages.append(message)
                message = ""
            if row["STATUS_UPDATE"] == "ERROR":
                message += f"""
:error_red: ERROR :error_red:
- *Role*: { row["ROLE_DF"] }
- *Site*: { row["SITE"] }
- *Job*: { row["NOMBRE_JOB"] } (*ID*: { row["JOB_ID"] })
- *Step*: { row["STEP_NAME"] }
- *Ejecutado por*: { row["EXECUTION_BY"] }
- *Date*: { row["FECHA"] }
- *Start Time (ARG)*: { row["START_TIME"] }
- *End Time (ARG)*: { row["END_TIME"] }
- *Execution Time*: { row["TIEMPO_EJECUCION"] }
"""

            elif row["STATUS_UPDATE"] == "TIMEOUT_DF":
                message += f"""
:info_orange2: TIMEOUT_DF_ALERT :info_orange2:
- *Role*: { row["ROLE_DF"] }
- *Site*: { row["SITE"] }
- *Job*: { row["NOMBRE_JOB"] } (*ID*: { row["JOB_ID"] })
- *Step*: { row["STEP_NAME"] }
- *Ejecutado por*: { row["EXECUTION_BY"] }
- *Date*: { row["FECHA"] }
- *Start Time (ARG)*: { row["START_TIME"] }
- *End Time (ARG)*: { row["END_TIME"] }
- *Execution Time*: { row["TIEMPO_EJECUCION"] }
"""

    if message:  # Asegurarse de agregar el último mensaje si quedó pendiente
        messages.append(message)
    return messages

def send_slack_message(messages):
    status = False
    for message in messages:
        blocks = [{
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":info: Alertas en ejecucion :info:"
            }
        }]

        blocks.append({"type": "divider"})

        section = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{message} \n"
            }
        }

        blocks.append(section)
        blocks.append({"type": "divider"})

        blocks.append({
            "type": "context",
            "elements": [{
                "type": "plain_text",
                "text": "Powered by TEAM"
            }]
        })

        try:
            response = client.chat_postMessage(
                channel=SLACK_CHANNEL,
                text="Alertas en ejecución",
                blocks=blocks
            )
            update_notification_flag()
            status = True

        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert e.response["error"]
    return status

if __name__ == "__main__":
    messages = make_messages()
    if messages:
        print(f"Mensaje enviado: {send_slack_message(messages)}")
    else:
        print("No hay mensajes para enviar.")