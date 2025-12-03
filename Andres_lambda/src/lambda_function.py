import urllib.parse
import json
from utils.utils import populate_final_model
from utils.utils import extract_final_key
from services.s3Service import save_data
from services.s3Service import read_data
import boto3
import os

sqs = boto3.client("sqs")

QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/886220647555/lambda_notifier"


def lambda_handler(event, context):
    print("Evento recibido:")
    print(json.dumps(event, indent=2))

    for record in event.get("Records", []):
        # Datos del archivo de entrada
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        event_name = record["eventName"]
    

        print(f"🪣 Bucket origen: {source_bucket}")
        print(f"📄 Archivo origen: {source_key}")
        print(f"⚡ Evento: {event_name}")
        

        # Configuración del bucket de salida
        target_bucket = "santiago-output"

        # Extraer nombre del archivo original
        target_key = extract_final_key(source_key)
         
        data = read_data(source_bucket, source_key)
        print(f"📄 Datos leídos: {data}")
        if data is None:
            continue
        
        final_model = populate_final_model(data, source_bucket, source_key, record)
       

        save_data(target_bucket, target_key, final_model)
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=final_model.model_dump_json()
        )
        
        print(f"Se subio el archivo modificado correctamente")       
        print(f"Proceso completado!")

    return {
        "statusCode": 200,
        "body": json.dumps("Procesamiento completado correctamente")
    }  


