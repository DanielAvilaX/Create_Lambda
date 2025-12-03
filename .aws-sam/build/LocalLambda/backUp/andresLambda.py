import urllib.parse
import json
from utils.utils import extract_final_key
from utils.utils import format_price
from utils.utils import add_metadata
from services.s3Service import save_data
from services.s3Service import read_data
import boto3


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
        
        #latest_id = getIdOfLatestVersionsn (source_key) 

        # Transformar el formato del peso
        data = format_price(data)
        print(f"📄 Datos transformados: {data}")

        #Insertar la metadata
        data = add_metadata(data)
        print(f"📄 Datos con metadata: {data}")

        save_data(target_bucket, target_key, data)

    return {
        "statusCode": 200,
        "body": json.dumps("Procesamiento completado correctamente")
    }  


