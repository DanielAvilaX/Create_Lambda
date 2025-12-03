import json
import boto3
import urllib.parse
from datetime import datetime

s3 = boto3.client('s3')

def extractFinalKey(source_key):
    date_folder = datetime.now().strftime("%Y-%m-%d")
    filename = source_key.split("/")[-1]
    filenamewithoutextension = filename.split(".")[0]
    extension = filename.split(".")[-1]
    new_filename = f"{filenamewithoutextension}-lambda.{extension}"
    target_key = f"{date_folder}/{new_filename}"
    return target_key

def transformData (source_bucket, source_key, data):
    try:
        response = s3.get_object(Bucket=source_bucket, Key=source_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
    except Exception as e:
        print(f"❌ Error leyendo el archivo original: {e}")
        return e
        

    # 🔹 Procesar el JSON
    try:
        # Formatear el precio en formato COP manualmente
        if "precio" in data:
            valor = float(data["precio"])
            precio_format = f"{valor:,.2f}" 
            precio_format = precio_format.replace(",", "X").replace(".", ",").replace("X", ".")
            data["precio"] = f"COP$ {precio_format}"

        # Agregar notificación y fecha de procesamiento
        data["Notificacion"] = "Notificación generada por Lambda"

        print(f"✅ JSON procesado: {data}")

    except Exception as e:
        print(f"❌ Error modificando el JSON: {e}")
        return e
    return data

def saveData (target_bucket, target_key):
    print(f"📂 Guardando archivo procesado en: {target_bucket}/{target_key}")

    try:
        s3.put_object(
            Bucket=target_bucket,
            Key=target_key,
            Body=json.dumps(data, indent=2, ensure_ascii=False),#preguntar
            ContentType='application/json'
        )
        print("✅ Archivo modificado y guardado correctamente")
    except Exception as e:
        print(f"❌ Error al guardar el archivo modificado: {e}")
    

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
        target_key = extractFinalKey(source_key)


        data = transformData(source_bucket, source_key)
        if isinstance(data, Exception):
            continue

        SaveData = saveData(target_bucket, target_key, data)

    return {
        "statusCode": 200,
        "body": json.dumps("Procesamiento completado correctamente")
    }  


